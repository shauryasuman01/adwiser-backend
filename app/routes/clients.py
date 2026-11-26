"""Monitor S4 client list. The frontend filters the complete site-scoped list."""
import logging
import operator

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.database.influx import query_influx
from app.middleware.auth import AuthError, require_user
from app.services.session import session_response
from app.routes.overview import now_ms, epoch, number, literal, predicate, scope_window, site_view, refresh_seconds
from app.routes.aps import mac_view, rate

router = APIRouter(prefix='/api/v1/adwiser', tags=['Monitor'])
logger = logging.getLogger(__name__)
CLIENT_TYPES = ['robot', 'camera', 'laptop', 'phone', 'unknown']
# Same operational freshness policy as the existing AP list/detail routes.
FRESH_MS = 3000
OPS = {'<': operator.lt, '>': operator.gt, '<=': operator.le,
       '>=': operator.ge, '=': operator.eq}


def latest_links_sql(site, now):
    fields = 'ap_id radio rssi_dbm snr_db tx_mcs nss tx_phy_mbps tx_bytes rx_bytes tx_retry_pct'
    last = ', '.join(f'last_value({f} ORDER BY time) AS {f}' for f in fields.split())
    return (f'SELECT sta_id,max(time) AS seen,{last} FROM sta_link '
            f'WHERE {predicate(site, now-10000, now)} GROUP BY sta_id')


def breach(rules, key, value, default_op, default_threshold):
    if value is None:
        return False
    rule = rules.get(key)
    if rule is None:
        return OPS[default_op](value, default_threshold)
    threshold = number(rule['threshold'])
    compare = OPS.get(rule['operator'])
    return bool(rule['enabled'] and threshold is not None and compare and compare(value, threshold))


def client_row(c, link, aps, flow_count, rules, permissions, now, event=None):
    sid = c['sta_id']
    seen = epoch(link.get('seen'))
    aid = link.get('ap_id')
    associated = aid in aps and seen is not None and 0 <= now-seen <= FRESH_MS
    # Do not retain a live association when a newer disassociation is known.
    if associated and event and event['type'] == 'disassociation' and epoch(event['ts']) >= seen:
        associated = False
    if not associated:
        aid = None
        link = {}
    rssi, retry = number(link.get('rssi_dbm')), number(link.get('tx_retry_pct'))
    weak = breach(rules, 'r-client-rssi', rssi, '<', -70) or breach(rules, 'r-client-retry', retry, '>', 10)
    connected_since = None
    # No first_seen substitution: it is not the start of the current association.
    if associated and event and event['type'] in ('association', 'roam'):
        if event['event_ap'] == aid and epoch(event['ts']) <= seen:
            connected_since = epoch(event['ts'])
    return {'staId':sid, 'name':c.get('hostname') or c.get('label') or sid,
            'type':c.get('device_type') or 'unknown', 'mac':mac_view(c.get('mac'),permissions),
            'randomised':bool(c.get('mac_randomised',False)), 'apId':aid,
            'ap':aps[aid] if associated else 'Not associated', 'radio':link.get('radio'),
            'rssi':rssi, 'snr':number(link.get('snr_db')), 'mcs':number(link.get('tx_mcs')),
            'nss':number(link.get('nss')), 'phyMbps':number(link.get('tx_phy_mbps')),
            'dl':rate(link.get('tx_bytes')), 'ul':rate(link.get('rx_bytes')), 'retry':retry,
            'flows':flow_count, 'connectedSince':connected_since,
            'signal':('neutral' if not associated or (rssi is None and retry is None)
                      else 'warn' if weak else 'good')}


@router.get('/clients')
async def list_clients(request: Request, user=Depends(require_user)):
    db, influx = request.app.state.pg_pool, request.app.state.influx_client
    session = await session_response(db, user)
    permissions = session['permissions']
    now = now_ms()
    window = scope_window(request.query_params, permissions, now)
    sites = await db.fetch('SELECT site_id,name,timezone,sla_target_pct FROM site ORDER BY name,site_id')
    requested = request.query_params.get('site')
    chosen = next((s for s in sites if s['site_id']==requested),None) if requested is not None else (sites[0] if sites else None)
    if chosen is None:
        raise AuthError(400 if sites else 503, 'INVALID_REQUEST' if sites else 'INTERNAL_ERROR',
                        'Unknown site.' if sites else 'No sites configured.')
    site = chosen['site_id']
    ap_rows = await db.fetch('SELECT ap_id,name FROM ap WHERE site_id=$1 AND retired_at IS NULL ORDER BY name,ap_id',site)
    aps = {r['ap_id']:r['name'] for r in ap_rows}
    managed = await db.fetch('SELECT sta_id,count(*) AS n FROM flow WHERE site_id=$1 AND managed IS TRUE AND retired_at IS NULL AND sta_id IS NOT NULL GROUP BY sta_id',site)
    flow_counts = {r['sta_id']:r['n'] for r in managed}
    rules = {r['rule_id']:r for r in await db.fetch("SELECT rule_id,operator,threshold,enabled FROM alert_rule WHERE rule_id IN ('r-client-rssi','r-client-retry')")}
    try:
        links = await query_influx(influx, latest_links_sql(site, now))
        link_map = {r['sta_id']:r for r in links}
        if len(sites) == 1:
            # Current installation is single-site; includes registered offline clients.
            inventory = await db.fetch('SELECT sta_id,hostname,label,device_type,mac::text AS mac,mac_randomised FROM client ORDER BY sta_id')
        else:
            # client currently has no site_id. Use site-specific inventory evidence and
            # configured flows; never return the entire global inventory in multi-site mode.
            members = await query_influx(influx, f'SELECT DISTINCT sta_id FROM sta_inventory WHERE site_id={literal(site)}')
            ids = sorted(set(link_map) | set(flow_counts) | {r['sta_id'] for r in members})
            inventory = await db.fetch('SELECT sta_id,hostname,label,device_type,mac::text AS mac,mac_randomised FROM client WHERE sta_id=ANY($1::text[]) ORDER BY sta_id',ids)
    except Exception as exc:
        logger.exception('Client list telemetry query failed')
        raise AuthError(503,'INTERNAL_ERROR','Telemetry query failed or is unavailable.') from exc
    clients = {r['sta_id']:dict(r) for r in inventory}
    # A station may publish before the periodic inventory worker registers it.
    for sid in link_map:
        clients.setdefault(sid, {'sta_id':sid})
    events = await db.fetch('''SELECT DISTINCT ON (entity_id) entity_id,type,ts,
        COALESCE(details->>'to_ap',details->>'ap_id') AS event_ap
        FROM event WHERE site_id=$1 AND entity_type='client'
        AND entity_id=ANY($2::text[]) AND type IN ('association','disassociation','roam')
        AND ts<=to_timestamp($3::double precision/1000)
        ORDER BY entity_id,ts DESC,event_id DESC''',site,list(clients),now)
    event_map = {r['entity_id']:r for r in events}
    rows = [client_row(c,link_map.get(sid,{}),aps,flow_counts.get(sid,0),rules,permissions,now,event_map.get(sid))
            for sid,c in sorted(clients.items())]
    counts = {'critical':0,'warning':0,'info':0}
    for r in await db.fetch("SELECT severity,count(*) AS n FROM alert WHERE site_id=$1 AND ended_at IS NULL AND status<>'resolved' GROUP BY severity",site):
        counts[r['severity']] = r['n']
    meta = {'user':session['user'],'permissions':permissions,'sites':[site_view(s) for s in sites],
            'site':site_view(chosen),'window':window,'generatedAt':now_ms(),'source':'api',
            'openAlerts':counts,'refreshS':refresh_seconds(window, permissions)}
    return JSONResponse(content={'meta':meta,'data':{'rows':rows,'aps':[{'apId':k,'name':v} for k,v in aps.items()],
                        'types':CLIENT_TYPES}},headers={'Cache-Control':'no-store'})
