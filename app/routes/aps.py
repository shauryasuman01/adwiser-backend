"""Monitor S2: AP list. Queries return bounded summaries, not raw point arrays."""
import asyncio
import logging
import os
from collections import Counter, defaultdict

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.database.influx import query_influx
from app.middleware.auth import AuthError, require_user
from app.services.session import session_response
from app.routes.overview import now_ms, epoch, number, predicate, scope_window, site_view, status, refresh_seconds
from app.services.ap_health import assess_ap

router = APIRouter(prefix='/api/v1/adwiser', tags=['Monitor'])
logger = logging.getLogger(__name__)
SAMPLE_MS = int(os.getenv('AP_SAMPLE_INTERVAL_MS', '20'))
if SAMPLE_MS <= 0:
    raise ValueError('AP_SAMPLE_INTERVAL_MS must be positive')


def mac_view(value, permissions):
    if value is None:
        return None
    text = str(value)
    return text if 'mac.full' in permissions else f'{text[:2]}:..:{text[-2:]}'


def rate(value):
    # AP TX is downlink, AP RX is uplink; bytes are per sample, not cumulative.
    return None if value is None else float(value) * 8 / (SAMPLE_MS * 1000)


def age(now, value):
    return None if value is None else max(0, (now - epoch(value)) / 1000)


def radio_query(where):
    fields = ('channel', 'width_mhz', 'n_clients', 'util_busy_pct',
              'tx_bytes', 'rx_bytes', 'tx_retry_pct', 'noise_dbm')
    last = ', '.join(f'last_value({f} ORDER BY time) AS {f}' for f in fields)
    return f'SELECT ap_id, radio, max(time) AS seen, {last} FROM ap_radio WHERE {where} GROUP BY ap_id, radio'


@router.get('/aps')
async def list_aps(request: Request, user=Depends(require_user)):
    db = request.app.state.pg_pool
    client = request.app.state.influx_client
    session = await session_response(db, user)
    permissions = session['permissions']
    now = now_ms()
    window = scope_window(request.query_params, permissions, now)
    sites = await db.fetch('SELECT site_id,name,timezone,sla_target_pct FROM site ORDER BY name,site_id')
    selected = request.query_params.get('site')
    chosen = next((s for s in sites if s['site_id'] == selected), None) if selected is not None else (sites[0] if sites else None)
    if chosen is None:
        raise AuthError(400 if sites else 503, 'INVALID_REQUEST' if sites else 'INTERNAL_ERROR', 'Unknown site.' if sites else 'No sites configured.')
    site = chosen['site_id']
    aps = await db.fetch('''SELECT a.ap_id,a.name,a.model,a.mac::text AS mac,
        host(a.mgmt_ip) AS ip,f.name AS floor_name FROM ap a
        LEFT JOIN floor f ON f.floor_id=a.floor_id AND f.site_id=a.site_id
        WHERE a.site_id=$1 AND a.retired_at IS NULL ORDER BY a.ap_id''', site)
    radios = await db.fetch('''SELECT r.ap_id,r.radio,r.band FROM radio r JOIN ap a ON a.ap_id=r.ap_id
        WHERE a.site_id=$1 AND a.retired_at IS NULL ORDER BY r.ap_id,r.radio''', site)
    managed = await db.fetch('SELECT flow_id,sta_id FROM flow WHERE site_id=$1 AND retired_at IS NULL AND managed IS TRUE', site)
    rules = {r['rule_id']:r for r in await db.fetch("SELECT rule_id,threshold,duration_s,enabled,operator FROM alert_rule WHERE rule_id IN ('r-ap-offline','r-ap-util','r-ap-retry','r-ap-lag')")}
    def rule(key, default_threshold, default_duration):
        r = rules.get(key)
        if r is None:
            return default_threshold, default_duration
        if not r['enabled'] or r['threshold'] is None or r['operator'] != '>':
            return None, 0
        return float(r['threshold']), max(0, r['duration_s'])
    offline = rules.get('r-ap-offline')
    offline_s = max(1, offline['duration_s']) if offline and offline['enabled'] else 3
    recent = predicate(site, now-300000, now)
    live = predicate(site, now-10000, now)
    async def sustained(table, metric, groups, threshold, duration):
        if threshold is None:
            return []
        # Read just one extra reporting interval for evidence at the start of the window.
        grace_ms = 3000 if table == 'ap_ingestion' else max(1000, 3*SAMPLE_MS)
        start = now-duration*1000
        where = predicate(site, start-grace_ms, now)
        group = ','.join(groups)
        # Evaluate all observations within the duration; require boundary coverage.
        # Missing samples inside the window are not fully detectable from this summary.
        rows = await query_influx(client, f'''SELECT {group},min({metric}) AS minimum,
          min(time) AS oldest,max(time) AS newest FROM {table} WHERE {where} GROUP BY {group}''')
        return [r for r in rows if r.get('minimum') is not None and float(r['minimum']) > threshold
                and epoch(r['oldest']) <= start + grace_ms and now-epoch(r['newest']) <= grace_ms]
    util_limit, util_s = rule('r-ap-util', 60, 60)
    retry_limit, retry_s = rule('r-ap-retry', 10, 60)
    lag_limit, lag_s = rule('r-ap-lag', 500, 30)
    try:
        latest, agents, flows, stations, util_bad, retry_bad, lag_bad = await asyncio.gather(
            query_influx(client, radio_query(recent)),
            query_influx(client, f'''SELECT ap_id,max(time) AS seen,max(CASE WHEN heartbeat_ok THEN time ELSE NULL END) AS last_heartbeat,last_value(heartbeat_ok ORDER BY time) AS heartbeat_ok,
                last_value(lag_ms_p95 ORDER BY time) AS lag FROM ap_ingestion WHERE {recent} GROUP BY ap_id'''),
            query_influx(client, f'SELECT flow_id,last_value(ap_id ORDER BY time) AS ap_id,max(time) AS seen FROM flow_kpi WHERE {live} GROUP BY flow_id'),
            query_influx(client, f'SELECT sta_id,last_value(ap_id ORDER BY time) AS ap_id,last_value(radio ORDER BY time) AS radio,max(time) AS seen FROM sta_link WHERE {live} GROUP BY sta_id'),
            sustained('ap_radio','util_busy_pct',('ap_id','radio'),util_limit,util_s),
            sustained('ap_radio','tx_retry_pct',('ap_id','radio'),retry_limit,retry_s),
            sustained('ap_ingestion','lag_ms_p95',('ap_id',),lag_limit,lag_s))
        radio_map = {(r['ap_id'],r['radio']):r for r in latest}
        if any((r['ap_id'],r['radio']) not in radio_map for r in radios) and window['range'] != 'live':
            history = await query_influx(client, radio_query(predicate(site,window['from'],window['to'])))
            for r in history:
                radio_map.setdefault((r['ap_id'],r['radio']),r)
    except Exception as exc:
        logger.exception('AP list telemetry query failed')
        raise AuthError(503,'INTERNAL_ERROR','Telemetry query failed or is unavailable.') from exc
    agents = {r['ap_id']:r for r in agents}
    station_map = {r['sta_id']:r for r in stations if age(now,r['seen']) <= 3}
    registered = {r['flow_id']:r['sta_id'] for r in managed}
    flow_counts, radio_counts = Counter(), Counter()
    for f in flows:
        if f['flow_id'] not in registered or age(now,f['seen']) > 3:
            continue
        aid = f['ap_id']
        flow_counts[aid] += 1
        sta = station_map.get(registered[f['flow_id']])
        if sta and sta['ap_id'] == aid:
            radio_counts[aid,sta['radio']] += 1
    reasons = defaultdict(list)
    for bad, label in ((util_bad,'utilisation'),(retry_bad,'retry rate')):
        for r in bad:
            reasons[r['ap_id']].append(f"{r['radio']} sustained high {label}")
    for r in lag_bad:
        reasons[r['ap_id']].append('Sustained telemetry lag')
    by_ap = defaultdict(list)
    for r in radios:
        aid, rid = r['ap_id'],r['radio']
        sample = radio_map.get((aid,rid),{})
        by_ap[aid].append({'radio':rid,'band':r['band'],'channel':sample.get('channel'),
            'widthMhz':sample.get('width_mhz'),'clients':sample.get('n_clients'),
            'util':number(sample.get('util_busy_pct')),'dl':rate(sample.get('tx_bytes')),
            'ul':rate(sample.get('rx_bytes')),'retry':number(sample.get('tx_retry_pct')),
            'noise':number(sample.get('noise_dbm')),'flows':radio_counts[aid,rid],
            'staleS':age(now,sample.get('seen'))})
    rows=[]
    for a in aps:
        aid=a['ap_id']; agent=agents.get(aid,{}); data=by_ap[aid]
        online,health=assess_ap(agent.get('heartbeat_ok'),age(now,agent.get('seen')),
                                [r['staleS'] for r in data],reasons[aid],offline_s)
        counts=[r['clients'] for r in data]
        row={'apId':aid,'name':a['name'],'status':health,'floor':a['floor_name'],
             'model':a['model'],'mac':mac_view(a['mac'],permissions),'ip':a['ip'],
             'radios':data,'clients':sum(counts) if counts and all(n is not None for n in counts) else None,
             'flows':flow_counts[aid],'lastSeen':epoch(agent.get('last_heartbeat')),
             # The fixed dashboard always renders this column. This duplicates
             # the already-public health state without exposing agent metrics.
             'agentState':'connected' if online else 'disconnected'}
        if 'details.engineering' in permissions:
            row['telemetryLagMs']=number(agent.get('lag'))
        rows.append(row)
    counts={'critical':0,'warning':0,'info':0}
    for r in await db.fetch("SELECT severity,count(*) AS n FROM alert WHERE site_id=$1 AND ended_at IS NULL AND status<>'resolved' GROUP BY severity",site):
        counts[r['severity']]=r['n']
    meta={'user':session['user'],'permissions':permissions,'sites':[site_view(s) for s in sites],
          'site':site_view(chosen),'window':window,'generatedAt':now_ms(),'source':'api',
          'openAlerts':counts,'refreshS':refresh_seconds(window, permissions)}
    return JSONResponse(content={'meta':meta,'data':{'rows':rows,
        'floors':sorted({a['floor_name'] for a in aps if a['floor_name']}),
        'bands':sorted({r['band'] for r in radios})}},headers={'Cache-Control':'no-store'})
