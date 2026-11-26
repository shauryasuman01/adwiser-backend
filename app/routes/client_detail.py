"""S4 client drawer. Current identity and scoped historical charts."""
import asyncio
import json
import logging
import math

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from app.database.influx import query_influx
from app.middleware.auth import AuthError, require_user
from app.routes.clients import list_clients
from app.routes.overview import epoch, now_ms, number, literal, predicate, chart, status
from app.routes.aps import rate

router = APIRouter(prefix='/api/v1/adwiser', tags=['Monitor'])
logger = logging.getLogger(__name__)


def association_history(events, aps, start, end):
    """Only observed association/roam events establish a span."""
    spans, markers = [], []
    active, opened = None, None
    for e in events:
        t = epoch(e['ts'])
        if t is None or t >= end:
            continue
        if active and opened is not None and max(opened, start) < min(t, end):
            spans.append({'apId':active, 'ap':aps.get(active, active),
                          'from':max(opened,start),'to':min(t,end)})
        active = e['event_ap'] if e['type'] in ('association','roam') else None
        opened = t if active else None
        if e['type']=='roam' and t >= start:
            markers.append({'t':t,'label':e['message'],'kind':'roam'})
    if active and opened is not None and max(opened,start)<end:
        spans.append({'apId':active,'ap':aps.get(active,active),'from':max(opened,start),'to':end})
    return spans, markers


def flow_row(c, r, current, history, identity, aps):
    p95, bound = number(r.get('p95')), number(c['lat_bound_ms'])
    offered, lost = number(r.get('offered')), number(r.get('lost'))
    state = current.get('worst')
    label,tone = {0:('In SLA','good'),1:('At risk','warn'),2:('Violating','crit')}.get(state,('Unknown','neutral'))
    aid = current.get('ap_id')
    return {'flowId':c['flow_id'],'name':c['name'],'status':status(label,tone),
            'slaClass':c['sla_name'],'client':identity['name'],'staId':identity['staId'],
            'ap':aps.get(aid,aid) if aid else 'Not associated','apId':aid,'direction':c['direction'],
            'p50':number(r.get('p50')),'p95':p95,'p99':number(r.get('p99')),
            'bound':bound,'jitter':number(r.get('jitter')),'thpt':number(r.get('thpt')),
            'minThpt':number(c['min_thpt_mbps']), 'lossPct':100*lost/offered if offered and lost is not None else None,
            'deadlineMiss':number(r.get('misses')),'compliance':number(r.get('compliance')),
            'margin':p95/bound if p95 is not None and bound and bound>0 else None,
            'spark':[number(s.get('p95' if bound is not None else 'thpt')) for s in history][-40:]}


@router.get('/clients/{sta_id}')
async def client_detail(sta_id: str, request: Request, user=Depends(require_user)):
    db, influx = request.app.state.pg_pool, request.app.state.influx_client
    # Reuse verified site scope, association freshness, MAC permissions and meta.
    listing = json.loads((await list_clients(request,user)).body)
    meta = listing['meta']; window = meta['window']; site = meta['site']['siteId']
    now = window['to'] if window['range']=='live' else now_ms()
    row = next((r for r in listing['data']['rows'] if r['staId']==sta_id),None)
    if row is None:
        raise AuthError(404,'NOT_FOUND','Client was not found.')
    aps = {a['apId']:a['name'] for a in listing['data']['aps']}
    c = await db.fetchrow('SELECT owner,host(ip) AS ip FROM client WHERE sta_id=$1',sta_id)
    identity = {k:row[k] for k in ('staId','name','type','mac','randomised','ap','apId')}
    identity.update(owner=c['owner'] if c else None,ip=c['ip'] if c else None,channel=None)
    # One pre-range event establishes a known association at the left boundary.
    events = await db.fetch('''WITH history AS (
        SELECT event_id,ts,type,message,COALESCE(details->>'to_ap',details->>'ap_id') AS event_ap
        FROM event WHERE site_id=$1 AND entity_type='client' AND entity_id=$2
        AND type IN ('association','disassociation','roam')
        AND ts<to_timestamp($4::double precision/1000)
    ), prior AS (SELECT * FROM history WHERE ts<to_timestamp($3::double precision/1000)
        ORDER BY ts DESC,event_id DESC LIMIT 1)
    SELECT * FROM prior UNION ALL SELECT * FROM history
    WHERE ts>=to_timestamp($3::double precision/1000) ORDER BY ts,event_id''',site,sta_id,window['from'],window['to'])
    spans, markers = association_history(events,aps,window['from'],window['to'])
    configs = await db.fetch('''SELECT f.flow_id,f.name,f.direction,s.name AS sla_name,
        s.lat_bound_ms,s.min_thpt_mbps FROM flow f LEFT JOIN sla_class s ON s.sla_class_id=f.sla_class_id
        WHERE f.site_id=$1 AND f.sta_id=$2 AND f.managed IS TRUE AND f.retired_at IS NULL ORDER BY f.flow_id''',site,sta_id)
    rules = {r['rule_id']:r for r in await db.fetch("SELECT rule_id,threshold,enabled,duration_s FROM alert_rule WHERE rule_id IN ('r-client-rssi','r-flow-violation')")}
    step = window['bucketS']; where = predicate(site,window['from'],window['to']); sw = f'{where} AND sta_id={literal(sta_id)}'
    async def q(sql):
        return await query_influx(influx,sql)
    async def empty():
        return []
    try:
        link, radio = await asyncio.gather(q(f"SELECT date_bin(INTERVAL '{step} seconds',time) AS bucket,avg(rssi_dbm) AS rssi,last_value(tx_mcs ORDER BY time) AS mcs,avg(tx_bytes) AS tx,avg(rx_bytes) AS rx FROM sta_link WHERE {sw} GROUP BY bucket ORDER BY bucket"),
            q(f"SELECT last_value(channel ORDER BY time) AS channel,max(time) AS seen FROM ap_radio WHERE {predicate(site,now-10000,now)} AND ap_id={literal(row['apId'])} AND radio={literal(row['radio'])}") if row['apId'] and row['radio'] else empty())
        if radio and radio[0].get('channel') is not None and radio[0].get('seen') is not None and 0<=now-epoch(radio[0]['seen'])<=3000:
            band = await db.fetchval('SELECT band FROM radio WHERE ap_id=$1 AND radio=$2',row['apId'],row['radio'])
            identity['channel'] = f"{band} ch {radio[0]['channel']}" if band else f"ch {radio[0]['channel']}"
        metrics, current, history = [], [], []
        if configs:
            ids = ','.join(literal(f['flow_id']) for f in configs)
            fw = f'{where} AND flow_id IN ({ids})'
            v = rules.get('r-flow-violation')
            duration = max(1,v['duration_s']) if v and v['enabled'] else 30
            live = predicate(site,now-max(10000,duration*1000),now)
            spark_step = max(step,math.ceil((window['to']-window['from'])/40000))
            metrics,current,history = await asyncio.gather(
                q(f'''SELECT flow_id,last_value(lat_p50_ms ORDER BY time) AS p50,last_value(lat_p95_ms ORDER BY time) AS p95,
                last_value(lat_p99_ms ORDER BY time) AS p99,last_value(jitter_ms ORDER BY time) AS jitter,last_value(thpt_mbps ORDER BY time) AS thpt,
                sum(pkts) AS offered,sum(lost_pkts) AS lost,sum(deadline_miss) AS misses,
                avg(CASE WHEN sla_state IN (0,1) THEN 100.0 WHEN sla_state=2 THEN 0.0 ELSE NULL END) AS compliance
                FROM flow_kpi WHERE {fw} GROUP BY flow_id'''),
                q(f'SELECT flow_id,max(time) AS seen,last_value(ap_id ORDER BY time) AS ap_id,max(sla_state) AS worst FROM flow_kpi WHERE {live} AND flow_id IN ({ids}) GROUP BY flow_id'),
                q(f"SELECT flow_id,date_bin(INTERVAL '{spark_step} seconds',time) AS bucket,last_value(lat_p95_ms ORDER BY time) AS p95,avg(thpt_mbps) AS thpt FROM flow_kpi WHERE {fw} GROUP BY flow_id,bucket ORDER BY bucket"))
    except Exception as exc:
        logger.exception('Client detail telemetry query failed')
        raise AuthError(503,'INTERNAL_ERROR','Telemetry query failed or is unavailable.') from exc
    rssi = chart(link,window,'dBm',[('rssi','RSSI','rssi')]); rssi.update(yMin=-95,yMax=-40,markers=markers)
    rule = rules.get('r-client-rssi')
    threshold = -70 if rule is None else number(rule['threshold']) if rule['enabled'] else None
    rssi['refs'] = [{'label':'RSSI warn','value':threshold,'tone':'warn'}] if threshold is not None else []
    mcs = chart(link,window,'',[('mcs','TX MCS','mcs')]); mcs.update(yMin=0,yMax=11,decimals=0)
    ul = chart([dict(r,dl=rate(r.get('tx')),ul=rate(r.get('rx'))) for r in link],window,'Mbps',[('ul','Uplink','ul'),('dl','Downlink','dl')]); ul['decimals']=2
    by_metric = {r['flow_id']:r for r in metrics}
    by_current = {r['flow_id']:r for r in current if r.get('seen') is not None and 0<=now-epoch(r['seen'])<=3000}
    flows = [flow_row(f,by_metric.get(f['flow_id'],{}),by_current.get(f['flow_id'],{}),[r for r in history if r['flow_id']==f['flow_id']],identity,aps) for f in configs]
    return JSONResponse(content={'meta':meta,'data':{'identity':identity,'rssi':rssi,'association':spans,'mcs':mcs,'ul':ul,'flows':flows}},headers={'Cache-Control':'no-store'})
