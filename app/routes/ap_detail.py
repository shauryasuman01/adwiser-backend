"""S3 AP details; SQL aggregates only, current association separate from chart range."""
import asyncio
import json
import logging
import math
from collections import Counter, defaultdict

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from app.database.influx import query_influx
from app.middleware.auth import AuthError, require_user
from app.services.session import session_response
from app.routes.overview import now_ms, epoch, number, literal, predicate, scope_window, chart, status
from app.routes.aps import list_aps, mac_view, rate

router = APIRouter(prefix='/api/v1/adwiser', tags=['Monitor'])
logger = logging.getLogger(__name__)


def refs(label, threshold):
    return [] if threshold is None else [{'label': label, 'value': threshold, 'tone': 'warn'}]


def aggregate_sql(table, where, step, columns, group=''):
    groups = f'{group}, ' if group else ''
    return (f"SELECT {groups}date_bin(INTERVAL '{step} seconds',time) AS bucket, {columns} "
            f"FROM {table} WHERE {where} GROUP BY {groups}bucket ORDER BY bucket")


def last_fields(fields):
    return ', '.join(f'last_value({f} ORDER BY time) AS {f}' for f in fields.split())


def radio_charts(row, samples, window, util_warn, retry_warn):
    util = chart(samples, window, '%', [('busy','Busy','busy'),('tx','TX','tx'),('rx','RX','rx'),('other','Other','other')])
    util['series'][0]['fill'] = True
    util['refs'] = refs('Warn', util_warn)
    throughput = chart([dict(r, tx=rate(r.get('tx_bytes')), rx=rate(r.get('rx_bytes'))) for r in samples], window, 'Mbps', [('rx','RX','rx'),('tx','TX','tx')])
    throughput['decimals'] = 2
    retries = chart(samples, window, '%', [('retry','Retry','retry'),('fail','Fail','fail')])
    retries['refs'] = refs('Retry warn', retry_warn)
    return {'radio':row['radio'],'label':row['band'],'util':util,'throughput':throughput,
            'retries':retries,'noise':chart(samples,window,'dBm',[('noise','Noise','noise')])}


@router.get('/aps/{ap_id}')
async def ap_detail(ap_id: str, request: Request, user=Depends(require_user)):
    db, client = request.app.state.pg_pool, request.app.state.influx_client
    # Validate scope and AP existence before expensive telemetry queries.
    session = await session_response(db,user)
    permissions = session['permissions']
    scope_window(request.query_params, permissions, now_ms())
    sites = await db.fetch('SELECT site_id FROM site ORDER BY name,site_id')
    if not sites:
        raise AuthError(503,'INTERNAL_ERROR','No sites configured.')
    site = request.query_params.get('site',sites[0]['site_id'])
    if site not in {s['site_id'] for s in sites}:
        raise AuthError(400,'INVALID_REQUEST','Unknown site.')
    ap = await db.fetchrow('''SELECT a.*,a.mac::text AS mac_text,host(a.mgmt_ip) AS ip_text,
       f.name AS floor_name FROM ap a LEFT JOIN floor f ON f.floor_id=a.floor_id AND f.site_id=a.site_id
       WHERE a.ap_id=$1 AND a.site_id=$2 AND a.retired_at IS NULL''',ap_id,site)
    if ap is None:
        raise AuthError(404,'NOT_FOUND','Access point was not found.')
    # Reuse the verified AP list health calculation and meta for consistency.
    listing = json.loads((await list_aps(request,user)).body)
    meta = listing['meta']; permissions = meta['permissions']; window = meta['window']; now = window['to'] if window['range']=='live' else now_ms()
    engineer = 'details.engineering' in permissions
    listed = next((r for r in listing['data']['rows'] if r['apId']==ap_id),None)
    if listed is None:
        raise AuthError(404,'NOT_FOUND','Access point was not found.')
    step = window['bucketS']; aid = literal(ap_id)
    where = predicate(site,window['from'],window['to'])
    ap_where = f'{where} AND ap_id={aid}'
    live = predicate(site,now-10000,now)
    recent = f'{predicate(site,now-300000,now)} AND ap_id={aid}'
    rules = {r['rule_id']:r for r in await db.fetch('SELECT rule_id,threshold,duration_s,enabled FROM alert_rule')}
    def threshold(key,default):
        r = rules.get(key)
        return default if r is None else number(r['threshold']) if r['enabled'] else None
    violation = rules.get('r-flow-violation')
    duration = max(1,violation['duration_s']) if violation and violation['enabled'] else 30
    flow_live = predicate(site,now-max(10000,duration*1000),now)
    async def q(sql):
        return await query_influx(client,sql)
    async def empty():
        return []
    configs = {r['flow_id']:r for r in await db.fetch('''SELECT f.flow_id,f.name,f.sta_id,f.direction,
        c.hostname,c.label,s.name AS sla_name,s.lat_bound_ms,s.min_thpt_mbps
        FROM flow f LEFT JOIN client c ON c.sta_id=f.sta_id LEFT JOIN sla_class s ON s.sla_class_id=f.sla_class_id
        WHERE f.site_id=$1 AND f.retired_at IS NULL AND f.managed IS TRUE''',site)}
    try:
        radio_rows, stations, current_flows, agent_rows, heartbeat, switch, ack, lag = await asyncio.gather(
            q(aggregate_sql('ap_radio',ap_where,step,
                'avg(util_busy_pct) AS busy,avg(util_tx_pct) AS tx,avg(util_rx_pct) AS rx,avg(util_other_pct) AS other,'
                'avg(tx_bytes) AS tx_bytes,avg(rx_bytes) AS rx_bytes,avg(tx_retry_pct) AS retry,avg(tx_fail_pct) AS fail,avg(noise_dbm) AS noise','radio')),
            q(f"SELECT sta_id,max(time) AS seen,{last_fields('ap_id radio rssi_dbm snr_db tx_mcs nss tx_phy_mbps tx_bytes rx_bytes tx_retry_pct')} FROM sta_link WHERE {live} GROUP BY sta_id"),
            q(f"SELECT flow_id,max(time) AS seen,last_value(ap_id ORDER BY time) AS ap_id,max(sla_state) AS worst FROM flow_kpi WHERE {flow_live} GROUP BY flow_id"),
            q(f"SELECT {last_fields('uptime_s cpu_pct mem_mb')} FROM ap_agent WHERE {recent}"),
            q(f"SELECT {last_fields('heartbeat_ok msgs_ps lag_ms_p95')},max(time) AS seen FROM ap_ingestion WHERE {recent}") if engineer else empty(),
            q(f"SELECT {last_fields('oper_up speed_mbps')} FROM switch_port WHERE {predicate(site,now-300000,now)} AND switch_id={literal(ap['switch_id'])} AND port={literal(ap['switch_port'])}") if ap['switch_id'] and ap['switch_port'] else empty(),
            q(aggregate_sql('ack_fb',ap_where,step,'last_value(lat_p50_ms ORDER BY time) AS p50,last_value(lat_p95_ms ORDER BY time) AS p95,avg(missing_ps) AS missing,avg(late_ps) AS late')) if engineer else empty(),
            q(aggregate_sql('ap_ingestion',f'{predicate(site,now-3600000,now)} AND ap_id={aid}',60,'max(lag_ms_p95) AS lag')) if engineer else empty())
        current = {r['flow_id']:r for r in current_flows if r['flow_id'] in configs and r.get('ap_id')==ap_id and now-epoch(r['seen'])<=3000}
        current_stas = {r['sta_id']:r for r in stations if r.get('ap_id')==ap_id and now-epoch(r['seen'])<=3000}
        flow_rows, sparks = [], []
        if current:
            ids = ','.join(literal(i) for i in current)
            flow_where = f'{where} AND flow_id IN ({ids})'
            spark_step = max(step, math.ceil((window['to']-window['from'])/40000))
            flow_rows,sparks = await asyncio.gather(
                q(f"SELECT flow_id,{last_fields('lat_p50_ms lat_p95_ms lat_p99_ms jitter_ms thpt_mbps')},sum(pkts) AS offered,sum(lost_pkts) AS lost,sum(deadline_miss) AS misses,avg(CASE WHEN sla_state IN (0,1) THEN 100.0 WHEN sla_state=2 THEN 0.0 ELSE NULL END) AS compliance FROM flow_kpi WHERE {flow_where} GROUP BY flow_id"),
                q(aggregate_sql('flow_kpi',flow_where,spark_step,'last_value(lat_p95_ms ORDER BY time) AS p95,avg(thpt_mbps) AS thpt','flow_id')))
    except Exception as exc:
        logger.exception('AP detail telemetry query failed')
        raise AuthError(503,'INTERNAL_ERROR','Telemetry query failed or is unavailable.') from exc
    inventory = {r['sta_id']:r for r in await db.fetch('''SELECT sta_id,hostname,label,device_type,mac::text AS mac,mac_randomised FROM client WHERE sta_id=ANY($1::text[])''',list(current_stas))} if current_stas else {}
    counts=Counter(c['sta_id'] for c in configs.values())
    clients=[]
    rssi_warn=threshold('r-client-rssi',-70); retry_warn=threshold('r-client-retry',10)
    for sid,r in current_stas.items():
        c=inventory.get(sid,{})
        rssi=number(r.get('rssi_dbm')); retry=number(r.get('tx_retry_pct'))
        weak=(rssi is not None and rssi_warn is not None and rssi<rssi_warn) or (retry is not None and retry_warn is not None and retry>retry_warn)
        # Current simulator publishes station link byte counts from the AP perspective.
        clients.append({'staId':sid,'name':c.get('hostname') or c.get('label') or sid,'type':c.get('device_type','unknown'),
            'mac':mac_view(c.get('mac'),permissions),'randomised':c.get('mac_randomised',False),'apId':ap_id,'ap':ap['name'],
            'radio':r.get('radio'),'rssi':rssi,'snr':number(r.get('snr_db')),'mcs':number(r.get('tx_mcs')),'nss':number(r.get('nss')),
            'phyMbps':number(r.get('tx_phy_mbps')),'dl':rate(r.get('tx_bytes')),'ul':rate(r.get('rx_bytes')),'retry':retry,
            'flows':counts[sid],'connectedSince':None,'signal':'warn' if weak else 'good' if rssi is not None else 'neutral'})
    clients.sort(key=lambda c:(-( (c['dl'] or 0)+(c['ul'] or 0)),c['staId']))
    metrics={r['flow_id']:r for r in flow_rows}; histories=defaultdict(list)
    for r in sparks:
        histories[r['flow_id']].append(r)
    flows=[]
    for fid in sorted(current):
        c=configs[fid]; r=metrics.get(fid,{})
        bound=number(c['lat_bound_ms']); p95=number(r.get('lat_p95_ms')); offered=number(r.get('offered')); lost=number(r.get('lost'))
        label,tone={0:('In SLA','good'),1:('At risk','warn'),2:('Violating','crit')}.get(current[fid].get('worst'),('Unknown','neutral'))
        flows.append({'flowId':fid,'name':c['name'],'status':status(label,tone),'slaClass':c['sla_name'],
            'client':c['hostname'] or c['label'] or c['sta_id'] or 'Unassigned','staId':c['sta_id'],'ap':ap['name'],'apId':ap_id,'direction':c['direction'],
            'p50':number(r.get('lat_p50_ms')),'p95':p95,'p99':number(r.get('lat_p99_ms')),'bound':bound,'jitter':number(r.get('jitter_ms')),
            'thpt':number(r.get('thpt_mbps')),'minThpt':number(c['min_thpt_mbps']),'lossPct':100*lost/offered if offered and lost is not None else None,
            'deadlineMiss':number(r.get('misses')),'compliance':number(r.get('compliance')),'margin':p95/bound if p95 is not None and bound else None,
            'spark':[number(x.get('p95' if bound else 'thpt')) for x in histories[fid]][-40:]})
    grouped=defaultdict(list)
    for r in radio_rows: grouped[r['radio']].append(r)
    radios=[radio_charts(r,grouped[r['radio']],window,threshold('r-ap-util',60),threshold('r-ap-retry',10)) for r in listed['radios']]
    a=agent_rows[0] if agent_rows else {}; h=heartbeat[0] if heartbeat else {}; sw=switch[0] if switch else {}
    loc=ap['floor_name'] or 'Unassigned'
    if ap['x_m'] is not None and ap['y_m'] is not None: loc+=f" ({float(ap['x_m']):g} m, {float(ap['y_m']):g} m)"
    result={'summary':{'apId':ap_id,'name':ap['name'],'status':listed['status'],'model':ap['model'],'vendor':ap['vendor'],
        'firmware':ap['fw_version'],'location':loc,'mac':mac_view(ap['mac_text'],permissions),'ip':ap['ip_text'],'uptimeS':number(a.get('uptime_s')),
        'agentVersion':ap['agent_version'],'radios':', '.join(r['radio'] for r in radios),'clients':listed['clients'],'flows':len(flows),
        'switchPort':ap['switch_port'],'linkUp':sw.get('oper_up'),'linkSpeedMbps':number(sw.get('speed_mbps'))},
        'radios':radios,'clients':clients[:5],'clientsTotal':len(clients),'flows':flows}
    if engineer:
        lag_window={'from':now-3600000,'to':now,'bucketS':60}
        latency=chart(ack,window,'ms',[('p50','P50','p50'),('p95','P95','p95')]); latency['refs']=refs('ACK warn',threshold('r-ack-lat',5))
        latency['notes']='Last reported percentile in each bucket; not a recomputed packet-level percentile.'
        result['agent']={'heartbeat':h.get('heartbeat_ok') is True and h.get('seen') is not None and now-epoch(h['seen'])<=3000,
            'msgsPs':number(h.get('msgs_ps')),'lagP95Ms':number(h.get('lag_ms_p95')),'cpuPct':number(a.get('cpu_pct')),'memMb':number(a.get('mem_mb')),
            'lagSpark':chart(lag,lag_window,'ms',[('lag','Lag','lag')])}
        result['agent']['lagSpark']['notes']='Maximum reported p95 in each minute.'
        result['ack']={'latency':latency,'missingPs':chart(ack,window,'/s',[('missing','Missing','missing'),('late','Late','late')])}
    else:
        # The fixed dashboard always dereferences data.agent. Preserve permission
        # filtering with null values while providing the safe structural shell it
        # needs to render the AP detail page.
        result['agent']={'heartbeat':listed['agentState']=='connected','msgsPs':None,'lagP95Ms':None,
            'cpuPct':None,'memMb':None,'lagSpark':chart([],window,'ms',[('lag','Lag','lag')])}
    return JSONResponse(content={'meta':meta,'data':result},headers={'Cache-Control':'no-store'})
