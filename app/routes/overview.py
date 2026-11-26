"""First Monitor view. Raw SQL aggregation; no simulator-specific device IDs."""
import asyncio
import json
import logging
import math
import time
from datetime import datetime, timezone
from urllib.parse import quote

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from app.database.influx import query_influx
from app.middleware.auth import AuthError, require_user
from app.services.session import session_response
from app.services.ap_health import assess_ap

router = APIRouter(prefix='/api/v1/adwiser', tags=['Monitor'])
logger = logging.getLogger(__name__)
STEPS = (1,2,5,10,15,30,60,120,300,600,900,1800,3600,7200,18000,36000,72000,180000,360000)


def now_ms():
    return time.time_ns() // 1_000_000


def epoch(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def number(value):
    return float(value) if value is not None and math.isfinite(float(value)) else None


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def predicate(site, start, end):
    def ts(ms):
        return 'TIMESTAMP ' + literal(datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat().replace('+00:00', 'Z'))
    return f"site_id = {literal(site)} AND time >= {ts(start)} AND time < {ts(end)}"


def scope_window(params, permissions, now):
    spans = {'live':300000,'15m':900000,'1h':3600000,'6h':21600000,'24h':86400000,'7d':604800000}
    name = params.get('range', 'live')
    try:
        if name == 'custom':
            start, end = int(params['from']), int(params['to'])
            if not 0 <= start < end or end > now or end-start > 400*86400000:
                raise ValueError
        else:
            if 'from' in params or 'to' in params:
                raise ValueError
            start, end = now-spans[name], now
    except (KeyError, ValueError, OverflowError):
        raise AuthError(400, 'INVALID_REQUEST', 'Invalid time range.')
    span = end-start
    minimum = 1 if span<=900000 else 10 if span<=21600000 else 60 if span<=604800000 else 3600
    if 'resolution.1s' not in permissions:
        minimum = max(minimum, 5)
    step = next(s for s in STEPS if s>=minimum and math.ceil((end - (start//(s*1000))*(s*1000))/(s*1000))<=600)
    return {'range':name,'from':start,'to':end,'bucketS':step,'tier':'raw'}


def refresh_seconds(window, permissions):
    """Avoid repeatedly scanning raw telemetry for a historical window."""
    if window['range'] != 'live':
        # The fixed dashboard accepts only a positive interval. A day makes an
        # immutable historical view effectively static until navigation.
        return 86400
    return 1 if 'resolution.1s' in permissions else 5


def chart(rows, window, unit, fields):
    step = window['bucketS']*1000
    start = window['from']//step*step
    count = math.ceil((window['to']-start)/step)
    series = [{'key':key,'label':label,'values':[None]*count} for key,label,col in fields]
    for r in rows:
        i = (epoch(r['bucket'])-start)//step
        if 0<=i<count:
            for item, (_,_,col) in zip(series,fields):
                item['values'][i]=number(r.get(col))
    return {'start':start,'stepMs':step,'unit':unit,'decimals':1,'series':series}


def status(label, tone):
    return {'label':label,'tone':tone}


def kpi(suffix,label,value,context,tone='neutral',**extra):
    return {'id':'S1-W1'+suffix,'label':label,'value':str(value) if value is not None else '—','context':context,'tone':tone,**extra}


def site_view(row):
    return {'siteId':row['site_id'],'name':row['name'],'timezone':row['timezone'],'slaTargetPct':number(row['sla_target_pct'])}


def alert_view(row):
    kind, ident = row['entity_type'], row['entity_id']
    encoded = quote(ident, safe='')
    link = {'ap':f'/aps/{encoded}','client':f'/clients?client={encoded}','flow':f'/flows/{encoded}','controller':'/scheduler'}.get(kind)
    return {'alertId':row['alert_id'],'title':row['title'],'severity':row['severity'],'source':row['source'],'entityType':kind,'entityId':ident,'entityName':row['entity_name'] or ident,'entityLink':link,'score':number(row['score']),'startedAt':epoch(row['started_at']),'endedAt':epoch(row['ended_at']),'status':row['status']}


async def closest_flows(db, client, site, where, configs, ap_names, window):
    eligible = {r['flow_id']:r for r in configs if r['lat_bound_ms'] and r['lat_bound_ms']>0}
    if not eligible:
        return []
    ids = ','.join(literal(i) for i in eligible)
    rows = await query_influx(client, f'''SELECT flow_id,
        last_value(ap_id ORDER BY time) AS ap_id,
        last_value(lat_p50_ms ORDER BY time) AS p50,
        last_value(lat_p95_ms ORDER BY time) AS p95,
        last_value(lat_p99_ms ORDER BY time) AS p99,
        last_value(jitter_ms ORDER BY time) AS jitter,
        last_value(thpt_mbps ORDER BY time) AS thpt,
        last_value(sla_state ORDER BY time) AS sla_state,
        sum(pkts) AS offered, sum(lost_pkts) AS lost,
        sum(deadline_miss) AS misses,
        avg(CASE WHEN sla_state=2 THEN 0.0 WHEN sla_state IN (0,1) THEN 100.0 ELSE NULL END) AS compliance
        FROM flow_kpi WHERE {where} AND flow_id IN ({ids}) GROUP BY flow_id''')
    out=[]
    for r in rows:
        cfg=eligible[r['flow_id']]; bound=number(cfg['lat_bound_ms']); p95=number(r.get('p95'))
        if p95 is None: continue
        aid=r.get('ap_id'); offered=number(r.get('offered')); lost=number(r.get('lost'))
        label,tone={0:('In SLA','good'),1:('At risk','warn'),2:('Violating','crit')}.get(r.get('sla_state'),('Unknown','neutral'))
        out.append({'flowId':r['flow_id'],'name':cfg['name'],'status':status(label,tone),'slaClass':cfg['sla_name'],'client':cfg['hostname'] or cfg['label'] or cfg['sta_id'],'staId':cfg['sta_id'],'ap':ap_names.get(aid,aid),'apId':aid,'direction':cfg['direction'],'p50':number(r.get('p50')),'p95':p95,'p99':number(r.get('p99')),'bound':bound,'jitter':number(r.get('jitter')),'thpt':number(r.get('thpt')),'minThpt':number(cfg['min_thpt_mbps']),'lossPct':100*lost/offered if offered and lost is not None else None,'deadlineMiss':number(r.get('misses')),'compliance':number(r.get('compliance')),'margin':p95/bound,'spark':[]})
    out.sort(key=lambda r:r['margin'],reverse=True); out=out[:5]
    if out:
        ids=','.join(literal(r['flowId']) for r in out)
        step=max(1,math.ceil((window['to']-window['from'])/40000))
        if window['bucketS']>step: step=window['bucketS']
        samples=await query_influx(client,f"SELECT flow_id,date_bin(INTERVAL '{step} seconds',time) AS bucket,avg(lat_p95_ms) AS p95 FROM flow_kpi WHERE {where} AND flow_id IN ({ids}) GROUP BY flow_id,bucket ORDER BY bucket")
        # Influx can omit an aggregate column for a sparse bucket. Keep that
        # bucket as unavailable instead of failing the entire Overview route.
        for r in out: r['spark']=[number(s.get('p95')) for s in samples if s.get('flow_id')==r['flowId']][-40:]
    return out


@router.get('/overview')
async def overview(request: Request, user=Depends(require_user)):
    db=request.app.state.pg_pool; client=request.app.state.influx_client
    session=await session_response(db,user); permissions=session['permissions']; now=now_ms()
    window=scope_window(request.query_params,permissions,now)
    sites=await db.fetch('SELECT site_id,name,timezone,sla_target_pct FROM site ORDER BY name,site_id')
    requested=request.query_params.get('site')
    chosen=next((s for s in sites if s['site_id']==requested),None) if requested is not None else (sites[0] if sites else None)
    if chosen is None: raise AuthError(400 if sites else 503,'INVALID_REQUEST' if sites else 'INTERNAL_ERROR','Unknown site.' if sites else 'No sites configured.')
    site=chosen['site_id']; step=window['bucketS']; where=predicate(site,window['from'],window['to']); recent=predicate(site,now-300000,now)
    aps=await db.fetch('SELECT ap_id,name,floor_id,x_m,y_m FROM ap WHERE site_id=$1 AND retired_at IS NULL ORDER BY ap_id',site)
    floors=await db.fetch('SELECT * FROM floor WHERE site_id=$1 ORDER BY name',site)
    configs=await db.fetch('''SELECT f.flow_id,f.name,f.sta_id,f.direction,c.hostname,c.label,s.name AS sla_name,s.lat_bound_ms,s.min_thpt_mbps FROM flow f JOIN client c ON c.sta_id=f.sta_id LEFT JOIN sla_class s ON s.sla_class_id=f.sla_class_id WHERE f.site_id=$1 AND f.retired_at IS NULL AND f.managed IS TRUE''',site)
    threshold=await db.fetchval("SELECT threshold FROM alert_rule WHERE rule_id='r-ap-util'")
    util_warn=number(threshold) if threshold is not None else 60.0
    counts={'critical':0,'warning':0,'info':0}
    for r in await db.fetch("SELECT severity,count(*) AS n FROM alert WHERE site_id=$1 AND ended_at IS NULL AND status<>'resolved' GROUP BY severity",site): counts[r['severity']]=r['n']
    alerts=await db.fetch('''SELECT a.*, COALESCE(p.name,c.hostname,c.label,f.name) AS entity_name FROM alert a LEFT JOIN ap p ON a.entity_type='ap' AND p.ap_id=a.entity_id LEFT JOIN client c ON a.entity_type='client' AND c.sta_id=a.entity_id LEFT JOIN flow f ON a.entity_type='flow' AND f.flow_id=a.entity_id WHERE a.site_id=$1 AND a.ended_at IS NULL AND a.status<>'resolved' ORDER BY a.started_at DESC LIMIT 5''',site)
    sql=f'''SELECT date_bin(INTERVAL '{step} seconds',time) AS bucket,
        max(worst_flow_p95_ms) AS worst,avg(median_flow_p95_ms) AS median,
        first_value(worst_flow_id ORDER BY worst_flow_p95_ms DESC NULLS LAST, time DESC) AS worst_id,
        avg(dl_mbps) AS dl,avg(ul_mbps) AS ul,max(flows_violating) AS violating,
        first_value(clients ORDER BY time) AS first_clients,last_value(clients ORDER BY time) AS clients,
        last_value(flows_in_sla ORDER BY time) AS in_sla,last_value(flows_at_risk ORDER BY time) AS at_risk,
        last_value(flows_violating ORDER BY time) AS last_violating,last_value(flows_managed ORDER BY time) AS managed,
        sum(flows_managed) AS observations,sum(flows_violating) AS violations
        FROM site_kpi WHERE {where} GROUP BY bucket ORDER BY bucket'''
    try:
        rows, radio, agents, hosts, schedules=await asyncio.gather(
            query_influx(client,sql),
            query_influx(client,f'SELECT ap_id,radio,last_value(util_busy_pct ORDER BY time) AS util,last_value(n_clients ORDER BY time) AS clients,max(time) AS seen FROM ap_radio WHERE {recent} GROUP BY ap_id,radio'),
            query_influx(client,f'SELECT ap_id,last_value(heartbeat_ok ORDER BY time) AS heartbeat_ok,max(time) AS seen FROM ap_ingestion WHERE {recent} GROUP BY ap_id'),
            query_influx(client,f'SELECT max(time) AS seen FROM ctrl_host WHERE {recent}'),
            query_influx(client,f'SELECT time,schedule_ver FROM sched_stats WHERE {recent} ORDER BY time DESC LIMIT 1'))
        closest=await closest_flows(db,client,site,where,configs,{a['ap_id']:a['name'] for a in aps},window)
    except Exception as exc:
        logger.exception('Overview telemetry query failed')
        raise AuthError(503,'INTERNAL_ERROR','Telemetry query failed or is unavailable.') from exc
    by_agent={r['ap_id']:r for r in agents}; per_ap=[]; offline=[]; unknown=[]; online=0
    for a in aps:
        aid=a['ap_id']; agent=by_agent.get(aid); rads=[r for r in radio if r['ap_id']==aid]
        fresh=[r for r in rads if r.get('seen') is not None and 0<=now-epoch(r['seen'])<=3000]
        util=max((number(r['util']) for r in fresh if r.get('util') is not None),default=None)
        heartbeat_age=None if not agent or agent.get('seen') is None else (now-epoch(agent['seen']))/1000
        is_online,state=assess_ap(agent.get('heartbeat_ok') if agent else None,heartbeat_age,
                                  [(now-epoch(r['seen']))/1000 for r in rads if r.get('seen') is not None],
                                  ['High airtime utilisation'] if util is not None and util>util_warn else [])
        if is_online: online+=1
        elif state['label']=='Offline': offline.append(a['name'])
        else: unknown.append(a['name'])
        per_ap.append({'apId':aid,'name':a['name'],'floorId':a['floor_id'],'xM':number(a['x_m']),'yM':number(a['y_m']),'clients':sum(int(r['clients'] or 0) for r in fresh) if fresh else None,'status':state,'util':util})
    first=rows[0] if rows else {}; last=rows[-1] if rows else {}; clients=last.get('clients'); beginning=first.get('first_clients')
    delta=clients-beginning if clients is not None and beginning is not None else None
    obs=sum(r.get('observations') or 0 for r in rows); vio=sum(r.get('violations') or 0 for r in rows)
    compliance=100*(obs-vio)/obs if obs else None; target=number(chosen['sla_target_pct'])
    host_seen=epoch(hosts[0].get('seen')) if hosts else None
    controller='Unknown' if host_seen is None else 'Stale' if now-host_seen>30000 else 'Healthy'
    schedule=schedules[0]['schedule_ver'] if schedules else None
    ap_context=(', '.join(offline)+' offline' if offline else '') + (('; ' if offline else '')+', '.join(unknown)+' unknown' if unknown else '')
    kpis=[kpi('a','APs online',f'{online} / {len(aps)}',ap_context or ('All APs online' if aps else 'No registered APs'),'warn' if offline or unknown else 'good' if aps else 'neutral',link='/aps?status=offline'),
          kpi('b','Clients',clients,f'{delta:+} vs range start' if delta is not None else 'Change since range start unavailable'),
          kpi('c','Managed flows',last.get('managed'),f"{last.get('in_sla')} in SLA, {last.get('at_risk')} at risk, {last.get('last_violating')} violating" if rows else 'No telemetry','crit' if last.get('last_violating') else 'warn' if last.get('at_risk') else 'good' if rows else 'neutral'),
          kpi('d','SLA compliance',f'{compliance:.1f}%' if compliance is not None else None,f'Target ≥ {target:g}%' if target is not None else 'No target','neutral' if compliance is None or target is None else 'good' if compliance>=target else 'warn',unit='%'),
          kpi('e','Open alerts',sum(counts.values()),f"{counts['critical']} critical, {counts['warning']} warning",'crit' if counts['critical'] else 'warn' if counts['warning'] else 'good'),
          kpi('f','Controller',controller,f'Schedule v{schedule}' if schedule is not None else 'Schedule unavailable','good' if controller=='Healthy' else 'warn' if controller=='Stale' else 'neutral',eng=True,**({'link':'/scheduler'} if 'scheduler.view' in permissions else {}))]
    latency=chart(rows,window,'ms',[('worst','Worst flow p95','worst'),('median','Median flow p95','median')]); throughput=chart(rows,window,'Mbps',[('dl','Downlink','dl'),('ul','Uplink','ul')])
    bounds=[float(r['lat_bound_ms']) for r in configs if r['lat_bound_ms'] is not None]; latency['refs']=[{'label':'Tightest SLA bound','value':min(bounds)}] if bounds else []
    latency['notes']=[None]*len(latency['series'][0]['values']); latency['bands']=[]; flow_names={r['flow_id']:r['name'] for r in configs}
    for r in rows:
        t=epoch(r['bucket']); i=(t-latency['start'])//latency['stepMs']; name=flow_names.get(r.get('worst_id'))
        if 0<=i<len(latency['notes']):
            if name: latency['notes'][i]='Worst: '+name
            if (r.get('violating') or 0)>0: latency['bands'].append({'from':max(t,window['from']),'to':min(t+latency['stepMs'],window['to'])})
    floor_data=[]
    for f in floors:
        rooms=f['rooms']; rooms=json.loads(rooms) if isinstance(rooms,str) else rooms
        floor_data.append({'floorId':f['floor_id'],'name':f['name'],'widthM':number(f['width_m']),'heightM':number(f['height_m']),'rooms':rooms,'planImageUrl':f['plan_image_url'],'aps':[{k:v for k,v in a.items() if k!='floorId'} for a in per_ap if a['floorId']==f['floor_id']]})
    data={'kpis':kpis,'floors':floor_data,'latency':latency,'throughput':throughput,'utilByAp':[{'label':a['name'],'value':a['util'],'tone':'neutral' if a['util'] is None else 'warn' if a['util']>util_warn else 'good','link':'/aps/'+quote(a['apId'],safe='')} for a in per_ap],'utilWarn':util_warn,'closest':closest,'recentAlerts':[alert_view(a) for a in alerts]}
    meta={'user':session['user'],'permissions':permissions,'sites':[site_view(s) for s in sites],'site':site_view(chosen),'window':window,'generatedAt':now_ms(),'source':'api','openAlerts':counts,'refreshS':refresh_seconds(window, permissions)}
    return JSONResponse(content={'meta':meta,'data':data},headers={'Cache-Control':'no-store'})
