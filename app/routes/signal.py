"""Signal dashboard snapshots and SSE from the generator's three Signal measurements."""
import asyncio
import json
import statistics
import time
import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from app.database.influx import query_influx
from app.middleware.auth import AuthError
from app.routes.overview import literal, now_ms, number, epoch

router=APIRouter(prefix='/api/v1',tags=['Signal dashboard'])
RANGES={'15m':(15,60,15),'1h':(12,300,60),'12h':(24,1800,720),'24h':(24,3600,1440),'7d':(28,21600,10080)}
BANDS={'2.4 GHz','5 GHz','6 GHz'}
CACHE={}
LOCKS={}


def output(data,warnings=None,stale=False,stale_for=None):
    warnings=warnings or []
    meta={'requestId':str(uuid.uuid4()),'generatedAt':datetime.now(timezone.utc).isoformat().replace('+00:00','Z'),
          'stale':stale,'partial':bool(data and data.get('missing')),'warnings':warnings}
    if stale_for is not None: meta['staleFor']=stale_for
    return {'data':data,'meta':meta}


async def filters_data(influx):
    rows=await query_influx(influx,"SELECT DISTINCT site_name,ssid FROM client_performance WHERE time >= now() - INTERVAL '7 days'")
    return {'sites':sorted({r['site_name'] for r in rows if r.get('site_name')}),
            'ssids':sorted({r['ssid'] for r in rows if r.get('ssid')})}


async def params(request):
    p=request.query_params
    name=p.get('range','12h'); site=p.get('site'); ssid=p.get('ssid'); band=p.get('band')
    if name not in RANGES: raise AuthError(400,'INVALID_REQUEST','range must be one of 15m, 1h, 12h, 24h, 7d.')
    if band and band not in BANDS: raise AuthError(400,'INVALID_REQUEST','Invalid band.')
    if site or ssid:
        try:choices=await filters_data(request.app.state.influx_client)
        except Exception:raise AuthError(503,'INFLUX_UNAVAILABLE','Telemetry source is unavailable.')
        if site and site not in choices['sites']: raise AuthError(400,'INVALID_REQUEST','Unknown site.')
        if ssid and ssid not in choices['ssids']: raise AuthError(400,'INVALID_REQUEST','Unknown SSID.')
    return name,site,ssid,band


def where(start,end,site,ssid,band,ap=False):
    from datetime import datetime,timezone
    ts=lambda n: 'TIMESTAMP '+literal(datetime.fromtimestamp(n/1000,timezone.utc).isoformat().replace('+00:00','Z'))
    terms=[f'time >= {ts(start)}',f'time < {ts(end)}']
    if site: terms.append('site_name='+literal(site))
    if ssid and not ap: terms.append('ssid='+literal(ssid))
    if band: terms.append('band='+literal(band))
    return ' AND '.join(terms)


async def build(request,key):
    name,site,ssid,band=key
    influx=request.app.state.influx_client
    buckets,step,minutes=RANGES[name]
    end=now_ms(); start=end-minutes*60000
    # Client cards use a fixed recent window, independent of graph range.
    recent=where(end-900000,end,site,ssid,band)
    window=where(start,end,site,ssid,band)
    rows=await query_influx(influx,f'''SELECT client_id,first_value(ap_name ORDER BY time DESC) AS ap_name,
      avg(health_score) AS utility,avg(rssi_dbm) AS rssi,avg(throughput_down_mbps) AS down,
      avg(throughput_up_mbps) AS up,avg(latency_ms) AS latency,avg(airtime_pct) AS airtime,
      avg(packet_error_rate_pct) AS packet_error,avg(packet_loss_pct) AS loss,avg(retry_rate_pct) AS retry
      FROM client_performance WHERE {recent} GROUP BY client_id''')
    if not rows: return None,[]
    previous=await query_influx(influx,f'''SELECT client_id,avg(health_score) AS utility,
      avg(throughput_down_mbps) AS down,avg(throughput_up_mbps) AS up,
      avg(packet_loss_pct) AS loss,avg(retry_rate_pct) AS retry
      FROM client_performance WHERE {where(end-4500000,end-3600000,site,ssid,band)} GROUP BY client_id''')
    clients=[]
    for r in rows:
        rssi=number(r.get('rssi'))
        clients.append({'client':r['client_id'],'ap':r.get('ap_name') or '',
          'signal':'neutral' if rssi is None else 'good' if rssi>=-67 else 'crit' if rssi<-75 else 'warn',
          'rssi':rssi,'throughputDown':number(r.get('down')),'throughputUp':number(r.get('up')),
          'latency':number(r.get('latency')),'airtime':number(r.get('airtime')),
          'packetError':number(r.get('packet_error')),'utility':number(r.get('utility'))})
    clients.sort(key=lambda r:r['rssi'] if r['rssi'] is not None else 0)
    # Aggregate per aligned bucket in Influx, then place results in bounded arrays.
    aligned=end//(step*1000)*(step*1000)
    first=aligned-(buckets-1)*step*1000
    series_where=where(first,end,site,ssid,band)
    series=await query_influx(influx,f'''SELECT client_id,date_bin(INTERVAL '{step} seconds',time) AS bucket,
      avg(rssi_dbm) AS rssi,avg(throughput_down_mbps) AS throughputDown,
      avg(throughput_up_mbps) AS throughputUp,avg(latency_ms) AS latency,
      avg(airtime_pct) AS airtime,avg(packet_error_rate_pct) AS packetError,
      avg(health_score) AS utility FROM client_performance WHERE {series_where}
      GROUP BY client_id,bucket ORDER BY bucket''')
    by_client={}
    fields=('rssi','throughputDown','throughputUp','latency','airtime','packetError','utility')
    for r in series:
        i=(epoch(r['bucket'])-first)//(step*1000)
        if 0<=i<buckets:
            item=by_client.setdefault(r['client_id'],{k:[None]*buckets for k in fields})
            for k in fields:item[k][i]=number(r.get(k))
    for c in clients[:10]:c['series']=by_client.get(c['client'],{k:[None]*buckets for k in fields})
    ap_map={}
    for c in clients:
        a=ap_map.setdefault(c['ap'],{'ap':c['ap'],'clients':0,'queueBacklog':None,'trafficDown':0.0,'trafficUp':0.0})
        a['clients']+=1
        a['trafficDown']+=c['throughputDown'] or 0
        a['trafficUp']+=c['throughputUp'] or 0
    for a in ap_map.values():
        members=[c for c in clients if c['ap']==a['ap']]
        if not any(c['throughputDown'] is not None for c in members):a['trafficDown']=None
        if not any(c['throughputUp'] is not None for c in members):a['trafficUp']=None
    warnings=[]
    ap_where=where(end-900000,end,site,None,band,ap=True)
    ap_series=[]
    try:
        health=await query_influx(influx,f'''SELECT ap_name,last_value(queue_backlog ORDER BY time) AS queue
          FROM ap_health WHERE {ap_where} GROUP BY ap_name''')
        for r in health:
            if r['ap_name'] in ap_map:ap_map[r['ap_name']]['queueBacklog']=number(r['queue'])
        ap_series=await query_influx(influx,f'''SELECT ap_name,date_bin(INTERVAL '{step} seconds',time) AS bucket,
          avg(queue_backlog) AS queue FROM ap_health WHERE {where(first,end,site,None,band,ap=True)}
          GROUP BY ap_name,bucket ORDER BY bucket''')
        if ssid:warnings.append('SSID filter is not available for AP queue data.')
    except Exception:
        warnings.append('AP queue data is unavailable.')
    aps=sorted(ap_map.values(),key=lambda a:-a['clients'])
    # AP time series are derived from the client bucket series for the ten displayed APs.
    for a in aps[:10]:
        members=[c for c in clients if c['ap']==a['ap']]
        series={'clients':[],'queueBacklog':[None]*buckets,'trafficDown':[],'trafficUp':[]}
        for i in range(buckets):
            present=[by_client[c['client']] for c in members if c['client'] in by_client and
                     any(by_client[c['client']][field][i] is not None for field in fields)]
            series['clients'].append(len(present) if present else None)
            for key,field in (('trafficDown','throughputDown'),('trafficUp','throughputUp')):
                values=[item[field][i] for item in present if item[field][i] is not None]
                series[key].append(sum(values) if values else None)
        a['series']=series
    for r in ap_series:
        a=ap_map.get(r['ap_name'])
        if not a or 'series' not in a:continue
        i=(epoch(r['bucket'])-first)//(step*1000)
        if 0<=i<buckets:a['series']['queueBacklog'][i]=number(r.get('queue'))
    apps=[]
    try:
        app_rows=await query_influx(influx,f'''SELECT ap_name,application,sum(bytes_down) AS down,sum(bytes_up) AS up
          FROM application_experience WHERE {window} GROUP BY ap_name,application''')
        apps=[{'ap':r['ap_name'] or '', 'app':r['application'],'bytesDown':int(r['down']),'bytesUp':int(r['up'])} for r in app_rows]
    except Exception:
        warnings.append('Application load is unavailable.')
    utility=[c['utility'] for c in clients if c['utility'] is not None]
    downs=[c['throughputDown'] for c in clients if c['throughputDown'] is not None]
    ups=[c['throughputUp'] for c in clients if c['throughputUp'] is not None]
    losses=sorted(number(r['loss']) for r in rows if r.get('loss') is not None)
    p95=losses[min(len(losses)-1,int(.95*len(losses)))] if losses else None
    healthy=sum(v>=70 for v in utility)
    retries=sum(number(r['retry'])>15 for r in rows if r.get('retry') is not None)
    prev_healthy=sum(number(r['utility'])>=70 for r in previous if r.get('utility') is not None)
    prev_retries=sum(number(r['retry'])>15 for r in previous if r.get('retry') is not None)
    prev_down=[number(r['down']) for r in previous if r.get('down') is not None]
    prev_up=[number(r['up']) for r in previous if r.get('up') is not None]
    prev_loss=sorted(number(r['loss']) for r in previous if r.get('loss') is not None)
    prev_p95=prev_loss[min(len(prev_loss)-1,int(.95*len(prev_loss)))] if prev_loss else None
    context=lambda current,old,unit: f'{current-old:+.1f} {unit} vs an hour ago' if current is not None and old is not None else 'Previous hour unavailable'
    kpis=[{'label':'Healthy clients','value':str(healthy),'context':context(healthy,prev_healthy if previous else None,'clients'),'tone':'good'},
          {'label':'High retry rate','value':str(retries),'context':context(retries,prev_retries if previous else None,'clients'),'tone':'warn' if retries else 'good'},
          {'label':'Median download','value':f'{statistics.median(downs):.1f}' if downs else '—','unit':'Mbps',
           'context':context(statistics.median(downs) if downs else None,statistics.median(prev_down) if prev_down else None,'Mbps'),'tone':'neutral'},
          {'label':'Median upload','value':f'{statistics.median(ups):.1f}' if ups else '—','unit':'Mbps',
           'context':context(statistics.median(ups) if ups else None,statistics.median(prev_up) if prev_up else None,'Mbps'),'tone':'neutral'},
          {'label':'Packet loss P95','value':f'{p95:.1f}%' if p95 is not None else '—',
           'context':context(p95,prev_p95,'%'),'tone':'neutral'}]
    return {'kpis':kpis,'breakdown':{'clients':clients[:50],'aps':aps,'apps':apps,
      'timeline':{'endsAt':end,'spanMinutes':(buckets-1)*step//60,'window':{'15m':'15 minute window','1h':'1 hour window','12h':'12 hour window','24h':'24 hour window','7d':'7 day window'}[name]}}},warnings


async def snapshot(request,key):
    now=time.monotonic(); cached=CACHE.get(key)
    if cached and now-cached[0]<8:return output(cached[1],cached[2])
    lock=LOCKS.setdefault(key,asyncio.Lock())
    async with lock:
        cached=CACHE.get(key)
        if cached and time.monotonic()-cached[0]<8:return output(cached[1],cached[2])
        try:
            data,warnings=await build(request,key)
            if len(CACHE)>=32:CACHE.pop(next(iter(CACHE)))
            CACHE[key]=(time.monotonic(),data,warnings)
            return output(data,warnings)
        except Exception:
            if cached and time.monotonic()-cached[0]<300:
                return output(cached[1],cached[2],True,int(time.monotonic()-cached[0]))
            raise AuthError(503,'INFLUX_UNAVAILABLE','Telemetry source is unavailable.')


@router.get('/filters')
async def filters(request:Request):
    try:data=await filters_data(request.app.state.influx_client)
    except Exception:raise AuthError(503,'INFLUX_UNAVAILABLE','Telemetry source is unavailable.')
    return JSONResponse(data,headers={'Cache-Control':'no-store'})


@router.get('/dashboard/performance')
async def performance(request:Request):
    key=await params(request)
    return JSONResponse(await snapshot(request,key),headers={'Cache-Control':'no-store'})


@router.get('/stream/performance')
async def performance_stream(request:Request):
    key=await params(request)
    async def stream():
        yield 'retry: 5000\n\nevent: config\ndata: {"intervalSeconds":10}\n\n'
        last_keep=time.monotonic()
        while not await request.is_disconnected():
            try:
                result=await snapshot(request,key)
                yield 'event: snapshot\ndata: '+json.dumps(result,separators=(',',':'))+'\n\n'
            except Exception:
                yield 'event: unavailable\ndata: {}\n\n'
            for _ in range(10):
                await asyncio.sleep(1)
                if await request.is_disconnected():return
                if time.monotonic()-last_keep>=15:
                    yield ': keep-alive\n\n';last_keep=time.monotonic()
    return StreamingResponse(stream(),media_type='text/event-stream',headers={'Cache-Control':'no-store','X-Accel-Buffering':'no'})
