"""SLA ledger and flow detail. All telemetry reads are site scoped."""
import asyncio
import math
from fastapi import APIRouter, Depends, Request
from app.database.influx import query_influx
from app.middleware.auth import AuthError, require_user
from app.routes.overview import now_ms, epoch, number, predicate, chart, status, literal
from app.services.monitor import scope, envelope

router = APIRouter(prefix='/api/v1/adwiser', tags=['Monitor flows'])


def row_for(f, t, ap_names, permissions, now):
    seen = epoch(t.get('seen'))
    fresh = seen is not None and 0 <= now-seen <= 5000
    t = t if fresh else {}
    bound = number(f['lat_bound_ms'])
    p95 = number(t.get('p95'))
    state = {0: status('In SLA', 'good'), 1: status('At risk', 'warn'), 2: status('Violating', 'crit')}.get(t.get('sla_state'), status('Unknown', 'neutral'))
    offered, lost = number(t.get('offered')), number(t.get('lost'))
    aid = t.get('ap_id')
    row = {'flowId': f['flow_id'], 'name': f['name'], 'status': state, 'slaClass': f['sla_name'],
           'client': f['hostname'] or f['label'] or f['sta_id'], 'staId': f['sta_id'],
           'ap': ap_names.get(aid, aid), 'apId': aid, 'direction': f['direction'],
           'p50': number(t.get('p50')), 'p95': p95, 'p99': number(t.get('p99')),
           'bound': bound, 'jitter': number(t.get('jitter')), 'thpt': number(t.get('thpt')),
           'minThpt': number(f['min_thpt_mbps']),
           'lossPct': 100*lost/offered if offered and lost is not None else None,
           'deadlineMiss': number(t.get('misses')), 'compliance': number(t.get('compliance')),
           'margin': p95/bound if p95 is not None and bound and bound > 0 else None,
           'spark': []}
    if 'details.engineering' in permissions:
        row['queueDepth'] = number(t.get('depth_pkts'))
    return row


async def ledger(request, user, selected_flow=None, include_sparks=True):
    db, influx = request.app.state.pg_pool, request.app.state.influx_client
    site, meta = await scope(request, user)
    now = now_ms()
    w = meta['window']
    flow_sql='''SELECT f.*,s.name AS sla_name,s.lat_bound_ms,s.min_thpt_mbps,s.max_loss_pct,s.max_jitter_ms,
        s.lat_percentile,s.eval_window_s,c.hostname,c.label FROM flow f JOIN client c ON c.sta_id=f.sta_id
        LEFT JOIN sla_class s ON s.sla_class_id=f.sla_class_id WHERE f.site_id=$1 AND f.managed IS TRUE
        AND f.retired_at IS NULL'''
    flow_args=[site]
    if selected_flow is not None:
        flow_sql+=' AND f.flow_id=$2'
        flow_args.append(selected_flow)
    flows = await db.fetch(flow_sql+' ORDER BY f.flow_id',*flow_args)
    aps = await db.fetch('SELECT ap_id,name FROM ap WHERE site_id=$1 AND retired_at IS NULL ORDER BY name', site)
    ap_names = {a['ap_id']: a['name'] for a in aps}
    where = predicate(site, w['from'], w['to'])
    telemetry_filter = f' AND flow_id={literal(selected_flow)}' if selected_flow is not None else ''
    flow_where = where + telemetry_filter
    duration = await db.fetchval("SELECT duration_s FROM alert_rule WHERE rule_id='r-flow-violation'")
    recent = predicate(site, now-max(5, duration or 30)*1000, now) + telemetry_filter
    if selected_flow is not None and not flows:
        return site, meta, flows, [], aps
    # A grouped range query provides range totals; a separate grouped latest query
    # prevents an old sample in a historical range being presented as current.
    aggregate_sql = f'''SELECT flow_id,sum(pkts) AS offered,sum(lost_pkts) AS lost,
      sum(deadline_miss) AS misses,avg(CASE WHEN sla_state=2 THEN 0.0 WHEN sla_state IN (0,1) THEN 100.0 ELSE NULL END) AS compliance
      FROM flow_kpi WHERE {flow_where} GROUP BY flow_id'''
    latest_sql = f'''SELECT flow_id,max(time) AS seen,max(sla_state) AS sla_state,
      last_value(ap_id ORDER BY time) AS ap_id,last_value(lat_p50_ms ORDER BY time) AS p50,
      last_value(lat_p95_ms ORDER BY time) AS p95,last_value(lat_p99_ms ORDER BY time) AS p99,
      last_value(jitter_ms ORDER BY time) AS jitter,last_value(thpt_mbps ORDER BY time) AS thpt
      FROM flow_kpi WHERE {recent} GROUP BY flow_id'''
    aggregate, latest = await asyncio.gather(
        query_influx(influx,aggregate_sql), query_influx(influx,latest_sql))
    totals = {r['flow_id']: r for r in aggregate}
    current = {r['flow_id']: r for r in latest}
    rows = []
    for f in flows:
        t = dict(totals.get(f['flow_id'], {}))
        t.update(current.get(f['flow_id'], {}))
        rows.append(row_for(f, t, ap_names, meta['permissions'], now))
    if rows and include_sparks:
        spark_step = max(w['bucketS'], math.ceil((w['to']-w['from'])/40000))
        sparks = await query_influx(influx, f'''SELECT flow_id,date_bin(INTERVAL '{spark_step} seconds',time) AS bucket,
          last_value(lat_p95_ms ORDER BY time) AS p95,avg(thpt_mbps) AS thpt
          FROM flow_kpi WHERE {flow_where} GROUP BY flow_id,bucket ORDER BY bucket''')
        by_flow = {}
        for s in sparks:
            by_flow.setdefault(s['flow_id'],[]).append(s)
        for row in rows:
            field='p95' if row['bound'] is not None else 'thpt'
            row['spark']=[number(s.get(field)) for s in by_flow.get(row['flowId'],[])][-40:]
    if 'details.engineering' in meta['permissions']:
        queues = await query_influx(influx, f'''SELECT flow_id,last_value(depth_pkts ORDER BY time) AS depth FROM ctrl_queue
          WHERE {predicate(site,now-5000,now)}{telemetry_filter} GROUP BY flow_id''')
        depth = {r['flow_id']: number(r['depth']) for r in queues}
        for row in rows:
            row['queueDepth'] = depth.get(row['flowId'])
    rank = {'Violating': 0, 'At risk': 1, 'In SLA': 2, 'Unknown': 3}
    rows.sort(key=lambda r: (rank[r['status']['label']], -(r['margin'] if r['margin'] is not None else -1), r['flowId']))
    return site, meta, flows, rows, aps


@router.get('/flows')
async def flow_list(request: Request, user=Depends(require_user)):
    site, meta, flows, rows, aps = await ledger(request, user)
    counts = {k: sum(r['status']['label'] == k for r in rows) for k in ('In SLA', 'At risk', 'Violating')}
    ul = sum(f['direction'] == 'uplink' for f in flows)
    dl = len(flows)-ul
    kpis = [
      {'id': 'S5-W1a', 'label': 'Managed flows', 'value': str(len(flows)), 'context': f'{ul} uplink, {dl} downlink', 'tone': 'neutral'},
      {'id': 'S5-W1b', 'label': 'In SLA now', 'value': str(counts['In SLA']), 'context': 'Current telemetry', 'tone': 'good', 'link': '/flows?status=in-sla'},
      {'id': 'S5-W1c', 'label': 'At risk', 'value': str(counts['At risk']), 'context': 'Current telemetry', 'tone': 'warn', 'link': '/flows?status=at-risk'},
      {'id': 'S5-W1d', 'label': 'Violating', 'value': str(counts['Violating']), 'context': 'Current telemetry', 'tone': 'crit', 'link': '/flows?status=violating'}]
    return envelope(meta, {'kpis': kpis, 'rows': rows, 'classes': sorted({f['sla_name'] for f in flows if f['sla_name']}), 'aps': [a['name'] for a in aps]})


@router.get('/flows/{flow_id}')
async def flow_detail(flow_id: str, request: Request, user=Depends(require_user)):
    site, meta, flows, rows, aps = await ledger(request, user, selected_flow=flow_id, include_sparks=False)
    f = next((f for f in flows if f['flow_id'] == flow_id), None)
    if f is None:
        raise AuthError(404, 'NOT_FOUND', 'Flow not found.')
    row = next(r for r in rows if r['flowId'] == flow_id)
    w = meta['window']; where = predicate(site, w['from'], w['to']) + ' AND flow_id=' + literal(flow_id)
    step = w['bucketS']
    influx=request.app.state.influx_client
    samples_sql=f'''SELECT date_bin(INTERVAL '{step} seconds',time) AS bucket,
      last_value(lat_p50_ms ORDER BY time) AS p50,last_value(lat_p95_ms ORDER BY time) AS p95,
      last_value(lat_p99_ms ORDER BY time) AS p99,avg(thpt_mbps) AS thpt,
      avg(jitter_ms) AS jitter,sum(pkts) AS pkts,sum(lost_pkts) AS lost,sum(deadline_miss) AS misses,
      max(sla_state) AS state FROM flow_kpi WHERE {where} GROUP BY bucket ORDER BY bucket'''
    hist_sql=f'SELECT le_ms,sum(count) AS n FROM flow_lat_hist WHERE {where} GROUP BY le_ms'
    async def no_rows(): return []
    engineering='details.engineering' in meta['permissions']
    queue_sql=f'''SELECT date_bin(INTERVAL '{step} seconds',time) AS bucket,
      last_value(depth_pkts ORDER BY time) AS depth,avg(drops_ps) AS drops
      FROM ctrl_queue WHERE {where} GROUP BY bucket ORDER BY bucket'''
    slot_sql=f'''SELECT date_bin(INTERVAL '{step} seconds',time) AS bucket,
      avg(slots_alloc) AS alloc,avg(slots_used) AS used FROM sched_flow WHERE {where}
      GROUP BY bucket ORDER BY bucket'''
    samples,events,hist_rows,queue_rows,slot_rows=await asyncio.gather(
      query_influx(influx,samples_sql),
      request.app.state.pg_pool.fetch('''SELECT event_id,ts,type,entity_type FROM event WHERE site_id=$1
        AND ts>=to_timestamp($2::double precision/1000) AND ts<to_timestamp($3::double precision/1000)
        AND ((entity_type='flow' AND entity_id=$4) OR (entity_type='client' AND entity_id=$5)
          OR (entity_type='ap' AND entity_id=$6) OR entity_type='controller')
        AND type IN ('roam','alert_raised','schedule_recomputed','agent_disconnected')
        ORDER BY ts LIMIT 600''',site,w['from'],w['to'],flow_id,f['sta_id'],row['apId']),
      query_influx(influx,hist_sql),
      query_influx(influx,queue_sql) if engineering else no_rows(),
      query_influx(influx,slot_sql) if engineering else no_rows())
    latency = chart(samples, w, 'ms', [('p50','p50','p50'),('p95','p95','p95'),('p99','p99','p99')])
    latency['refs'] = [{'label':'SLA bound','value':row['bound']}] if row['bound'] is not None else []
    latency['bands'] = [{'from':epoch(s['bucket']),'to':epoch(s['bucket'])+step*1000} for s in samples if s.get('state') == 2]
    latency['markers']=[{'t':epoch(e['ts']),'label':e['type'].replace('_',' '),
      'kind':{'roam':'roam','alert_raised':'alert','schedule_recomputed':'schedule'}.get(e['type'],'event'),
      'link':'/events?entity='+flow_id} for e in events]
    throughput = chart(samples,w,'Mbps',[('thpt','Throughput','thpt')]); throughput['refs'] = [{'label':'Minimum throughput','value':row['minThpt']}] if row['minThpt'] is not None else []
    for s in samples:
        p, lost = number(s.get('pkts')), number(s.get('lost'))
        s['loss'] = 100*lost/p if p and lost is not None else None
        s['missPct'] = 100*number(s['misses'])/p if p and s.get('misses') is not None else None
    loss = chart(samples,w,'%',[('loss','Loss','loss'),('miss','Deadline misses','missPct')])
    jitter = chart(samples,w,'ms',[('jitter','Jitter','jitter')]); jitter['refs'] = [{'label':'Maximum jitter','value':number(f['max_jitter_ms'])}] if f['max_jitter_ms'] is not None else []
    terms = []
    for label, val, unit, symbol in [('p95',f['lat_bound_ms'],'ms','≤'),('throughput',f['min_thpt_mbps'],'Mbps','≥'),('loss',f['max_loss_pct'],'%','≤'),('jitter',f['max_jitter_ms'],'ms','≤')]:
        if val is not None: terms.append(f'{label} {symbol} {number(val):g} {unit}')
    # The fixed dashboard renders this value with slaTerms.map(...). Keep each
    # configured term separate while retaining the documented wording.
    summary = {'slaTerms': terms, 'since': None, 'fiveTuple': None, 'classifier': f['classifier'],
               'managedSince': epoch(f['created_at']), 'apChannel': None, 'deadlineMissTotal': row['deadlineMiss'], 'lossPct': row['lossPct']}
    if f['src_ip'] and f['dst_ip']:
        summary['fiveTuple'] = f"{f['src_ip']}:{f['src_port']} → {f['dst_ip']}:{f['dst_port']} {f['proto'] or ''}".strip()
    hist=sorted(((float(h['le_ms']),number(h['n'])) for h in hist_rows if h.get('n') is not None),key=lambda x:x[0])
    delivered=sum(max(0,(number(s.get('pkts')) or 0)-(number(s.get('lost')) or 0)) for s in samples)
    le=[x for x,_ in hist]; cum=[min(100,100*n/delivered) for _,n in hist] if delivered else []
    p95=None
    if cum and cum[-1]>=95:
        for i,c in enumerate(cum):
            if c>=95:
                lower_le=le[i-1] if i else 0
                lower_c=cum[i-1] if i else 0
                p95=lower_le+(le[i]-lower_le)*(95-lower_c)/(c-lower_c) if c>lower_c else le[i]
                break
    data = {'row':row,'summary':summary,'latency':latency,'throughput':throughput,
            'cdf':{'le':le,'cum':cum,'bound':row['bound'],'p95':p95},'loss':loss,'jitter':jitter}
    if engineering:
        data.update({'queue':chart(queue_rows,w,'packets',[('depth','Depth','depth')]),
                     'drops':chart(queue_rows,w,'drops/s',[('drops','Drops','drops')]),
                     'slots':chart(slot_rows,w,'slots',[('alloc','Allocated','alloc'),('used','Used','used')])})
    return envelope(meta,data)
