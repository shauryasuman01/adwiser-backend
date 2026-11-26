"""Scheduler view backed by sampled, validated allocation snapshots."""
import json
from fastapi import APIRouter, Depends, Request
from app.database.influx import query_influx
from app.middleware.auth import AuthError, require_permission
from app.routes.overview import epoch, number, predicate, chart, literal, now_ms
from app.services.monitor import scope, envelope

router=APIRouter(prefix='/api/v1/adwiser',tags=['Monitor scheduler'])


@router.get('/scheduler')
async def scheduler(request:Request,user=Depends(require_permission('scheduler.view'))):
    site,meta=await scope(request,user); db=request.app.state.pg_pool; influx=request.app.state.influx_client
    p=request.query_params; w=meta['window']; where=predicate(site,w['from'],w['to']); step=w['bucketS']
    versions=await db.fetch('SELECT * FROM schedule_version WHERE site_id=$1 ORDER BY computed_at DESC LIMIT 100',site)
    selected=versions[0] if versions else None
    if 'version' in p:
        try: ver=int(p['version'])
        except ValueError: raise AuthError(400,'INVALID_REQUEST','Invalid schedule version.')
        selected=next((v for v in versions if v['schedule_ver']==ver),None)
        if selected is None: raise AuthError(404,'NOT_FOUND','Schedule version not found.')
    stats=await query_influx(influx,f'''SELECT date_bin(INTERVAL '{step} seconds',time) AS bucket,
      avg(alloc_pct) AS alloc,avg(used_pct) AS used,sum(admission_rejects) AS rejects,
      max(compute_us_p99) AS compute,max(overruns) AS overruns,last_value(schedule_ver ORDER BY time) AS ver
      FROM sched_stats WHERE {where} GROUP BY bucket ORDER BY bucket''')
    latest=stats[-1] if stats else {}
    queues=await query_influx(influx,f'''SELECT flow_id,last_value(ap_id ORDER BY time) AS ap_id,
      last_value(depth_pkts ORDER BY time) AS depth_pkts,last_value(depth_bytes ORDER BY time) AS depth_bytes,
      last_value(sojourn_p95_us ORDER BY time) AS sojourn_us,last_value(drops_ps ORDER BY time) AS drops_ps,
      sum(deadline_drops) AS deadline_drops,avg(enq_pps) AS enq_pps,avg(deq_pps) AS deq_pps
      FROM ctrl_queue WHERE {where} GROUP BY flow_id''')
    names={r['flow_id']:r for r in await db.fetch('''SELECT f.flow_id,f.name,s.name AS cls FROM flow f LEFT JOIN sla_class s ON s.sla_class_id=f.sla_class_id WHERE f.site_id=$1''',site)}
    aps={r['ap_id']:r['name'] for r in await db.fetch('SELECT ap_id,name FROM ap WHERE site_id=$1',site)}
    now=now_ms()
    agents=await query_influx(influx,f'''SELECT ap_id,max(time) AS seen,
      last_value(heartbeat_ok ORDER BY time) AS heartbeat_ok FROM ap_ingestion
      WHERE {predicate(site,now-5000,now)} GROUP BY ap_id''')
    online={r['ap_id']:r.get('heartbeat_ok') is True and r.get('seen') is not None
            and 0 <= now-epoch(r['seen']) <= 3000 for r in agents}
    qrows=[{'flowId':q['flow_id'],'flow':names.get(q['flow_id'],{}).get('name',q['flow_id']),
      'ap':aps.get(q.get('ap_id'),q.get('ap_id')),'depthPkts':number(q.get('depth_pkts')),'depthBytes':number(q.get('depth_bytes')),
      'sojournUs':number(q.get('sojourn_us')),'dropsPs':number(q.get('drops_ps')),'deadlineDrops':number(q.get('deadline_drops')),
      'enqPps':number(q.get('enq_pps')),'deqPps':number(q.get('deq_pps'))} for q in queues]
    ack=await query_influx(influx,f'SELECT ap_id,avg(lat_p95_ms) AS p95 FROM ack_fb WHERE {where} GROUP BY ap_id')
    host_rows=await query_influx(influx,f'''SELECT max(rt_lat_max_us) AS rt,max(cpu_iso_pct) AS cpu,
       sum(nic_rx_drops_ps) AS nic,min(disk_free_gb) AS disk FROM ctrl_host WHERE {where}''')
    host_row=host_rows[0] if host_rows else {}
    ingestion_rows=await query_influx(influx,f'''SELECT
       last_value(ingest_msgs_ps ORDER BY time) AS msgs,
       last_value(ingest_lag_p95_ms ORDER BY time) AS lag,
       last_value(influx_write_p95_ms ORDER BY time) AS write
       FROM ctrl_ingestion WHERE {where}''')
    ingestion=ingestion_rows[0] if ingestion_rows else {}
    top=sorted(qrows,key=lambda q:q['depthPkts'] or 0,reverse=True)[:3]
    queue_chart=chart([],w,'packets',[(q['flowId'],q['flow'],q['flowId']) for q in top])
    if top:
        ids=','.join(literal(q['flowId']) for q in top)
        qseries=await query_influx(influx,f'''SELECT flow_id,date_bin(INTERVAL '{step} seconds',time) AS bucket,
          last_value(depth_pkts ORDER BY time) AS depth FROM ctrl_queue WHERE {where}
          AND flow_id IN ({ids}) GROUP BY flow_id,bucket ORDER BY bucket''')
        start=queue_chart['start']; ms=queue_chart['stepMs']
        by_id={s['key']:s for s in queue_chart['series']}
        for r in qseries:
            i=(epoch(r['bucket'])-start)//ms
            if 0<=i<len(by_id[r['flow_id']]['values']):by_id[r['flow_id']]['values'][i]=number(r['depth'])
    threshold=await db.fetchval("SELECT threshold FROM alert_rule WHERE rule_id='r-ack-lat'")
    v=lambda r:{'scheduleVer':r['schedule_ver'],'runId':r['run_id'],'computedAt':epoch(r['computed_at']),'trigger':r['trigger'],
      'scope':site,'allocPct':number(r['alloc_pct']),'usedPct':number(r['used_pct']),
      'active':bool(versions and r['run_id']==versions[0]['run_id'] and r['schedule_ver']==versions[0]['schedule_ver'])} if r else None
    allocations=selected['allocations'] if selected else []
    if isinstance(allocations,str): allocations=json.loads(allocations)
    totals={}
    for a in allocations:
        key=(a.get('ap_id'),a.get('radio'));totals[key]=totals.get(key,0)+(a.get('slots_alloc') or 0)
    representative={aid:max((radio for apid,radio in totals if apid==aid),
      key=lambda radio:(totals[aid,radio],radio),default=None) for aid in aps}
    shown=[a for a in allocations if representative.get(a.get('ap_id'))==a.get('radio')]
    slots=[{'apId':a.get('ap_id'),'radio':a.get('radio'),'flowId':a.get('flow_id'),
      'label':f"{a.get('radio')} · {names.get(a.get('flow_id'),{}).get('name',a.get('flow_id'))}",
      'flow':names.get(a.get('flow_id'),{}).get('name'),'cls':a.get('sla_class') or names.get(a.get('flow_id'),{}).get('cls'),
      'startUs':a.get('start_us'),'lenUs':a.get('len_us'),'usedSlots':a.get('slots_used')} for a in shown]
    gantt={'aps':[{'apId':aid,'name':name+(f" · {representative[aid]} representative" if representative.get(aid) else ''),'online':online.get(aid,False)} for aid,name in aps.items()], 'slots':slots,
      'contentionFromUs':None,'superframeUs':selected['superframe_us'] if selected else None,
      'slotUs':selected['slot_us'] if selected else None,'totalSlots':selected['total_slots'] if selected else None,
      'representation':'representative-radio','representativeRadios':representative,
      'classes':sorted({r['cls'] for r in names.values() if r['cls']}),
      'flows':[{'flowId':fid,'short':fid,'name':r['name'],'cls':r['cls']} for fid,r in names.items()]}
    rejects=sum(number(r.get('rejects')) or 0 for r in stats)
    kpis=[{'id':'S8-W1'+id,'label':label,'value':str(value) if value is not None else '—','context':context,'tone':'neutral'}
      for id,label,value,context in [
       ('a','Active schedule',f"v{selected['schedule_ver']}" if selected else None,'Latest stored detailed snapshot'),
       ('b','Superframe / slot',f"{selected['superframe_us']/1000:g} / {selected['slot_us']/1000:g}" if selected else None,'ms'),
       ('c','Slot utilisation',number(latest.get('used')),'Used %'),
       ('d','Admission rejects',rejects,'Selected range'),
       ('e','Schedule compute p99',number(latest.get('compute'))/1000 if latest.get('compute') is not None else None,'ms')]]
    data={'kpis':kpis,'version':v(selected),'versions':[v(r) for r in versions], 'gantt':gantt,
      'slotUtil':chart(stats,w,'%',[('alloc','Allocated','alloc'),('used','Used','used')]),
      'queueTop':queue_chart,'ackByAp':[{'label':aps.get(a['ap_id'],a['ap_id']),'value':number(a['p95']),'tone':'neutral'} for a in ack],
      'ackWarn':number(threshold),'queues':qrows,
      'host':[{'label':'RT latency max','value':f"{number(host_row['rt']):g} µs" if host_row.get('rt') is not None else 'Unavailable'},
              {'label':'Isolated cores CPU','value':f"{number(host_row['cpu']):g}%" if host_row.get('cpu') is not None else 'Unavailable'},
              {'label':'NIC RX drops','value':f"{number(host_row['nic']):g}/s" if host_row.get('nic') is not None else 'Unavailable'},
              {'label':'Telemetry ingest','value':f"{number(ingestion['msgs']):g} msg/s · {number(ingestion['lag']):g} ms p95" if ingestion.get('msgs') is not None and ingestion.get('lag') is not None else 'Unavailable'},
              {'label':'InfluxDB write latency p95','value':f"{number(ingestion['write']):g} ms" if ingestion.get('write') is not None else 'Unavailable'},
              {'label':'Disk free','value':f"{number(host_row['disk']):g} GB" if host_row.get('disk') is not None else 'Unavailable'}]}
    return envelope(meta,data)
