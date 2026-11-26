"""Alert list, drawer, and transactional state changes."""
import json
import uuid
from fastapi import APIRouter, Depends, Request
from app.middleware.auth import AuthError, require_user, require_permission
from app.routes.overview import alert_view, epoch
from app.services.monitor import scope, envelope, json_body
from app.routes.events import event_row
from app.workers.alert_evaluator import SUPPORTED
from app.database.influx import query_influx
from app.routes.overview import chart, predicate, literal, now_ms

router=APIRouter(prefix='/api/v1/adwiser',tags=['Monitor alerts'])
SELECT='''SELECT a.*,coalesce(p.name,c.hostname,c.label,f.name) AS entity_name FROM alert a
 LEFT JOIN ap p ON a.entity_type='ap' AND p.ap_id=a.entity_id
 LEFT JOIN client c ON a.entity_type='client' AND c.sta_id=a.entity_id
 LEFT JOIN flow f ON a.entity_type='flow' AND f.flow_id=a.entity_id'''


async def drawer(db,site,alert_id,permissions,influx=None):
    a=await db.fetchrow(SELECT+' WHERE a.site_id=$1 AND a.alert_id=$2',site,alert_id)
    if a is None: raise AuthError(404,'NOT_FOUND','Alert not found.')
    rule=await db.fetchrow('SELECT * FROM alert_rule WHERE rule_id=$1',a['rule_id']) if a['rule_id'] else None
    events=await db.fetch('''SELECT e.* FROM event e WHERE e.site_id=$1 AND e.entity_type=$2 AND e.entity_id=$3
       AND e.ts BETWEEN $4::timestamptz - interval '30 minutes' AND $4::timestamptz + interval '30 minutes'
       ORDER BY e.ts DESC LIMIT 12''',site,a['entity_type'],a['entity_id'],a['started_at'])
    related=await db.fetch(SELECT+" WHERE a.site_id=$1 AND a.entity_type=$2 AND a.entity_id=$3 AND a.ended_at IS NULL AND a.status<>'resolved' AND a.alert_id<>$4 ORDER BY a.started_at DESC",site,a['entity_type'],a['entity_id'],alert_id)
    notes=await db.fetch('''SELECT n.note_id,n.ts,n.text,u.name FROM alert_note n JOIN app_user u ON n.user_id=u.user_id
      WHERE n.alert_id=$1 ORDER BY n.ts''',alert_id)
    history=[{'ts':epoch(a['started_at']),'text':'Raised'}]
    for e in events:
        details=json.loads(e['details']) if isinstance(e['details'],str) else e['details']
        if e['type'] in ('alert_cleared','alert_acknowledged','alert_resolved','note_added') and details.get('alert_id')==alert_id:
            history.append({'ts':epoch(e['ts']),'text':e['message']})
    history.sort(key=lambda h:h['ts'])
    rule_text=(f"{rule['name']}: {rule['metric']} {rule['operator']} {rule['threshold']} for {rule['duration_s']} s" if rule else (a['detector'] or 'Rule unavailable'))
    metric_chart=None
    spec=SUPPORTED.get(rule['metric']) if rule else None
    if influx is not None and spec:
        table,entity,field=spec
        end=min(now_ms(),epoch(a['started_at'])+900000)
        start=epoch(a['started_at'])-900000
        window={'from':start,'to':end,'bucketS':5}
        where=predicate(site,start,end)+' AND '+entity+'='+literal(a['entity_id'])
        try:
            samples=await query_influx(influx,f'''SELECT date_bin(INTERVAL '5 seconds',time) AS bucket,
              avg({field}) AS value FROM {table} WHERE {where} GROUP BY bucket ORDER BY bucket''')
            metric_chart=chart(samples,window,'',[('metric',rule['name'],'value')])
            metric_chart['refs']=[{'label':'Threshold','value':float(rule['threshold'])}] if rule['threshold'] is not None else []
            metric_chart['bands']=[{'from':epoch(a['started_at']),'to':epoch(a['ended_at']) or end}]
        except Exception:
            metric_chart=None
    return {'alert':alert_view(a),'ruleText':rule_text,
      'context':(json.loads(a['context']) if isinstance(a['context'],str) else a['context']) if 'details.engineering' in permissions else {},'metric':metric_chart,
      'related':{'events':[event_row(dict(e),'details.engineering' in permissions) for e in events],
                 'alerts':[alert_view(r) for r in related]},'history':history,
      'notes':[{'noteId':n['note_id'],'user':n['name'],'ts':epoch(n['ts']),'text':n['text']} for n in notes],
      'can':{'acknowledge':'alerts.acknowledge' in permissions and a['status']=='open',
             'resolve':'alerts.resolve' in permissions and a['status']!='resolved',
             'note':'notes.add' in permissions}}


@router.get('/alerts')
async def alerts(request:Request,user=Depends(require_user)):
    site,meta=await scope(request,user); db=request.app.state.pg_pool
    rows=await db.fetch(SELECT+' WHERE a.site_id=$1 ORDER BY a.started_at DESC,a.alert_id DESC',site)
    counts={'open':0,'acknowledged':0,'resolved':0,'active':0,'cleared':0}
    for r in rows:
        counts[r['status']]+=1
        if r['ended_at'] is None and r['status']!='resolved': counts['active']+=1
        elif r['ended_at'] is not None and r['status']!='resolved': counts['cleared']+=1
    return envelope(meta,{'rows':[alert_view(r) for r in rows],'counts':counts})


@router.get('/alerts/{alert_id}')
async def alert_detail(alert_id:str,request:Request,user=Depends(require_user)):
    site,meta=await scope(request,user)
    return envelope(meta,await drawer(request.app.state.pg_pool,site,alert_id,meta['permissions'],request.app.state.influx_client))


async def change(alert_id,request,user,action):
    body=await json_body(request,note=action=='notes')
    site,meta=await scope(request,user); db=request.app.state.pg_pool
    async with db.acquire() as conn:
        async with conn.transaction():
            a=await conn.fetchrow('SELECT * FROM alert WHERE alert_id=$1 AND site_id=$2 FOR UPDATE',alert_id,site)
            if a is None: raise AuthError(404,'NOT_FOUND','Alert not found.')
            if action=='ack':
                if a['status']!='open': raise AuthError(409,'CONFLICT','Alert is not open.')
                await conn.execute("UPDATE alert SET status='acknowledged',acked_by=$1,acked_at=now() WHERE alert_id=$2",user['user_id'],alert_id)
                event_type,message='alert_acknowledged','Alert acknowledged'
            elif action=='resolve':
                if a['status']=='resolved': raise AuthError(409,'CONFLICT','Alert is already resolved.')
                await conn.execute("UPDATE alert SET status='resolved',resolved_by=$1,resolved_at=now(),ended_at=coalesce(ended_at,now()) WHERE alert_id=$2",user['user_id'],alert_id)
                event_type,message='alert_resolved','Alert resolved'
            else:
                await conn.execute('INSERT INTO alert_note(note_id,alert_id,user_id,text) VALUES($1,$2,$3,$4)', 'an-'+uuid.uuid4().hex,alert_id,user['user_id'],body['text'])
                event_type,message='note_added','Alert note added'
            await conn.execute('''INSERT INTO event(event_id,site_id,type,severity,entity_type,entity_id,message,actor,details)
              VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)''','evt-'+uuid.uuid4().hex,site,event_type,a['severity'],a['entity_type'],a['entity_id'],message,user['user_id'],json.dumps({'alert_id':alert_id}))
    site,meta=await scope(request,user)
    return envelope(meta,await drawer(db,site,alert_id,meta['permissions'],request.app.state.influx_client))


@router.post('/alerts/{alert_id}/ack')
async def ack(alert_id:str,request:Request,user=Depends(require_permission('alerts.acknowledge'))):
    return await change(alert_id,request,user,'ack')


@router.post('/alerts/{alert_id}/resolve')
async def resolve(alert_id:str,request:Request,user=Depends(require_permission('alerts.resolve'))):
    return await change(alert_id,request,user,'resolve')


@router.post('/alerts/{alert_id}/notes')
async def add_alert_note(alert_id:str,request:Request,user=Depends(require_permission('notes.add'))):
    return await change(alert_id,request,user,'notes')
