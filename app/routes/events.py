"""Site-scoped event log and notes."""
import uuid
import json
from fastapi import APIRouter, Depends, Request
from app.middleware.auth import AuthError, require_user, require_permission
from app.routes.overview import epoch
from app.services.monitor import scope, envelope, json_body

router = APIRouter(prefix='/api/v1/adwiser', tags=['Monitor events'])
GROUPS = {'association':'Wi-Fi','disassociation':'Wi-Fi','roam':'Wi-Fi','channel_change':'Wi-Fi',
          'agent_connected':'Agent','agent_disconnected':'Agent','schedule_recomputed':'Schedule',
          'alert_raised':'Alerts','alert_cleared':'Alerts','alert_acknowledged':'Alerts','alert_resolved':'Alerts',
          'note_added':'User actions','user_signed_in':'User actions','user_changed':'User actions','role_access_changed':'User actions'}


def event_row(r, engineering=False, notes=None):
    kind, ident = r['entity_type'], r['entity_id']
    link = {'ap':f'/aps/{ident}','client':f'/clients?client={ident}','flow':f'/flows/{ident}','controller':'/scheduler'}.get(kind)
    out = {'eventId':r['event_id'],'ts':epoch(r['ts']),'type':r['type'],'group':GROUPS.get(r['type'],'User actions'),
           'severity':r['severity'],'entityType':kind,'entityId':ident,'entityName':r.get('entity_name') or ident,
           'entityLink':link,'message':r['message'],'actor':r.get('actor_name') or r['actor']}
    if engineering:
        out['details'] = json.loads(r['details']) if isinstance(r['details'],str) else r['details']
        out['notes'] = notes or []
    return out


@router.get('/events')
async def list_events(request: Request, user=Depends(require_user)):
    site, meta = await scope(request,user)
    p = request.query_params; w=meta['window']; db=request.app.state.pg_pool
    try:
        page = int(p.get('page','1')); size = int(p.get('pageSize','100'))
        if page < 1 or not 1 <= size <= 5000: raise ValueError
        q = p.get('q','')
        if len(q)>100: raise ValueError
        narrow_from = int(p.get('narrowFrom',w['from'])); narrow_to = int(p.get('narrowTo',w['to']))
        if not w['from'] <= narrow_from < narrow_to <= w['to']: raise ValueError
        severity = p.get('severity')
        if severity and severity not in ('info','warning','critical'): raise ValueError
        types = [t for t in p.get('types','').split(',') if t]
        if any(t not in GROUPS for t in types): raise ValueError
    except ValueError:
        raise AuthError(400,'INVALID_REQUEST','Invalid event filter.')
    entity = p.get('entity')
    clauses = ['e.site_id=$1','e.ts >= to_timestamp($2::double precision/1000)','e.ts < to_timestamp($3::double precision/1000)']
    args = [site,narrow_from,narrow_to]
    if types: args.append(types); clauses.append(f'e.type=ANY(${len(args)}::text[])')
    if severity: args.append(severity); clauses.append(f'e.severity=${len(args)}')
    if entity: args.append(entity); clauses.append(f'e.entity_id=${len(args)}')
    if q: args.append('%'+q+'%'); clauses.append(f'(e.message ILIKE ${len(args)} OR e.entity_id ILIKE ${len(args)})')
    joins=""" LEFT JOIN ap a ON e.entity_type='ap' AND a.ap_id=e.entity_id
      LEFT JOIN flow f ON e.entity_type='flow' AND f.flow_id=e.entity_id
      LEFT JOIN client c ON e.entity_type='client' AND c.sta_id=e.entity_id"""
    if q:
        clauses[-1]=f"(e.message ILIKE ${len(args)} OR e.entity_id ILIKE ${len(args)} OR a.name ILIKE ${len(args)} OR f.name ILIKE ${len(args)} OR c.hostname ILIKE ${len(args)} OR c.label ILIKE ${len(args)})"
    where = ' AND '.join(clauses)
    total = await db.fetchval(f'SELECT count(*) FROM event e {joins} WHERE {where}',*args)
    args.extend([size,(page-1)*size])
    rows = await db.fetch(f'''SELECT e.*,coalesce(a.name,f.name,c.hostname,c.label) AS entity_name,u.name AS actor_name
      FROM event e {joins} LEFT JOIN app_user u ON e.actor=u.user_id WHERE {where}
      ORDER BY e.ts DESC,e.event_id DESC LIMIT ${len(args)-1} OFFSET ${len(args)}''',*args)
    note_map={}
    if 'details.engineering' in meta['permissions'] and rows:
        ids=[r['event_id'] for r in rows]
        note_rows=await db.fetch('''SELECT n.event_id,n.note_id,n.ts,n.text,u.name FROM event_note n
          JOIN app_user u ON u.user_id=n.user_id WHERE n.event_id=ANY($1::text[]) ORDER BY n.ts''',ids)
        for note in note_rows:
            note_map.setdefault(note['event_id'],[]).append({'noteId':note['note_id'],'user':note['name'],
              'ts':epoch(note['ts']),'text':note['text']})
    step = 60000
    while (w['to']-w['from'])/step > 600: step *= 2
    start = w['from']//step*step
    n = min(600,(w['to']-start+step-1)//step)
    groups = ['Wi-Fi','Agent','Schedule','Alerts','User actions']
    counts = [[0]*n for _ in groups]
    volume = await db.fetch('''SELECT floor(extract(epoch FROM ts)*1000/$4::double precision)::bigint AS bin,
       type,count(*) AS n FROM event WHERE site_id=$1 AND ts>=to_timestamp($2::double precision/1000)
       AND ts<to_timestamp($3::double precision/1000) GROUP BY bin,type''',site,w['from'],w['to'],step)
    for e in volume:
        idx=e['bin']-start//step
        if 0<=idx<n: counts[groups.index(GROUPS[e['type']])][idx]+=e['n']
    entities = [{'id':r['ap_id'],'name':r['name'],'type':'ap'} for r in await db.fetch('SELECT ap_id,name FROM ap WHERE site_id=$1',site)]
    entities += [{'id':r['flow_id'],'name':r['name'],'type':'flow'} for r in await db.fetch('SELECT flow_id,name FROM flow WHERE site_id=$1',site)]
    entities += [{'id':r['sta_id'],'name':r['name'],'type':'client'} for r in await db.fetch('''SELECT DISTINCT c.sta_id,coalesce(c.hostname,c.label,c.sta_id) AS name
      FROM client c JOIN flow f ON f.sta_id=c.sta_id WHERE f.site_id=$1''',site)]
    entities.append({'id':site,'name':'Controller','type':'controller'})
    data={'volume':{'start':start,'stepMs':step,'groups':groups,'counts':counts},
          'rows':[event_row(dict(r),'details.engineering' in meta['permissions'],note_map.get(r['event_id'])) for r in rows],
          'total':total,'page':page,'pageSize':size,'types':list(GROUPS), 'entities':entities}
    return envelope(meta,data)


@router.post('/events/{event_id}/notes')
async def add_note(event_id: str, request: Request, user=Depends(require_permission('notes.add'))):
    body=await json_body(request,note=True)
    site,meta=await scope(request,user)
    db=request.app.state.pg_pool
    async with db.acquire() as conn:
        async with conn.transaction():
            row=await conn.fetchrow('SELECT * FROM event WHERE event_id=$1 AND site_id=$2 FOR UPDATE',event_id,site)
            if row is None: raise AuthError(404,'NOT_FOUND','Event not found.')
            note_id='en-'+uuid.uuid4().hex
            await conn.execute('INSERT INTO event_note(note_id,event_id,user_id,text) VALUES($1,$2,$3,$4)',note_id,event_id,user['user_id'],body['text'])
            await conn.execute('''INSERT INTO event(event_id,site_id,type,entity_type,entity_id,message,actor,details)
                VALUES($1,$2,'note_added',$3,$4,$5,$6,$7::jsonb)''', 'evt-'+uuid.uuid4().hex,site,row['entity_type'],row['entity_id'],'Event note added',user['user_id'],__import__('json').dumps({'event_id':event_id}))
    notes=await db.fetch('SELECT n.note_id,n.ts,n.text,u.name FROM event_note n JOIN app_user u ON u.user_id=n.user_id WHERE n.event_id=$1 ORDER BY n.ts',event_id)
    out=event_row(dict(row),True,[{'noteId':n['note_id'],'user':n['name'],'ts':epoch(n['ts']),'text':n['text']} for n in notes])
    return envelope(meta,out)
