"""Evaluate only supported threshold rules on bounded telemetry windows."""
import asyncio
import json
import logging
import uuid
from app.database.influx import query_influx
from app.routes.overview import predicate, now_ms

logger=logging.getLogger(__name__)
SUPPORTED={'ap_radio.util_busy_pct':('ap_radio','ap_id','util_busy_pct'),
 'ap_radio.tx_retry_pct':('ap_radio','ap_id','tx_retry_pct'),
 'flow_kpi.sla_state':('flow_kpi','flow_id','sla_state'),
 'ack_fb.lat_p95_ms':('ack_fb','ap_id','lat_p95_ms'),
 'sta_link.rssi_dbm':('sta_link','sta_id','rssi_dbm'),
 'sta_link.tx_retry_pct':('sta_link','sta_id','tx_retry_pct')}


def triggered(value,operator,threshold):
    if value is None or threshold is None:return False
    return {'>':lambda:value>threshold,'<':lambda:value<threshold,'=':lambda:value==threshold,
            '>=':lambda:value>=threshold,'<=':lambda:value<=threshold}[operator]()


async def evaluate_once(pool,influx):
    # A transaction-level advisory lock coordinates multiple API workers.
    async with pool.acquire() as conn:
        async with conn.transaction():
            if not await conn.fetchval('SELECT pg_try_advisory_xact_lock(417392001)'):
                return 0
            rules=await conn.fetch('SELECT * FROM alert_rule WHERE enabled IS TRUE ORDER BY rule_id')
            sites=await conn.fetch('SELECT site_id FROM site')
            changes=0
            now=now_ms()
            for site in sites:
                sid=site['site_id']
                for rule in rules:
                    spec=SUPPORTED.get(rule['metric'])
                    if not spec or rule['threshold'] is None:continue
                    table,entity,metric=spec
                    span=max(rule['duration_s'],10)*1000
                    where=predicate(sid,now-span-2000,now)
                    # min/max over the duration proves sustained conditions; one
                    # sample alone cannot satisfy a 30 or 60 second rule.
                    rows=await query_influx(influx,f'''SELECT {entity} AS entity_id,min(time) AS first_seen,max(time) AS last_seen,
                      min({metric}) AS minimum,max({metric}) AS maximum,
                      last_value({metric} ORDER BY time) AS latest FROM {table} WHERE {where} GROUP BY {entity}''')
                    for r in rows:
                        eid=r['entity_id']; first=r.get('first_seen'); last=r.get('last_seen')
                        from app.routes.overview import epoch
                        recent=last is not None and now-epoch(last)<=5000
                        observed=first is not None and epoch(first)<=now-rule['duration_s']*1000
                        value=(r['minimum'] if rule['operator'] in ('>','>=') else
                               r['maximum'] if rule['operator'] in ('<','<=') else
                               r['minimum'] if r['minimum']==r['maximum'] else None)
                        active=await conn.fetchrow('''SELECT * FROM alert WHERE site_id=$1 AND rule_id=$2 AND entity_id=$3
                         AND ended_at IS NULL AND status<>'resolved' FOR UPDATE''',sid,rule['rule_id'],eid)
                        latest_alert=await conn.fetchrow('''SELECT * FROM alert WHERE site_id=$1 AND rule_id=$2 AND entity_id=$3
                          ORDER BY started_at DESC LIMIT 1''',sid,rule['rule_id'],eid)
                        cleared_after_end=False
                        if latest_alert and latest_alert['ended_at']:
                            cleared_after_end=await conn.fetchval('''SELECT EXISTS(SELECT 1 FROM event WHERE site_id=$1 AND type='alert_cleared'
                              AND details->>'alert_id'=$2 AND ts >= $3)''',sid,latest_alert['alert_id'],latest_alert['ended_at'])
                        condition=recent and observed and triggered(value,rule['operator'],float(rule['threshold']))
                        context=json.dumps({'metric':rule['metric'],'value':value,'threshold':float(rule['threshold'])})
                        if condition and not active and latest_alert and latest_alert['status']!='resolved' and latest_alert['ended_at']:
                            # A cleared but manually unresolved condition is the
                            # same lifecycle. Reopen it instead of accumulating
                            # a new open row for every telemetry oscillation.
                            aid=latest_alert['alert_id']
                            await conn.execute('UPDATE alert SET ended_at=NULL,context=$2::jsonb WHERE alert_id=$1',aid,context)
                            await conn.execute('''INSERT INTO event(event_id,site_id,type,severity,entity_type,entity_id,message,details)
                              VALUES($1,$2,'alert_raised',$3,$4,$5,$6,$7::jsonb)''','evt-'+uuid.uuid4().hex,sid,rule['severity'],rule['entity_type'],eid,'Alert condition reoccurred',json.dumps({'alert_id':aid}))
                            changes+=1
                        elif condition and not active and (not latest_alert or (latest_alert['status']=='resolved' and cleared_after_end)):
                            aid='al-'+uuid.uuid4().hex
                            await conn.execute('''INSERT INTO alert(alert_id,site_id,rule_id,source,title,severity,entity_type,entity_id,started_at,context)
                              VALUES($1,$2,$3,'rule',$4,$5,$6,$7,now(),$8::jsonb) ON CONFLICT DO NOTHING''',aid,sid,rule['rule_id'],rule['name'],rule['severity'],rule['entity_type'],eid,context)
                            await conn.execute('''INSERT INTO event(event_id,site_id,type,severity,entity_type,entity_id,message,details)
                              VALUES($1,$2,'alert_raised',$3,$4,$5,$6,$7::jsonb)''','evt-'+uuid.uuid4().hex,sid,rule['severity'],rule['entity_type'],eid,rule['name'],json.dumps({'alert_id':aid}))
                            changes+=1
                        elif latest_alert and recent and not triggered(r.get('latest'),rule['operator'],float(rule['threshold'])) and (active or not cleared_after_end):
                            target=active or latest_alert
                            if active: await conn.execute('UPDATE alert SET ended_at=now() WHERE alert_id=$1',target['alert_id'])
                            await conn.execute('''INSERT INTO event(event_id,site_id,type,severity,entity_type,entity_id,message,details)
                              VALUES($1,$2,'alert_cleared',$3,$4,$5,$6,$7::jsonb)''','evt-'+uuid.uuid4().hex,sid,rule['severity'],rule['entity_type'],eid,'Alert condition cleared',json.dumps({'alert_id':target['alert_id']}))
                            changes+=1
            return changes


async def run_alert_evaluator(pool,influx,stop_event,interval_s=10):
    while not stop_event.is_set():
        try:await evaluate_once(pool,influx)
        except Exception:logger.exception('Alert evaluation failed')
        try:await asyncio.wait_for(stop_event.wait(),interval_s)
        except asyncio.TimeoutError:pass
