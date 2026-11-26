"""Persist sampled generator schedule snapshots from InfluxDB to PostgreSQL."""
import asyncio
import json
import logging

from app.database.influx import query_influx
from app.repositories.registration import parse_timestamp

logger = logging.getLogger(__name__)


def validate_snapshot(row):
    """Validate allocation bounds and per-radio non-overlap.

    Returns the decoded allocation list. Values are microseconds and slot
    indices. Invalid metadata is rejected as a whole rather than partially
    persisted.
    """
    allocations = json.loads(row['allocations_json'])
    total = int(row['total_slots'])
    slot_us = int(row['slot_us'])
    if not isinstance(allocations,list) or len(allocations)>2000 or total<=0 or slot_us<=0:
        raise ValueError('invalid schedule dimensions')
    if int(row['superframe_us']) != total*slot_us:
        raise ValueError('slot duration does not fill superframe')
    occupied = {}
    required = {'ap_id','radio','flow_id','direction','sla_class','start_slot',
                'slots_alloc','slots_used','start_us','len_us'}
    for item in allocations:
        if not isinstance(item,dict) or set(item)!=required:
            raise ValueError('invalid allocation shape')
        start=int(item['start_slot']); count=int(item['slots_alloc']); used=int(item['slots_used'])
        if start<0 or count<=0 or not 0<=used<=count or start+count>total:
            raise ValueError('allocation outside superframe')
        if int(item['start_us'])!=start*slot_us or int(item['len_us'])!=count*slot_us:
            raise ValueError('allocation time does not match slots')
        key=(item['ap_id'],item['radio'])
        slots=occupied.setdefault(key,set())
        allocated=set(range(start,start+count))
        if slots & allocated:
            raise ValueError('overlapping allocation on one radio')
        slots.update(allocated)
    return allocations


async def sync_once(pool, influx, after=None):
    """Persist new snapshots and return ``(insert_count, newest_timestamp)``.

    The first call reads a bounded recovery window. Later calls use the last
    observed Influx timestamp, avoiding repeated transfer and JSON decoding of
    the large allocation payloads.
    """
    condition = ''
    if after is not None:
        value = after.isoformat().replace('+00:00', 'Z').replace("'", "''")
        condition = f" WHERE time > timestamp '{value}'"
    rows = await query_influx(
        influx, f'SELECT * FROM schedule_snapshot{condition} ORDER BY time DESC LIMIT 300')
    inserted = 0
    async with pool.acquire() as conn:
        async with conn.transaction():
            inventory={}
            for row in reversed(rows):
                allocations = validate_snapshot(row)
                site=row['site_id']
                if site not in inventory:
                    aps={r['ap_id'] for r in await conn.fetch('SELECT ap_id FROM ap WHERE site_id=$1 AND retired_at IS NULL',site)}
                    flows={r['flow_id']:r['direction'] for r in await conn.fetch('SELECT flow_id,direction FROM flow WHERE site_id=$1 AND retired_at IS NULL',site)}
                    radios={(r['ap_id'],r['radio']) for r in await conn.fetch('''SELECT r.ap_id,r.radio FROM radio r
                      JOIN ap a ON a.ap_id=r.ap_id WHERE a.site_id=$1 AND a.retired_at IS NULL''',site)}
                    inventory[site]=(aps,flows,radios)
                aps,flows,radios=inventory[site]
                if any(a['ap_id'] not in aps or (a['ap_id'],a['radio']) not in radios
                       or flows.get(a['flow_id'])!=a['direction'] for a in allocations):
                    continue
                payload=json.dumps(allocations,separators=(',',':'))
                result=await conn.execute('''INSERT INTO schedule_version(
                  schedule_ver,run_id,site_id,computed_at,trigger,superframe_us,slot_us,total_slots,
                  n_aps,n_flows,alloc_pct,used_pct,allocations,metadata_bytes)
                  VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14)
                  ON CONFLICT(site_id,run_id,schedule_ver) DO NOTHING''',
                  int(row['schedule_ver']),row['run_id'],site,parse_timestamp(row['time']),row['trigger'],
                  int(row['superframe_us']),int(row['slot_us']),int(row['total_slots']),int(row['n_aps']),
                  int(row['n_flows']),float(row['alloc_pct']),float(row['used_pct']),payload,len(payload.encode()))
                inserted += result.endswith('1')
    newest = max((parse_timestamp(row['time']) for row in rows), default=after)
    return inserted, newest


async def run_schedule_sync(pool,influx,stop_event,interval_s=2):
    """Periodically copy new sampled snapshots until shutdown."""
    logger.info('Schedule metadata worker started')
    cursor = None
    while not stop_event.is_set():
        try:
            count,cursor=await sync_once(pool,influx,cursor)
            if count: logger.info('Persisted %s schedule snapshot(s)',count)
        except Exception:
            logger.exception('Schedule metadata synchronisation failed')
        try: await asyncio.wait_for(stop_event.wait(),interval_s)
        except asyncio.TimeoutError: pass
    logger.info('Schedule metadata worker stopped')
