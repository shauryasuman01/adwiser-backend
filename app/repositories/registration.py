from datetime import datetime, timezone
import hashlib
import json


def parse_timestamp(value):
    timestamp = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(
            tzinfo=timezone.utc
        )

    return timestamp


async def register_inventory(pool, inventory):

    async with pool.acquire() as conn:

        async with conn.transaction():

            # 1. Register sites

            await conn.executemany("""
                INSERT INTO site (
                    site_id, name, timezone
                )
                VALUES ($1, $2, $3)
                ON CONFLICT (site_id) DO NOTHING
            """, [
                (
                    row["site_id"],
                    row["name"],
                    row.get("timezone", "UTC")
                )
                for row in inventory["sites"]
            ])

            # 2. Register access points

            await conn.executemany("""
                INSERT INTO ap (
                    ap_id, site_id, name, mac,
                    vendor, model, mgmt_ip,
                    fw_version, agent_version
                )
                VALUES (
                    $1, $2, $3, $4::text::macaddr,
                    $5, $6, $7::text::inet, $8, $9
                )
                ON CONFLICT (ap_id) DO UPDATE SET
                    mac = COALESCE(ap.mac, EXCLUDED.mac),
                    vendor = COALESCE(ap.vendor, EXCLUDED.vendor),
                    model = COALESCE(ap.model, EXCLUDED.model),
                    mgmt_ip = COALESCE(ap.mgmt_ip, EXCLUDED.mgmt_ip),
                    fw_version = COALESCE(ap.fw_version, EXCLUDED.fw_version),
                    agent_version = COALESCE(ap.agent_version, EXCLUDED.agent_version)
            """, [
                (
                    row["ap_id"],
                    row["site_id"],
                    row["name"],
                    row.get("mac"),
                    row.get("vendor"),
                    row.get("model"),
                    row.get("mgmt_ip"),
                    row.get("fw_version"),
                    row.get("agent_version")
                )
                for row in inventory["aps"]
            ])

            # 3. Register radios

            await conn.executemany("""
                INSERT INTO radio (
                    ap_id, radio, band
                )
                VALUES ($1, $2, $3)
                ON CONFLICT (ap_id, radio) DO NOTHING
            """, [
                (
                    row["ap_id"],
                    row["radio"],
                    row["band"]
                )
                for row in inventory["radios"]
            ])

            # 4. Register stations

            await conn.executemany("""
                INSERT INTO client (
                    sta_id, mac, mac_randomised,
                    hostname, device_type, ip,
                    first_seen, last_seen
                )
                VALUES (
                    $1, $2::text::macaddr, $3,
                    $4, $5, $6::text::inet,
                    $7, $7
                )
                ON CONFLICT (sta_id) DO UPDATE SET
                    last_seen = GREATEST(
                        client.last_seen,
                        EXCLUDED.last_seen
                    ),
                    hostname = COALESCE(
                        EXCLUDED.hostname,
                        client.hostname
                    ),
                    ip = COALESCE(
                        EXCLUDED.ip,
                        client.ip
                    )
                WHERE EXCLUDED.last_seen >= client.last_seen
            """, [
                (
                    row["sta_id"],
                    row.get("mac"),
                    row.get("mac_randomised", False),
                    row.get("hostname"),
                    row.get("device_type", "unknown"),
                    row.get("ip"),
                    parse_timestamp(row["time"])
                )
                for row in inventory["stations"]
            ])

            # 5. Register flows

            # The generator publishes SLA terms with flow inventory. Preserve them
            # in PostgreSQL so the API can join each flow to its configured SLA.
            sla_rows = {}
            for row in inventory['flows']:
                key = (row['site_id'], row.get('lat_bound_ms'), row.get('min_thpt_mbps'),
                       row.get('max_loss_pct'), row.get('max_jitter_ms'))
                sla_rows[key] = 'generator-' + hashlib.sha256(json.dumps(key).encode()).hexdigest()[:16]
            for key, class_id in sla_rows.items():
                await conn.execute('''INSERT INTO sla_class(sla_class_id,name,lat_bound_ms,min_thpt_mbps,max_loss_pct,max_jitter_ms)
                  VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(sla_class_id) DO UPDATE SET
                  lat_bound_ms=EXCLUDED.lat_bound_ms,min_thpt_mbps=EXCLUDED.min_thpt_mbps,
                  max_loss_pct=EXCLUDED.max_loss_pct,max_jitter_ms=EXCLUDED.max_jitter_ms''',
                  class_id,'Generator SLA',key[1],key[2],key[3],key[4])

            await conn.executemany("""
                INSERT INTO flow (
                    flow_id, site_id, name,
                    sta_id, direction, managed, sla_class_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (flow_id) DO UPDATE SET
                    sla_class_id=COALESCE(flow.sla_class_id,EXCLUDED.sla_class_id)
            """, [
                (
                    row["flow_id"],
                    row["site_id"],
                    row["flow_id"],
                    row["sta_id"],
                    row["direction"],
                    row.get("managed", True),
                    sla_rows[(row['site_id'], row.get('lat_bound_ms'), row.get('min_thpt_mbps'),
                              row.get('max_loss_pct'), row.get('max_jitter_ms'))]
                )
                for row in inventory["flows"]
            ])

    return {
        name: len(records)
        for name, records in inventory.items()
    }
