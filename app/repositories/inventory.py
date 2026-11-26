import asyncio

from app.database.influx import query_influx


INVENTORY_TABLES = {
    "sites": (
        "site_inventory",
        ["site_id"]
    ),
    "aps": (
        "ap_inventory",
        ["site_id", "ap_id"]
    ),
    "radios": (
        "radio_inventory",
        ["site_id", "ap_id", "radio"]
    ),
    "stations": (
        "sta_inventory",
        ["site_id", "sta_id"]
    ),
    "flows": (
        "flow_inventory",
        ["site_id", "flow_id"]
    )
}


async def get_latest_records(client, table, keys):

    partition = ", ".join(keys)

    sql = f"""
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {partition}
                    ORDER BY time DESC
                ) AS row_num
            FROM {table}
        ) AS ranked
        WHERE row_num = 1
    """

    records = await query_influx(client, sql)

    for record in records:
        record.pop("row_num", None)

    return records


async def get_inventory(client):

    tasks = [
        get_latest_records(client, table, keys)
        for table, keys in INVENTORY_TABLES.values()
    ]

    results = await asyncio.gather(*tasks)

    return dict(
        zip(INVENTORY_TABLES.keys(), results)
    )
