import httpx

from app.config import env


def create_client():

    return httpx.AsyncClient(
        base_url=env.INFLUX_URL,
        headers={
            "Authorization": f"Bearer {env.INFLUX_TOKEN}",
            "Accept": "application/json"
        },
        timeout=10.0
    )


async def query_influx(client, sql):

    response = await client.get(
        "/api/v3/query_sql",
        params={
            "db": env.INFLUX_DATABASE,
            "q": sql,
            "format": "json"
        }
    )

    if not response.is_success:
        raise RuntimeError(
            f"InfluxDB query failed: HTTP {response.status_code}"
        )

    return response.json()


async def check_influx(client):

    return await query_influx(
        client,
        "SELECT 1 AS healthy"
    )
