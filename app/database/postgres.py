import asyncpg

from app.config import env


async def create_pool():

    return await asyncpg.create_pool(
        host=env.PG_HOST,
        port=env.PG_PORT,
        database=env.PG_DATABASE,
        user=env.PG_USER,
        password=env.PG_PASSWORD,
        min_size=1,
        max_size=10,
        timeout=5
    )


async def check_postgres(pool):

    return await pool.fetchval(
        "SELECT current_database()"
    )
