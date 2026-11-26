import asyncio
import logging

from app.repositories.inventory import get_inventory
from app.repositories.registration import register_inventory


logger = logging.getLogger(__name__)


async def sync_once(pool, client):

    inventory = await get_inventory(client)

    result = await register_inventory(
        pool,
        inventory
    )

    logger.info(
        "Inventory synchronisation successful: %s",
        result
    )

    return result


async def run_inventory_sync(
    pool,
    client,
    stop_event,
    interval_s=60
):

    if interval_s <= 0:
        raise ValueError("Invalid synchronisation interval")

    logger.info(
        "Inventory worker started"
    )

    while not stop_event.is_set():

        try:
            await sync_once(pool, client)

        except Exception:
            logger.exception(
                "Inventory synchronisation failed"
            )

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=interval_s
            )

        except asyncio.TimeoutError:
            pass

    logger.info(
        "Inventory worker stopped"
    )
