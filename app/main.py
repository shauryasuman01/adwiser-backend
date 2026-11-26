from app.routes.client_detail import router as client_detail_router
from app.routes.clients import router as clients_router
from app.routes.ap_detail import router as ap_detail_router
from app.routes.overview import router as overview_router
from app.routes.admin import router as admin_router
from app.middleware.auth import (AuthError,auth_error_handler)
from app.routes.auth import router as auth_router
from app.routes.aps import router as aps_router
from app.routes.flows import router as flows_router
from app.routes.alerts import router as alerts_router
from app.routes.events import router as events_router
from app.routes.scheduler import router as scheduler_router
from app.routes.signal import router as signal_router
import asyncio
import logging

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import uuid
from fastapi.exceptions import RequestValidationError

from app.database.postgres import (
    create_pool,
    check_postgres
)

from app.database.influx import (
    create_client,
    check_influx
)

from app.workers.inventory_sync import (
    run_inventory_sync
)
from app.workers.alert_evaluator import run_alert_evaluator
from app.workers.schedule_sync import run_schedule_sync


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):

    logger.info("Starting ADWISER backend")

    pool = await create_pool()

    try:
        async with create_client() as client:

            await check_postgres(pool)
            try:
                await check_influx(client)
            except Exception:
                logger.warning('InfluxDB unavailable at startup; telemetry routes will report unavailable')

            app.state.pg_pool = pool
            app.state.influx_client = client

            stop_event = asyncio.Event()

            worker = asyncio.create_task(
                run_inventory_sync(
                    pool,
                    client,
                    stop_event,
                    interval_s=60
                )
            )
            alert_worker = asyncio.create_task(run_alert_evaluator(pool, client, stop_event))
            schedule_worker = asyncio.create_task(run_schedule_sync(pool, client, stop_event))

            logger.info("ADWISER backend ready")

            try:
                yield

            finally:
                stop_event.set()

                try:
                    await asyncio.wait_for(
                        worker,
                        timeout=15
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Inventory worker shutdown timed out"
                    )
                    worker.cancel()
                try:
                    await asyncio.wait_for(alert_worker, timeout=15)
                except asyncio.TimeoutError:
                    alert_worker.cancel()
                try:
                    await asyncio.wait_for(schedule_worker, timeout=15)
                except asyncio.TimeoutError:
                    schedule_worker.cancel()

    finally:
        await pool.close()

        logger.info("ADWISER backend stopped")


app = FastAPI(
    title="ADWISER Backend",
    description="ADWISER network monitoring and management API",
    version="1.0.0",
    lifespan=lifespan
)
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(overview_router)
app.include_router(aps_router)
app.include_router(ap_detail_router)
app.include_router(clients_router)
app.include_router(client_detail_router)
app.include_router(flows_router)
app.include_router(alerts_router)
app.include_router(events_router)
app.include_router(scheduler_router)
app.include_router(signal_router)

allowed_origins = [s.strip() for s in os.getenv('CORS_ORIGINS', '').split(',') if s.strip()]
if allowed_origins:
    app.add_middleware(CORSMiddleware, allow_origins=allowed_origins, allow_credentials=True,
                       allow_methods=['GET', 'POST', 'DELETE'], allow_headers=['Content-Type'])

app.add_exception_handler(
    AuthError,
    auth_error_handler
)

@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=400, headers={'Cache-Control':'no-store'},
        content={'error':{'code':'INVALID_REQUEST','message':'Invalid request parameters.',
                          'requestId':str(uuid.uuid4()),'retryable':False}})

@app.exception_handler(Exception)
async def internal_error(request: Request, exc: Exception):
    logger.exception('Unhandled API error')
    return JSONResponse(status_code=500, headers={'Cache-Control':'no-store'},
        content={'error':{'code':'INTERNAL_ERROR','message':'Internal server error.',
                          'requestId':str(uuid.uuid4()),'retryable':True}})

@app.middleware('http')
async def no_store(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith('/api/v1'):
        response.headers['Cache-Control'] = 'no-store'
    return response

@app.get("/")
async def root():

    return {
        "service": "ADWISER Backend",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health(request: Request):

    postgres, influx = await asyncio.gather(
        asyncio.wait_for(
            check_postgres(request.app.state.pg_pool),
            timeout=5
        ),
        check_influx(request.app.state.influx_client),
        return_exceptions=True
    )

    postgres_ok = not isinstance(postgres, BaseException)
    influx_ok = not isinstance(influx, BaseException)

    healthy = postgres_ok and influx_ok

    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "status": "healthy" if healthy else "unhealthy",
            "postgres": "connected" if postgres_ok else "unavailable",
            "influxdb": "connected" if influx_ok else "unavailable"
        }
    )


@app.get('/ready')
async def ready(request: Request):
    return await health(request)
