import asyncio
import json
import os
import uuid
import ipaddress

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.utils.passwords import (
    hash_password,
    verify_password
)

from app.services.session import (
    COOKIE_NAME,
    SESSION_DURATION,
    create_session,
    get_current_user,
    delete_session,
    session_response
)


router = APIRouter(
    prefix="/api/v1/adwiser",
    tags=["Authentication"]
)

SECURE_COOKIE = (
    os.getenv("NODE_ENV") == "production"
)

DUMMY_HASH = hash_password(uuid.uuid4().hex)

PASSWORD_LIMIT = asyncio.Semaphore(2)


def error_response(status, code, message):

    return JSONResponse(
        status_code=status,
        headers={"Cache-Control": "no-store"},
        content={
            "error": {
                "code": code,
                "message": message,
                "requestId": str(uuid.uuid4()),
                "retryable": status == 429
            }
        }
    )


@router.post("/session")
async def login(request: Request):

    try:
        content_type = request.headers.get(
            "content-type", ""
        ).split(";")[0].strip().lower()

        if content_type != "application/json":
            raise ValueError("Expected JSON")

        raw = bytearray()

        async for chunk in request.stream():
            raw.extend(chunk)

            if len(raw) > 8192:
                raise ValueError("Request too large")

        body = json.loads(raw)

        if not isinstance(body, dict):
            raise ValueError("Invalid body")

        if set(body) != {"email", "password"}:
            raise ValueError("Invalid fields")

        email = body["email"].strip().lower()
        password = body["password"]

        if (
            not email
            or len(email) > 254
            or not isinstance(password, str)
            or not 1 <= len(password) <= 200
        ):
            raise ValueError("Invalid credentials")

    except (ValueError, TypeError, AttributeError):

        return error_response(
            400,
            "INVALID_REQUEST",
            "Email and password are required."
        )

    pool = request.app.state.pg_pool
    address = request.client.host if request.client else 'unknown'
    trusted = {s.strip() for s in os.getenv('TRUSTED_PROXY_IPS','').split(',') if s.strip()}
    if address in trusted and request.headers.get('x-real-ip'):
        try:address = str(ipaddress.ip_address(request.headers['x-real-ip']))
        except ValueError:pass
    async with pool.acquire() as conn, conn.transaction():
        # Serialize checks and inserts for both email and client address.
        await conn.execute('SELECT pg_advisory_xact_lock(7241,hashtext($1))',address)
        await conn.execute('SELECT pg_advisory_xact_lock(7242,hashtext($1))',email)
        await conn.execute("DELETE FROM login_failure WHERE ts < now() - interval '10 minutes'")
        failures = await conn.fetchrow('''SELECT
          (SELECT count(*) FROM login_failure WHERE email=$1 AND ts>now()-interval '1 minute') AS email_count,
          (SELECT count(*) FROM login_failure WHERE address=$2 AND ts>now()-interval '10 minutes') AS address_count''',email,address)
        if failures['email_count'] >= 5 or failures['address_count'] >= 20:
            return error_response(429,'RATE_LIMITED','Too many sign-in attempts. Try again later.')
        user = await conn.fetchrow('SELECT * FROM app_user WHERE LOWER(email)=$1',email)
        async with PASSWORD_LIMIT:
            valid = await asyncio.to_thread(verify_password,password,user['password_hash'] if user else DUMMY_HASH)
        if not user or not valid:
            await conn.execute('INSERT INTO login_failure(email,address) VALUES($1,$2)',email,address)
            return error_response(401,'INVALID_CREDENTIALS','Email or password is incorrect.')
        if not user['active']:
            return error_response(403,'ACCOUNT_DISABLED','This account is disabled.')
        fresh = await conn.fetchrow("""
                UPDATE app_user
                SET last_login_at = NOW()
                WHERE user_id = $1
                  AND active = TRUE
                  AND password_hash = $2
                  AND session_version = $3
                RETURNING *
            """,user['user_id'],user['password_hash'],user['session_version'])
        if fresh is None:
            return error_response(401,'INVALID_CREDENTIALS','Please sign in again.')
        token = await create_session(conn, fresh)

    response = JSONResponse(
        content=await session_response(pool, fresh),
        headers={"Cache-Control": "no-store"}
    )

    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=SESSION_DURATION,
        httponly=True,
        secure=SECURE_COOKIE,
        samesite="strict",
        path="/api/v1"
    )

    return response


@router.get("/session")
async def current_session(request: Request):

    pool = request.app.state.pg_pool

    user = await get_current_user(request)

    return JSONResponse(
        content=await session_response(pool, user),
        headers={"Cache-Control": "no-store"}
    )


@router.delete("/session")
async def logout(request: Request):

    pool = request.app.state.pg_pool

    user = await get_current_user(request)

    if user is None:

        return error_response(
            401,
            "UNAUTHENTICATED",
            "Sign in to continue."
        )

    await delete_session(
        pool,
        request.cookies.get(COOKIE_NAME)
    )

    response = JSONResponse(
        content=await session_response(pool, None),
        headers={"Cache-Control": "no-store"}
    )

    response.delete_cookie(
        key=COOKIE_NAME,
        path="/api/v1",
        httponly=True,
        secure=SECURE_COOKIE,
        samesite="strict"
    )

    return response
