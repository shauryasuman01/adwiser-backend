"""Account and role administration, using the existing dashboard contract."""
import asyncio
import json
import re
import time
import uuid

import asyncpg
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.middleware.auth import AuthError, require_permission
from app.services.session import PERMISSIONS, epoch_ms, hash_token, COOKIE_NAME
from app.utils.passwords import hash_password

router = APIRouter(prefix="/api/v1/adwiser/admin", tags=["Administration"])
admin_required = require_permission("admin.manage")
ROLES = {"operator": "Operator", "engineer": "Engineer", "superadmin": "Super Admin"}
LABELS = {
    "scheduler.view": ("View scheduler", "Access scheduler and queue details"),
    "details.engineering": ("Engineering details", "See agent, queue, ACK, and event diagnostics"),
    "mac.full": ("Full MAC addresses", "View unmasked client MAC addresses"),
    "resolution.1s": ("One second resolution", "Use one second telemetry buckets"),
    "alerts.acknowledge": ("Acknowledge alerts", "Acknowledge open alerts"),
    "alerts.resolve": ("Resolve alerts", "Resolve alerts"),
    "notes.add": ("Add notes", "Add alert and event notes"),
    "export.csv": ("Export CSV", "Export view data"),
    "admin.manage": ("Manage accounts", "Manage users and role permissions"),
}
HASH_LIMIT = asyncio.Semaphore(2)


def fail(status, code, message):
    raise AuthError(status, code, message)


async def read_body(request):
    if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
        fail(415, "INVALID_REQUEST", "Content-Type must be application/json.")
    raw = bytearray()
    async for chunk in request.stream():
        if len(raw) + len(chunk) > 16384:
            fail(413, "INVALID_REQUEST", "Request body exceeds 16 KB.")
        raw.extend(chunk)
    try:
        body = json.loads(raw or b"{}")
    except (ValueError, UnicodeError):
        fail(400, "INVALID_REQUEST", "Request body must be valid JSON.")
    if not isinstance(body, dict):
        fail(400, "INVALID_REQUEST", "Request body must be a JSON object.")
    return body


def public_user(row):
    return {"userId": row["user_id"], "name": row["name"], "email": row["email"],
            "role": row["role"], "active": row["active"],
            "createdAt": epoch_ms(row["created_at"]), "lastLoginAt": epoch_ms(row["last_login_at"])}


async def users_data(db):
    rows = await db.fetch("SELECT user_id,name,email,role,active,created_at,last_login_at FROM app_user ORDER BY created_at,user_id")
    return {"users": [public_user(row) for row in rows],
            "roles": [{"role": role, "label": label} for role, label in ROLES.items()]}


async def roles_data(db):
    rows = await db.fetch("SELECT role,permission FROM role_permission ORDER BY role,permission")
    return {"permissions": [{"key": key, "label": LABELS[key][0], "description": LABELS[key][1],
                             "locked": key == "admin.manage"} for key in PERMISSIONS],
            "roles": [{"role": role, "label": label, "editable": role != "superadmin",
                       "granted": list(PERMISSIONS) if role == "superadmin" else
                       [r["permission"] for r in rows if r["role"] == role]} for role,label in ROLES.items()]}


async def get_scope(db, request):
    sites = await db.fetch("SELECT site_id,name,timezone,sla_target_pct FROM site ORDER BY name,site_id")
    requested = request.query_params.get("site")
    if not sites:
        fail(503, "INTERNAL_ERROR", "No sites are configured.")
    chosen = next((s for s in sites if s["site_id"] == requested), None) if requested else sites[0]
    if chosen is None:
        fail(400, "INVALID_REQUEST", "Unknown site.")
    return sites, chosen


def time_window(request):
    params = request.query_params
    now = time.time_ns() // 1_000_000
    range_name = params.get("range", "live")
    spans = {"live": 300_000, "15m": 900_000, "1h": 3_600_000,
             "6h": 21_600_000, "24h": 86_400_000, "7d": 604_800_000}
    if range_name == "custom":
        try:
            start, end = int(params["from"]), int(params["to"])
        except (KeyError, ValueError):
            fail(400, "INVALID_REQUEST", "Custom range requires from/to epoch milliseconds.")
        if end <= start or end > now or end - start > 400 * 86_400_000:
            fail(400, "INVALID_REQUEST", "Invalid custom range.")
        if start >= end:
            fail(400, "INVALID_REQUEST", "Custom range must end after it starts.")
    elif range_name in spans and "from" not in params and "to" not in params:
        start, end = now - spans[range_name], now
    else:
        fail(400, "INVALID_REQUEST", "Invalid range.")
    span = end - start
    minimum = 1 if span <= 900_000 else 10 if span <= 21_600_000 else 60 if span <= 604_800_000 else 3600
    steps = [1,2,5,10,15,30,60,120,300,600,900,1800,3600,7200,18000,36000,72000,180000,360000]
    bucket = next((s for s in steps if s >= minimum and span <= s * 600_000), steps[-1])
    # Administration reads PostgreSQL, not historical telemetry; don't claim a rollup was read.
    return {"range": range_name, "from": start, "to": end, "bucketS": bucket, "tier": "raw"}


async def view(db, request, actor, data, scope, window):
    sites, chosen = scope
    def site_view(row):
        return {"siteId": row["site_id"], "name": row["name"], "timezone": row["timezone"],
                "slaTargetPct": row["sla_target_pct"]}
    counts = {"critical": 0, "warning": 0, "info": 0}
    for row in await db.fetch("SELECT severity,count(*) AS count FROM alert WHERE site_id=$1 AND ended_at IS NULL AND status<>'resolved' GROUP BY severity", chosen["site_id"]):
        counts[row["severity"]] = row["count"]
    return JSONResponse(headers={"Cache-Control": "no-store"}, content={"meta": {
        "user": public_user(actor), "permissions": list(PERMISSIONS),
        "sites": [site_view(s) for s in sites], "site": site_view(chosen),
        "window": window, "generatedAt": time.time_ns() // 1_000_000,
        "source": "api", "openAlerts": counts,
        "refreshS": 1 if window["range"] == "live" else 86400}, "data": data})


def validate_user(body, partial=False):
    required = {"name", "email", "role", "password"}
    allowed = required | ({"active"} if partial else set())
    if not body or not set(body) <= allowed or (not partial and set(body) != required):
        fail(400, "INVALID_REQUEST", "Provide valid account fields.")
    body = dict(body)
    if "name" in body:
        if not isinstance(body["name"], str) or not 1 <= len(body["name"].strip()) <= 100:
            fail(400, "INVALID_REQUEST", "Name must contain 1 to 100 characters.")
        body["name"] = body["name"].strip()
    if "email" in body:
        if not isinstance(body["email"], str) or len(body["email"]) > 254 or not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", body["email"].strip()):
            fail(400, "INVALID_REQUEST", "A valid email address is required.")
        body["email"] = body["email"].strip().lower()
    if "role" in body and (not isinstance(body["role"], str) or body["role"] not in ROLES):
        fail(400, "INVALID_REQUEST", "Invalid role.")
    if "active" in body and type(body["active"]) is not bool:
        fail(400, "INVALID_REQUEST", "active must be boolean.")
    if "password" in body and (not isinstance(body["password"], str) or not 10 <= len(body["password"]) <= 200):
        fail(400, "INVALID_REQUEST", "Password must contain 10 to 200 characters.")
    return body


async def password_digest(password):
    async with HASH_LIMIT:
        return await asyncio.to_thread(hash_password, password)


async def lock_admin(conn, request, actor):
    await conn.execute("SELECT pg_advisory_xact_lock($1::bigint)", 7347001)
    # Recheck authority after serializing changes, including session revocation.
    current = await conn.fetchrow("""SELECT u.user_id FROM app_user u JOIN user_session s ON s.user_id=u.user_id
        WHERE u.user_id=$1 AND u.role='superadmin' AND u.active AND s.token_hash=$2
          AND s.expires_at>now() AND s.session_version=u.session_version FOR UPDATE OF u""",
        actor["user_id"], hash_token(request.cookies.get(COOKIE_NAME, "")))
    if current is None:
        fail(403, "FORBIDDEN", "Administrator authority is no longer valid.")


async def audit(conn, scope, actor, kind, entity_id, message, details):
    await conn.execute("""INSERT INTO event(event_id,site_id,type,severity,entity_type,entity_id,message,actor,details)
        VALUES($1,$2,$3,'info','user',$4,$5,$6,$7::jsonb)""",
        "ev-" + str(uuid.uuid4()), scope[1]["site_id"], kind, entity_id, message, actor["user_id"], json.dumps(details))


@router.get("/users")
async def list_users(request: Request, actor=Depends(admin_required)):
    db = request.app.state.pg_pool
    scope, window = await get_scope(db, request), time_window(request)
    return await view(db, request, actor, await users_data(db), scope, window)


@router.post("/users")
async def create_user(request: Request, actor=Depends(admin_required)):
    db = request.app.state.pg_pool
    body = validate_user(await read_body(request))
    digest = await password_digest(body["password"])
    scope, window = await get_scope(db, request), time_window(request)
    uid = "u-" + str(uuid.uuid4())
    try:
        async with db.acquire() as conn, conn.transaction():
            await lock_admin(conn, request, actor)
            await conn.execute("INSERT INTO app_user(user_id,name,email,role,password_hash) VALUES($1,$2,$3,$4,$5)",
                               uid, body["name"], body["email"], body["role"], digest)
            await audit(conn, scope, actor, "user_changed", uid, "Created account " + body["email"], {"change": "created"})
    except asyncpg.UniqueViolationError:
        fail(409, "CONFLICT", "An account with that email already exists.")
    return await view(db, request, actor, await users_data(db), scope, window)


async def change_user(request, actor, user_id, body, deleting=False):
    db = request.app.state.pg_pool
    scope, window = await get_scope(db, request), time_window(request)
    digest = await password_digest(body["password"]) if "password" in body else None
    try:
        async with db.acquire() as conn, conn.transaction():
            await lock_admin(conn, request, actor)
            target = await conn.fetchrow("SELECT * FROM app_user WHERE user_id=$1 FOR UPDATE", user_id)
            if target is None:
                fail(404, "NOT_FOUND", "User was not found.")
            role = body.get("role", target["role"])
            active = body.get("active", target["active"])
            if user_id == actor["user_id"] and (role != target["role"] or not active):
                fail(409, "CONFLICT", "You cannot change your own role or disable yourself.")
            if target["role"] == "superadmin" and target["active"] and (role != "superadmin" or not active):
                count = await conn.fetchval("SELECT count(*) FROM app_user WHERE role='superadmin' AND active")
                if count <= 1:
                    fail(409, "CONFLICT", "At least one active Super Admin must remain.")
            revoke = digest is not None or not active or role != target["role"]
            await conn.execute("""UPDATE app_user SET name=$2,email=$3,role=$4,active=$5,
                password_hash=COALESCE($6,password_hash),session_version=session_version+$7 WHERE user_id=$1""",
                user_id,body.get("name",target["name"]),body.get("email",target["email"]),role,active,digest,int(revoke))
            if revoke:
                await conn.execute("DELETE FROM user_session WHERE user_id=$1", user_id)
            await audit(conn,scope,actor,"user_changed",user_id,
                        ("Disabled account " if deleting else "Updated account ") + target["email"],
                        {"change":"deleted"} if deleting else {"changed":list(body)})
    except asyncpg.UniqueViolationError:
        fail(409, "CONFLICT", "An account with that email already exists.")
    return await view(db,request,actor,await users_data(db),scope,window)


@router.post("/users/{user_id}")
async def update_user(user_id: str, request: Request, actor=Depends(admin_required)):
    return await change_user(request,actor,user_id,validate_user(await read_body(request),partial=True))


@router.post("/users/{user_id}/delete")
async def disable_user(user_id: str, request: Request, actor=Depends(admin_required)):
    if await read_body(request):
        fail(400,"INVALID_REQUEST","This action requires an empty JSON object.")
    return await change_user(request,actor,user_id,{"active":False},deleting=True)


@router.get("/roles")
async def list_roles(request: Request, actor=Depends(admin_required)):
    db = request.app.state.pg_pool
    scope, window = await get_scope(db,request), time_window(request)
    return await view(db,request,actor,await roles_data(db),scope,window)


@router.post("/roles/{role}")
async def update_role(role: str, request: Request, actor=Depends(admin_required)):
    if role == "superadmin":
        fail(409,"CONFLICT","Super Admin permissions cannot be edited.")
    if role not in ROLES:
        fail(404,"NOT_FOUND","Role was not found.")
    body = await read_body(request)
    if set(body) != {"permissions"} or not isinstance(body["permissions"],list) or any(
        not isinstance(p,str) or p not in PERMISSIONS or p == "admin.manage" for p in body["permissions"]):
        fail(400,"INVALID_REQUEST","Provide valid, non-locked permission keys.")
    next_grants = sorted(set(body["permissions"]))
    db = request.app.state.pg_pool
    scope,window = await get_scope(db,request),time_window(request)
    async with db.acquire() as conn, conn.transaction():
        await lock_admin(conn,request,actor)
        old = {r["permission"] for r in await conn.fetch("SELECT permission FROM role_permission WHERE role=$1",role)}
        await conn.execute("DELETE FROM role_permission WHERE role=$1",role)
        await conn.executemany("INSERT INTO role_permission(role,permission) VALUES($1,$2)",[(role,p) for p in next_grants])
        await audit(conn,scope,actor,"role_access_changed",role,"Updated " + role + " permissions",
                    {"added":sorted(set(next_grants)-old),"removed":sorted(old-set(next_grants))})
    return await view(db,request,actor,await roles_data(db),scope,window)
