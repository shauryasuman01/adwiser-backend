import hashlib
import secrets


COOKIE_NAME = "adw_session"
SESSION_DURATION = 43200

PERMISSIONS = [
    "scheduler.view",
    "details.engineering",
    "mac.full",
    "resolution.1s",
    "alerts.acknowledge",
    "alerts.resolve",
    "notes.add",
    "export.csv",
    "admin.manage"
]


def hash_token(token):

    return hashlib.sha256(
        token.encode("utf-8")
    ).hexdigest()


async def create_session(conn, user):

    token = secrets.token_urlsafe(32)

    await conn.execute("""
        INSERT INTO user_session (
            token_hash,
            user_id,
            session_version,
            expires_at
        )
        VALUES (
            $1, $2, $3,
            NOW() + INTERVAL '12 hours'
        )
    """,
        hash_token(token),
        user["user_id"],
        user["session_version"]
    )

    return token


async def get_current_user(request):

    token = request.cookies.get(COOKIE_NAME)

    if not token or len(token) != 43:
        return None

    pool = request.app.state.pg_pool

    return await pool.fetchrow("""
        SELECT u.*
        FROM app_user u
        JOIN user_session s
            ON u.user_id = s.user_id
        WHERE s.token_hash = $1
          AND s.expires_at > NOW()
          AND u.active = TRUE
          AND u.session_version = s.session_version
    """, hash_token(token))


async def delete_session(pool, token):

    if token:
        await pool.execute("""
            DELETE FROM user_session
            WHERE token_hash = $1
        """, hash_token(token))


def epoch_ms(value):

    if value is None:
        return None

    return int(value.timestamp() * 1000)


async def session_response(pool, user):

    if user is None:
        public_user = None
        permissions = []

    else:

        public_user = {
            "userId": user["user_id"],
            "name": user["name"],
            "email": user["email"],
            "role": user["role"],
            "active": user["active"],
            "createdAt": epoch_ms(user["created_at"]),
            "lastLoginAt": epoch_ms(user["last_login_at"])
        }

        if user["role"] == "superadmin":

            permissions = PERMISSIONS.copy()

        else:

            rows = await pool.fetch("""
                SELECT permission
                FROM role_permission
                WHERE role = $1
                ORDER BY permission
            """, user["role"])

            permissions = [
                row["permission"]
                for row in rows
            ]

    return {
        "user": public_user,
        "permissions": permissions,
        "demoAccounts": [],
        "demoPassword": None
    }
