import uuid

from fastapi import Depends, Request
from fastapi.responses import JSONResponse

from app.services.session import (
    get_current_user,
    PERMISSIONS
)


class AuthError(Exception):

    def __init__(self, status, code, message):

        self.status = status
        self.code = code
        self.message = message

        super().__init__(message)


async def auth_error_handler(
    request: Request,
    exc: AuthError
):

    return JSONResponse(
        status_code=exc.status,
        headers={"Cache-Control": "no-store"},
        content={
            "error": {
                "code": exc.code,
                "message": exc.message,
                "requestId": str(uuid.uuid4()),
                "retryable": exc.status in (429, 502, 503)
            }
        }
    )


async def require_user(request: Request):

    user = await get_current_user(request)

    if user is None:

        raise AuthError(
            401,
            "UNAUTHENTICATED",
            "Sign in to continue."
        )

    return user


def require_permission(permission: str):

    if permission not in PERMISSIONS:

        raise ValueError(
            f"Unknown permission: {permission}"
        )

    async def check_permission(
        request: Request,
        user=Depends(require_user)
    ):

        if user["role"] == "superadmin":
            return user

        pool = request.app.state.pg_pool

        allowed = await pool.fetchval("""
            SELECT EXISTS (
                SELECT 1
                FROM role_permission
                WHERE role = $1
                  AND permission = $2
            )
        """,
            user["role"],
            permission
        )

        if not allowed:

            raise AuthError(
                403,
                "FORBIDDEN",
                "Insufficient permission."
            )

        return user

    return check_permission
