import asyncio
import uuid

from getpass import getpass

from app.database.postgres import create_pool
from app.utils.passwords import hash_password


async def create_admin(name, email, password):

    password_hash = await asyncio.to_thread(
        hash_password,
        password
    )

    pool = await create_pool()

    try:
        async with pool.acquire() as conn:

            async with conn.transaction():

                await conn.execute("""
                    LOCK TABLE app_user
                    IN SHARE ROW EXCLUSIVE MODE
                """)

                exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM app_user
                        WHERE role = 'superadmin'
                    )
                """)

                if exists:
                    print("Super Admin already exists.")
                    return

                await conn.execute("""
                    INSERT INTO app_user (
                        user_id,
                        name,
                        email,
                        role,
                        password_hash
                    )
                    VALUES (
                        $1, $2, $3,
                        'superadmin', $4
                    )
                """,
                    str(uuid.uuid4()),
                    name,
                    email,
                    password_hash
                )

        print("Super Admin created successfully.")

    finally:
        await pool.close()


def main():

    name = input("Admin name: ").strip()
    email = input("Admin email: ").strip().lower()

    password = getpass("Password: ")
    confirm = getpass("Confirm password: ")

    if not name or len(name) > 100:
        raise ValueError("Invalid name")

    if "@" not in email or len(email) > 254:
        raise ValueError("Invalid email")

    if not 12 <= len(password) <= 128:
        raise ValueError(
            "Password must contain 12 to 128 characters"
        )

    if password != confirm:
        raise ValueError("Passwords do not match")

    asyncio.run(
        create_admin(name, email, password)
    )


if __name__ == "__main__":
    main()
