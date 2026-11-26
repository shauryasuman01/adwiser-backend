BEGIN;

CREATE TABLE IF NOT EXISTS user_session (
    token_hash TEXT PRIMARY KEY,

    user_id TEXT NOT NULL
        REFERENCES app_user(user_id)
        ON DELETE CASCADE,

    session_version INTEGER NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_session_user
    ON user_session(user_id);

CREATE INDEX IF NOT EXISTS idx_session_expiry
    ON user_session(expires_at);

COMMIT;
