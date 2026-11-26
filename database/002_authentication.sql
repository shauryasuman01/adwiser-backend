BEGIN;

-- User accounts

CREATE TABLE IF NOT EXISTS app_user (
    user_id TEXT PRIMARY KEY,

    name TEXT NOT NULL CHECK (
        LENGTH(name) BETWEEN 1 AND 100
    ),

    email TEXT NOT NULL,

    role TEXT NOT NULL CHECK (
        role IN (
            'operator',
            'engineer',
            'superadmin'
        )
    ),

    password_hash TEXT NOT NULL,

    session_version INTEGER NOT NULL DEFAULT 1,

    active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    last_login_at TIMESTAMPTZ
);


CREATE UNIQUE INDEX IF NOT EXISTS app_user_email_lower_uq
    ON app_user (LOWER(email));


-- Role permissions

CREATE TABLE IF NOT EXISTS role_permission (

    role TEXT NOT NULL CHECK (
        role IN ('operator', 'engineer')
    ),

    permission TEXT NOT NULL,

    PRIMARY KEY (role, permission),

    CHECK (permission <> 'admin.manage')
);


-- Default permissions

INSERT INTO role_permission (role, permission)
VALUES
    ('operator', 'alerts.acknowledge'),
    ('operator', 'export.csv'),

    ('engineer', 'scheduler.view'),
    ('engineer', 'details.engineering'),
    ('engineer', 'mac.full'),
    ('engineer', 'resolution.1s'),
    ('engineer', 'alerts.acknowledge'),
    ('engineer', 'alerts.resolve'),
    ('engineer', 'notes.add'),
    ('engineer', 'export.csv')

ON CONFLICT DO NOTHING;

COMMIT;
