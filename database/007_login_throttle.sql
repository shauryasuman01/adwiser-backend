BEGIN;
CREATE TABLE IF NOT EXISTS login_failure (
  failure_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  email text NOT NULL,
  address text NOT NULL,
  ts timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS login_failure_email_ts_idx ON login_failure(email,ts DESC);
CREATE INDEX IF NOT EXISTS login_failure_address_ts_idx ON login_failure(address,ts DESC);
CREATE INDEX IF NOT EXISTS login_failure_ts_idx ON login_failure(ts);
COMMIT;
