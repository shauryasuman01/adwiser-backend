BEGIN;
CREATE TABLE IF NOT EXISTS alert_note (
  note_id text PRIMARY KEY,
  alert_id text NOT NULL REFERENCES alert(alert_id) ON DELETE CASCADE,
  user_id text NOT NULL REFERENCES app_user(user_id),
  ts timestamptz NOT NULL DEFAULT now(),
  text text NOT NULL CHECK (length(text) BETWEEN 1 AND 2000)
);
CREATE TABLE IF NOT EXISTS event_note (
  note_id text PRIMARY KEY,
  event_id text NOT NULL REFERENCES event(event_id) ON DELETE CASCADE,
  user_id text NOT NULL REFERENCES app_user(user_id),
  ts timestamptz NOT NULL DEFAULT now(),
  text text NOT NULL CHECK (length(text) BETWEEN 1 AND 2000)
);
CREATE TABLE IF NOT EXISTS schedule_version (
  schedule_ver bigint NOT NULL,
  site_id text NOT NULL REFERENCES site(site_id),
  computed_at timestamptz NOT NULL,
  trigger text NOT NULL,
  superframe_us integer NOT NULL CHECK (superframe_us > 0),
  slot_us integer NOT NULL CHECK (slot_us > 0),
  n_aps integer NOT NULL CHECK (n_aps >= 0),
  n_flows integer NOT NULL CHECK (n_flows >= 0),
  alloc_pct numeric(6,3) NOT NULL CHECK (alloc_pct BETWEEN 0 AND 100),
  allocations jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(allocations) = 'array'),
  PRIMARY KEY (site_id, schedule_ver)
);
CREATE INDEX IF NOT EXISTS alert_note_alert_idx ON alert_note(alert_id);
CREATE INDEX IF NOT EXISTS event_note_event_idx ON event_note(event_id);
CREATE INDEX IF NOT EXISTS schedule_site_computed_idx ON schedule_version(site_id, computed_at DESC);
COMMIT;
