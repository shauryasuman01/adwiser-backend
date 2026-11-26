BEGIN;

CREATE TABLE IF NOT EXISTS alert_rule (
  rule_id text PRIMARY KEY, name text NOT NULL,
  entity_type text NOT NULL CHECK (entity_type IN ('site','ap','client','flow','controller','user')),
  metric text NOT NULL, operator text NOT NULL CHECK (operator IN ('>','<','=','>=','<=')),
  threshold numeric, threshold_ref text, duration_s integer NOT NULL CHECK (duration_s >= 0),
  severity text NOT NULL CHECK (severity IN ('critical','warning','info')), enabled boolean NOT NULL DEFAULT true
);
CREATE TABLE IF NOT EXISTS alert (
  alert_id text PRIMARY KEY, site_id text NOT NULL REFERENCES site(site_id), rule_id text REFERENCES alert_rule(rule_id),
  source text NOT NULL CHECK (source IN ('rule','anomaly')), detector text, title text NOT NULL,
  severity text NOT NULL CHECK (severity IN ('critical','warning','info')),
  entity_type text NOT NULL CHECK (entity_type IN ('site','ap','client','flow','controller','user')),
  entity_id text NOT NULL, score numeric(4,3) CHECK (score BETWEEN 0 AND 1),
  started_at timestamptz NOT NULL, ended_at timestamptz,
  status text NOT NULL DEFAULT 'open' CHECK (status IN ('open','acknowledged','resolved')),
  acked_by text REFERENCES app_user(user_id), acked_at timestamptz,
  resolved_by text REFERENCES app_user(user_id), resolved_at timestamptz,
  context jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE TABLE IF NOT EXISTS event (
  event_id text PRIMARY KEY, site_id text REFERENCES site(site_id), ts timestamptz NOT NULL DEFAULT now(),
  type text NOT NULL CHECK (type IN ('association','disassociation','roam','channel_change','agent_connected','agent_disconnected','schedule_recomputed','alert_raised','alert_cleared','alert_acknowledged','alert_resolved','note_added','user_signed_in','user_changed','role_access_changed')),
  severity text NOT NULL DEFAULT 'info' CHECK (severity IN ('critical','warning','info')),
  entity_type text CHECK (entity_type IN ('site','ap','client','flow','controller','user')),
  entity_id text, message text NOT NULL, actor text NOT NULL DEFAULT 'system', details jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS event_site_ts_idx ON event(site_id, ts DESC);
CREATE INDEX IF NOT EXISTS event_entity_ts_idx ON event(entity_type, entity_id, ts DESC);
CREATE INDEX IF NOT EXISTS alert_site_status_started_idx ON alert(site_id, status, started_at DESC);

COMMIT;
