BEGIN;

ALTER TABLE schedule_version ADD COLUMN IF NOT EXISTS run_id text;
ALTER TABLE schedule_version ADD COLUMN IF NOT EXISTS total_slots integer;
ALTER TABLE schedule_version ADD COLUMN IF NOT EXISTS used_pct numeric(6,3);
ALTER TABLE schedule_version ADD COLUMN IF NOT EXISTS metadata_bytes integer;

UPDATE schedule_version SET run_id='legacy' WHERE run_id IS NULL;
ALTER TABLE schedule_version ALTER COLUMN run_id SET NOT NULL;
ALTER TABLE schedule_version ALTER COLUMN run_id SET DEFAULT 'legacy';

ALTER TABLE schedule_version DROP CONSTRAINT IF EXISTS schedule_version_pkey;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='schedule_version_run_pkey') THEN
    ALTER TABLE schedule_version ADD CONSTRAINT schedule_version_run_pkey
      PRIMARY KEY(site_id,run_id,schedule_ver);
  END IF;
END $$;

ALTER TABLE schedule_version DROP CONSTRAINT IF EXISTS schedule_total_slots_check;
ALTER TABLE schedule_version ADD CONSTRAINT schedule_total_slots_check
  CHECK(total_slots IS NULL OR total_slots > 0);
ALTER TABLE schedule_version DROP CONSTRAINT IF EXISTS schedule_used_pct_check;
ALTER TABLE schedule_version ADD CONSTRAINT schedule_used_pct_check
  CHECK(used_pct IS NULL OR used_pct BETWEEN 0 AND 100);
ALTER TABLE schedule_version DROP CONSTRAINT IF EXISTS schedule_metadata_bytes_check;
ALTER TABLE schedule_version ADD CONSTRAINT schedule_metadata_bytes_check
  CHECK(metadata_bytes IS NULL OR metadata_bytes >= 0);

CREATE INDEX IF NOT EXISTS schedule_identity_idx
  ON schedule_version(site_id,run_id,schedule_ver);

COMMIT;
