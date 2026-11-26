BEGIN;

-- 1. Site
CREATE TABLE IF NOT EXISTS site (
    site_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'Asia/Kolkata',
    sla_target_pct DOUBLE PRECISION DEFAULT 99.0
        CHECK (sla_target_pct BETWEEN 0 AND 100)
);


-- 2. Floor
CREATE TABLE IF NOT EXISTS floor (
    floor_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES site(site_id),
    name TEXT NOT NULL,
    plan_image_url TEXT,
    width_m DOUBLE PRECISION,
    height_m DOUBLE PRECISION,
    rooms JSONB NOT NULL DEFAULT '[]'::jsonb,

    UNIQUE (site_id, floor_id)
);


-- 3. Access Point
CREATE TABLE IF NOT EXISTS ap (
    ap_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES site(site_id),
    floor_id TEXT,
    name TEXT NOT NULL,
    mac MACADDR UNIQUE,
    vendor TEXT,
    model TEXT,
    serial TEXT,
    fw_version TEXT,
    agent_version TEXT,
    mgmt_ip INET,
    x_m DOUBLE PRECISION,
    y_m DOUBLE PRECISION,
    switch_id TEXT,
    switch_port TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retired_at TIMESTAMPTZ,

    FOREIGN KEY (site_id, floor_id)
        REFERENCES floor(site_id, floor_id)
);


-- 4. AP Radio
CREATE TABLE IF NOT EXISTS radio (
    ap_id TEXT NOT NULL REFERENCES ap(ap_id),
    radio TEXT NOT NULL,

    band TEXT NOT NULL CHECK (
        band IN ('2.4 GHz', '5 GHz', '6 GHz')
    ),

    max_width_mhz INTEGER,

    PRIMARY KEY (ap_id, radio)
);


-- 5. Client / Station
CREATE TABLE IF NOT EXISTS client (
    sta_id TEXT PRIMARY KEY,
    mac MACADDR UNIQUE,
    mac_randomised BOOLEAN DEFAULT FALSE,
    hostname TEXT,
    label TEXT,

    device_type TEXT DEFAULT 'unknown' CHECK (
        device_type IN (
            'robot',
            'camera',
            'laptop',
            'phone',
            'unknown'
        )
    ),

    owner TEXT,
    ip INET,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ DEFAULT NOW()
);


-- 6. SLA Class
CREATE TABLE IF NOT EXISTS sla_class (
    sla_class_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    lat_percentile INTEGER DEFAULT 95,
    lat_bound_ms DOUBLE PRECISION,
    min_thpt_mbps DOUBLE PRECISION,
    max_loss_pct DOUBLE PRECISION,
    max_jitter_ms DOUBLE PRECISION,
    eval_window_s INTEGER DEFAULT 1
        CHECK (eval_window_s > 0)
);


-- 7. Network Flow
CREATE TABLE IF NOT EXISTS flow (
    flow_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES site(site_id),
    name TEXT NOT NULL,
    sta_id TEXT NOT NULL REFERENCES client(sta_id),
    sla_class_id TEXT REFERENCES sla_class(sla_class_id),

    direction TEXT NOT NULL CHECK (
        direction IN ('uplink', 'downlink')
    ),

    src_ip INET,
    src_port INTEGER CHECK (
        src_port BETWEEN 0 AND 65535
    ),
    dst_ip INET,
    dst_port INTEGER CHECK (
        dst_port BETWEEN 0 AND 65535
    ),
    proto TEXT,
    dscp INTEGER CHECK (dscp BETWEEN 0 AND 63),
    classifier TEXT,
    managed BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    created_by TEXT,
    retired_at TIMESTAMPTZ
);


-- Indexes for faster lookups

CREATE INDEX IF NOT EXISTS idx_ap_site
    ON ap(site_id);

CREATE INDEX IF NOT EXISTS idx_floor_site
    ON floor(site_id);

CREATE INDEX IF NOT EXISTS idx_flow_site
    ON flow(site_id);

CREATE INDEX IF NOT EXISTS idx_flow_station
    ON flow(sta_id);

COMMIT;
