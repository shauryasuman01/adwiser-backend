BEGIN;
INSERT INTO alert_rule(rule_id,name,entity_type,metric,operator,threshold,duration_s,severity) VALUES
 ('r-ap-util','High channel utilisation','ap','ap_radio.util_busy_pct','>',60,60,'warning'),
 ('r-ap-retry','High AP retry rate','ap','ap_radio.tx_retry_pct','>',10,60,'warning'),
 ('r-flow-violation','Flow SLA violation','flow','flow_kpi.sla_state','=',2,30,'warning'),
 ('r-ack-lat','High ACK latency','ap','ack_fb.lat_p95_ms','>',5,30,'info'),
 ('r-client-rssi','Weak client signal','client','sta_link.rssi_dbm','<',-70,60,'info'),
 ('r-client-retry','High client retry rate','client','sta_link.tx_retry_pct','>',10,60,'info')
ON CONFLICT(rule_id) DO NOTHING;
CREATE UNIQUE INDEX IF NOT EXISTS alert_active_condition_idx
 ON alert(site_id,rule_id,entity_type,entity_id)
 WHERE rule_id IS NOT NULL AND ended_at IS NULL AND status <> 'resolved';
COMMIT;
