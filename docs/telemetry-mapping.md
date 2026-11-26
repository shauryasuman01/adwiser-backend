# Telemetry mapping and unavailable data

The generator contract is `/home/shaurya/Downloads/adwiser-generator/schema.py`; ingestion writes its measurement names and field types to InfluxDB 3. Tags are strings. Timestamp precision is milliseconds. Numeric percentages are 0–100.

| Measurement | API use | Current limitation |
|---|---|---|
| `flow_kpi` | Flow ledger and detail, SLA state, packet loss | Per-snapshot p50/p95/p99; historical charts use latest sample in a bucket, not an aggregate percentile. |
| `flow_lat_hist` | Flow detail CDF | Finite histogram bounds; values beyond the final bound leave CDF below 100%, and p95 is null if unobserved. |
| `ctrl_queue`, `sched_flow` | Scheduler queues and 20 ms aggregate slot counts | Allocated and used slots are distinct; these measurements do not carry positions. |
| `schedule_snapshot` | Exact sampled scheduler geometry, version and per-radio allocation JSON | Sampled at most every 10 seconds plus recovery, so not every intermediate version is retained. The dashboard shows one labelled representative radio per AP. |
| `sched_stats`, `ack_fb`, `ctrl_host`, `ctrl_ingestion` | Scheduler KPIs and host | Generator host data and ingestion-owned message/lag/write health remain separate. |
| `ap_radio`, `ap_agent`, `sta_link`, `ap_ingestion` | Existing AP/client views, alert rules | `ap_agent` is device resource data. Ingestion derives `ap_ingestion` only from validated AP envelopes, sequence observations, freshness, and acknowledged storage outcomes. |
| `site_kpi` | Existing overview | Synthetic generator KPI, not independently measured. |
| `client_performance` | Signal client cards and graphs | `latency_ms` optional. |
| `ap_health` | Signal AP queue | No SSID tag; an SSID filter cannot apply to queue data. |
| `application_experience` | Signal application byte totals | Byte increments are summed; no rate conversion. |
| `site_inventory`, `ap_inventory`, `radio_inventory`, `sta_inventory`, `flow_inventory` | PostgreSQL inventory sync | Flow inventory has SLA terms, but no five-tuple. Actual schedule allocations are carried separately. |
| `switch_port` | Existing Monitor views | Synthetic switch measurements. |

`alert_rule` seeds only six rules supported by emitted metrics. A defined association-start event source is still required for exact connection duration. Historical rollups and retention are not implemented; responses identify the actual source as raw.
