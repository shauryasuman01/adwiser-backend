# Scheduler metadata

The simulator divides each configured sampling superframe into `superframe_slots` equal slots. With the current 20 ms interval and 20 slots, the verified geometry is 20,000 microseconds per superframe and 1,000 microseconds per slot. These values are derived from configuration and rejected if the division is not exact.

Each radio schedules independently. Eligible backlogged directional flows receive a rotating fair share, capped by demand and PHY/packet capacity; unused capacity is redistributed. Queue service remains FIFO by packet cohort. Parallel radios may reuse the same slot indices, while allocations on one `(AP, radio)` cannot overlap.

During actual assignment the generator records AP, radio, flow, direction, SLA class, start slot, allocated slots, used slots, start microseconds and duration. The schedule signature includes geometry and the ordered `(AP, radio, flow, start, length)` allocation sequence. A change in order, radio or position therefore creates a new run-local version. A random `run_id` distinguishes restarts, and PostgreSQL keys versions by `(site_id, run_id, schedule_ver)`.

Aggregate `sched_flow` and `sched_stats` telemetry remains at 20 ms. Detailed `schedule_snapshot` points are large, so they are emitted at startup/change subject to `min_interval_s`, plus `recovery_interval_s` recovery. Current values are 10 and 60 seconds. Sampling means an intermediate run-local version may never be stored; the API never claims otherwise.

The snapshot includes geometry, counts, allocation/used percentage, trigger and compact JSON allocations. Ingestion applies the same schema as every other point. The backend validates exact shape, bounds, `start_us`/`len_us`, per-radio non-overlap and inventory references before writing JSONB. Startup scans a bounded recovery window; later polls use a timestamp cursor.

Observed payloads averaged 15,723 bytes, ranging from 13,229 to 17,465 bytes. At one snapshot per ten seconds, payload alone projects to about 135.8 MB/day (decimal) or 129.5 MiB/day, duplicated across Influx and PostgreSQL before indexes, row format, compression and filesystem overhead. This is a planning estimate, not measured disk growth. No metadata retention is automatic.

The dashboard contract has one Gantt row per AP but the simulator has up to two radios per AP. The API selects the radio with the most allocated slots, labels it `representative`, and returns only that radio to the Gantt. It does not concatenate parallel transmissions. `representativeRadios`, `slotUs`, `totalSlots` and the full selected version identity remain available in the response; PostgreSQL retains all radio allocations.

Contention is `null` because this synthetic scheduler does not model a separate contention period. It must not be invented from unallocated airtime.
