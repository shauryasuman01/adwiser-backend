# Architecture

ADWISER is four independently deployable processes. The Python generator owns synthetic network state and emits version-1 ZeroMQ envelopes. The ingestion service validates complete envelopes, converts points to Influx line protocol and writes them to InfluxDB 3 Core. FastAPI reads raw telemetry, synchronizes durable relational metadata to PostgreSQL, and presents Monitor HTTP and Signal HTTP/SSE contracts. The existing Vite dashboard consumes only the API.

No Kafka, Redis, durable transport spool, application queue, roll-up worker or retention manager is present. ZeroMQ PUB/SUB is deliberately bounded and loss-tolerant. InfluxDB owns time-series telemetry; PostgreSQL owns configuration and lifecycle state. These boundaries prevent the high-rate path from turning PostgreSQL into a telemetry store.

## Processing and time semantics

The generator advances one coherent 20 ms simulation window using monotonic deadlines, then stamps data with scheduled UTC epoch milliseconds. Modules publish independently over four sockets. Ingestion preserves sample timestamps and separately observes receive lag, sequence gaps and HTTP acknowledgement latency. Clock synchronization is required before cross-device lag has meaning.

The API computes rates and percentages from aggregate counters where appropriate; it does not average already-derived loss percentages across time. Missing observations remain `null`. `live` is a five-minute raw window. Longer requests are bucketed at query time, so database cost rises with raw history.

## Relational workers

Inventory sync reads inventory measurements every 60 seconds and upserts sites, floors, APs, radios, clients, flows and generated SLA classes. Alert evaluation runs every ten seconds with PostgreSQL advisory locking and a uniqueness constraint for open rule/entity conditions. Schedule sync validates sampled JSON metadata against both its internal slot geometry and current inventory before insertion.

## Security and isolation

Monitor uses HttpOnly sessions, role permissions and explicit site scoping. Passwords are Argon2 hashes and session tokens are stored only as hashes. Signal is read-only but unauthenticated. CORS is disabled unless origins are explicitly configured. ZeroMQ is plaintext and unauthenticated, so generator ports belong on a trusted or firewalled network.

## Floor plan

PostgreSQL floor width/height and AP x/y values are physical metres. Changing them to shrink a view would corrupt scale and marker placement. The current dashboard maps those values into an SVG whose CSS uses the available width. There is no supported backend configuration for a 650-by-300-pixel cap while preserving dimensions. Because the dashboard is protected as read-only, visual sizing remains unchanged; all floor dimensions, rooms, markers and scale are accurate.

## Deferred work

Automatic retention, multi-resolution roll-ups, durable transport/replay, authenticated Signal routes, real AP/controller integration, multi-radio dashboard rows, runtime topology reload and physical-model validation are intentionally deferred.
