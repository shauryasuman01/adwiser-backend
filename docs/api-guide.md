# Frontend integration notes

Use `/api/v1/adwiser/session` to sign in with `{ "email": "...", "password": "..." }`. Send Monitor requests with `credentials: 'include'`; the session is a cookie. Obtain `meta.sites`, `meta.permissions`, and `meta.refreshS` from each view. Poll only the `live` range, at the returned refresh interval. Custom ranges take epoch millisecond `from` and `to` and cannot end in the future. Display `null` as unavailable, not zero.

Every Monitor GET except `/session` has `{ "meta": {...}, "data": {...} }`. Writes require JSON and return the updated drawer or event row in the same envelope. Handle 400 invalid scope, 401 expired session, 403 missing permission, 404 unknown scoped entity, 409 invalid alert transition, and 415 wrong content type. `/scheduler` and extra engineering fields are permission-dependent.

Signal `/api/v1/filters` returns `{ "sites": [], "ssids": [] }`. The performance snapshot returns `{ "data": null, "meta": {...} }` when filters have no data. The stream emits `config` once, then `snapshot` every 10 seconds. An `unavailable` event means retry the data source; a `snapshot` with `meta.stale: true` means recent cached data were served. Signal is currently unauthenticated; its contract does not prescribe the Monitor session.

The OpenAPI document is `/openapi.json`. Runtime measurements and full example
responses require a healthy InfluxDB connection. If a shell exports an obsolete
`INFLUX_TOKEN`, unset it before startup so the repository `.env` value is used.

Scheduler `version` and `gantt` always come from the same PostgreSQL snapshot.
`gantt.slotUs` and `totalSlots` describe its geometry. AP row names ending in
`representative` intentionally expose the immutable one-row-per-AP limitation;
do not interpret the view as a serialization of all parallel radios.
