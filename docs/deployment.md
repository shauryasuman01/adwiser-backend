# Distributed deployment

All services can run on one host or separate devices. `127.0.0.1` reaches only the current device. `0.0.0.0` is a server bind wildcard and is never a client destination. A client must use the server device's stable LAN/DNS address.

## Generator device

Set `transport.bind_host` to a specific trusted interface address or `0.0.0.0`, retain ports 5551–5554, and configure topology, scenario and the required 20 ms interval in `config.yaml`. Permit inbound TCP only from the ingestion device. Start:

```bash
cd /home/shaurya/Downloads/adwiser-generator
.venv/bin/python main.py --scenario normal
```

## Database and ingestion device

Run InfluxDB 3 Core on its intended data volume. Set `generator.host` or `GENERATOR_HOST` to the generator's actual address, set the four ports, and configure `database.url`, database name and token environment variable. Set write concurrency from measured Influx capacity, not by guesswork.

```bash
cd /home/shaurya/Downloads/adwiser-ingestion
export INFLUXDB3_AUTH_TOKEN='read-from-secret-store'
.venv/bin/python main.py
```

PostgreSQL can share this device or use another trusted host. Do not expose either database publicly. Back up PostgreSQL and the Influx data directory independently.

## Backend and dashboard devices

Point backend `PG_HOST`/`PG_PORT` and `INFLUX_URL` at their real servers. Bind Uvicorn to `0.0.0.0` only when remote access is required, restrict the port, set exact `CORS_ORIGINS`, and terminate HTTPS at a reverse proxy for production cookies.

```bash
cd /home/shaurya/Downloads/adwiser-backend
env -u INFLUX_TOKEN .venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 3001

cd /home/shaurya/dashboard
npm install
npm run dev -- --host 0.0.0.0
```

The dashboard's existing proxy must address the reachable backend. This repository does not alter dashboard configuration.

## Operations checklist

- Synchronize all hosts with NTP/chrony; otherwise publisher-to-ingestion lag is invalid.
- Check `nc -vz GENERATOR_IP 5551` through 5554 from ingestion, database ports from backend, and `curl BACKEND_IP:3001/health` from the dashboard network.
- Start databases, ingestion, generator, backend, then dashboard. Starting ingestion before the publisher reduces PUB/SUB startup loss; periodic inventory and schedule recovery repair metadata visibility, not missing raw samples.
- Restrict firewall sources, keep tokens/passwords in secret storage, rotate credentials and use TLS across untrusted links.
- Use SIGINT/SIGTERM for graceful shutdown. Stop the generator first when draining the high-rate path, then ingestion, backend, and databases.
