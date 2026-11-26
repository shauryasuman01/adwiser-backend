import os
from pathlib import Path
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]

load_dotenv(BASE_DIR / ".env")


def required(name):
    value = os.getenv(name)

    if not value or not value.strip():
        raise ValueError(
            f"Missing environment variable: {name}"
        )

    return value


def get_port(name, default):
    value = int(os.getenv(name, default))

    if not 1 <= value <= 65535:
        raise ValueError(f"Invalid port: {name}")

    return value


# Application configuration

HOST = os.getenv("HOST", "127.0.0.1")
PORT = get_port("PORT", 3001)


# PostgreSQL configuration

PG_HOST = required("PG_HOST")
PG_PORT = get_port("PG_PORT", 5433)
PG_DATABASE = required("PG_DATABASE")
PG_USER = required("PG_USER")
PG_PASSWORD = required("PG_PASSWORD")


# InfluxDB configuration

INFLUX_URL = required("INFLUX_URL")
INFLUX_DATABASE = required("INFLUX_DATABASE")
INFLUX_TOKEN = required("INFLUX_TOKEN")
