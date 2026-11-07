"""Idempotently register the Debezium Postgres connector with Kafka Connect.

PUT /connectors/<name>/config creates the connector or updates it in place, so the
container can run on every `docker compose up` without a 409.
"""
import json
import logging
import os
import sys
import time

import requests

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("connector-init")

CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083").rstrip("/")
NAME = os.getenv("CONNECTOR_NAME", "postgres-connector")
TOPIC_PREFIX = os.getenv("CDC_TOPIC_PREFIX", "banking_server")


def build_config() -> dict:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.environ["POSTGRES_HOST"],
        "database.port": os.getenv("POSTGRES_PORT", "5432"),
        "database.user": os.environ["POSTGRES_USER"],
        "database.password": os.environ["POSTGRES_PASSWORD"],
        "database.dbname": os.environ["POSTGRES_DB"],
        "topic.prefix": TOPIC_PREFIX,
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
        # Emit bare change events (no schema envelope): smaller messages. The consumer
        # accepts both shapes.
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        # keeps the replication slot advancing on quiet tables (prevents WAL build-up)
        "heartbeat.interval.ms": "10000",
    }


def wait_for_connect(timeout: int = 300) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if requests.get(f"{CONNECT_URL}/connectors", timeout=5).ok:
                return
        except requests.RequestException:
            pass
        log.info("Waiting for Kafka Connect at %s ...", CONNECT_URL)
        time.sleep(3)
    raise SystemExit("Kafka Connect did not become ready in time")


def main() -> int:
    wait_for_connect()
    resp = requests.put(
        f"{CONNECT_URL}/connectors/{NAME}/config",
        headers={"Content-Type": "application/json"},
        data=json.dumps(build_config()),
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        log.error("Failed to register connector (%s): %s", resp.status_code, resp.text)
        return 1
    log.info("Connector %s registered (HTTP %s)", NAME, resp.status_code)

    # give the connector a moment, then report its state
    for _ in range(20):
        time.sleep(3)
        status = requests.get(f"{CONNECT_URL}/connectors/{NAME}/status", timeout=10).json()
        state = status.get("connector", {}).get("state")
        tasks = [t.get("state") for t in status.get("tasks", [])]
        log.info("Connector state=%s tasks=%s", state, tasks)
        if state == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks):
            return 0
        if state == "FAILED" or "FAILED" in tasks:
            log.error("Connector failed: %s", json.dumps(status))
            return 1
    log.warning("Connector not RUNNING yet; check %s/connectors/%s/status", CONNECT_URL, NAME)
    return 0


if __name__ == "__main__":
    sys.exit(main())
