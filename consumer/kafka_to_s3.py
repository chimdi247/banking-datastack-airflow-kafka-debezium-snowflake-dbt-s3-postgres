"""Consume Debezium CDC topics and land them as Parquet in an S3-compatible bucket.

Delivery is at-least-once: Kafka offsets are committed only AFTER every buffered
batch has been uploaded. Downstream (dbt staging) de-duplicates.
"""
from __future__ import annotations

import io
import logging
import os
import signal
import sys
import time

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from confluent_kafka import Consumer, KafkaError, KafkaException

from batching import BatchBuffer, object_key, parse_event, table_from_topic

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        log.error("Missing required environment variable %s", name)
        sys.exit(2)
    return value


KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP")
KAFKA_GROUP = env("KAFKA_GROUP", "banking-s3-sink")
TOPIC_PREFIX = env("KAFKA_TOPIC_PREFIX", "banking_server.public")
TABLES = [t.strip() for t in env("CDC_TABLES", "customers,accounts,transactions").split(",") if t.strip()]
S3_ENDPOINT = env("S3_ENDPOINT")
S3_ACCESS_KEY = env("S3_ACCESS_KEY")
S3_SECRET_KEY = env("S3_SECRET_KEY")
BUCKET = env("S3_BUCKET")
BATCH_SIZE = int(env("BATCH_SIZE", "50"))
FLUSH_INTERVAL = float(env("FLUSH_INTERVAL_SECONDS", "10"))

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=os.getenv("S3_REGION", "us-east-1"),
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
)


def ensure_bucket(attempts: int = 60) -> None:
    for attempt in range(1, attempts + 1):
        try:
            s3.head_bucket(Bucket=BUCKET)
            return
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchBucket", "NotFound"}:
                s3.create_bucket(Bucket=BUCKET)
                log.info("Created bucket %s", BUCKET)
                return
            log.warning("Object store not ready (%s), attempt %d/%d", code, attempt, attempts)
        except BotoCoreError as exc:
            log.warning("Object store unreachable (%s), attempt %d/%d", exc, attempt, attempts)
        time.sleep(2)
    log.error("Object store never became ready")
    sys.exit(1)


def write_parquet(table: str, rows: list[dict]) -> None:
    columns = sorted({k for r in rows for k in r})
    arrow_table = pa.table({c: [r.get(c) for r in rows] for c in columns})
    buf = io.BytesIO()
    pq.write_table(arrow_table, buf, compression="snappy")
    key = object_key(table)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    log.info("Uploaded %d records to s3://%s/%s", len(rows), BUCKET, key)


def main() -> int:
    ensure_bucket()

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit only after a successful upload
            "session.timeout.ms": 30000,
        }
    )
    topics = [f"{TOPIC_PREFIX}.{t}" for t in TABLES]
    consumer.subscribe(topics)
    log.info("Subscribed to %s", ", ".join(topics))

    buffer = BatchBuffer(BATCH_SIZE, FLUSH_INTERVAL)
    running = True

    def stop(signum, _frame):
        nonlocal running
        log.info("Signal %s received, flushing and shutting down", signum)
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def flush() -> None:
        batches = buffer.drain()
        if not batches:
            return
        for table, rows in batches.items():
            write_parquet(table, rows)
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError._NO_OFFSET:
                raise

    try:
        while running:
            msg = consumer.poll(1.0)
            now = time.monotonic()
            if msg is None:
                if buffer.should_flush(now):
                    flush()
                continue
            if msg.error():
                if msg.error().code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._PARTITION_EOF):
                    continue  # topic not created yet (before the first CDC snapshot)
                raise KafkaException(msg.error())

            row = parse_event(msg.value())
            if row is not None:
                buffer.add(table_from_topic(msg.topic()), row, now)
            if buffer.should_flush(now):
                flush()
        flush()
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
