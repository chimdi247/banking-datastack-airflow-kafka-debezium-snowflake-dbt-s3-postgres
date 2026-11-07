"""Land Parquet files from the object store into Snowflake RAW tables.

download -> load (PUT + COPY INTO, table stage) -> archive (move to processed/).
Objects are only archived after Snowflake confirms the COPY, so a failed run is
simply retried; COPY's load metadata prevents double-loading.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from banking_common import TABLES, s3_client, snowflake_connect

BUCKET = os.getenv("S3_BUCKET", "banking-cdc")
SCHEDULE = os.getenv("INGEST_SCHEDULE", "*/5 * * * *")
PROCESSED_PREFIX = "processed/"


@dag(
    dag_id="s3_to_snowflake_banking",
    description="Load CDC Parquet from the object store into Snowflake RAW tables",
    schedule=SCHEDULE,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["banking", "ingest"],
)
def s3_to_snowflake_banking():
    @task
    def download() -> dict:
        s3 = s3_client()
        workdir = tempfile.mkdtemp(prefix="cdc-")
        manifest: dict = {"workdir": workdir, "files": {t: [] for t in TABLES}}
        paginator = s3.get_paginator("list_objects_v2")
        for table in TABLES:
            for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{table}/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith(".parquet"):
                        continue
                    local = os.path.join(workdir, key.replace("/", "__"))
                    s3.download_file(BUCKET, key, local)
                    manifest["files"][table].append({"key": key, "path": local})
        total = sum(len(v) for v in manifest["files"].values())
        print(f"Downloaded {total} files")
        return manifest

    @task
    def load(manifest: dict) -> dict:
        if not any(manifest["files"].values()):
            print("Nothing new to load.")
            return manifest
        conn = snowflake_connect()
        try:
            cur = conn.cursor()
            for table in TABLES:  # TABLES is a fixed allow-list, safe to interpolate
                files = manifest["files"].get(table, [])
                if not files:
                    continue
                for f in files:
                    cur.execute(f"PUT 'file://{f['path']}' @%{table} AUTO_COMPRESS=FALSE")
                cur.execute(
                    f"COPY INTO {table} FROM @%{table} "
                    "FILE_FORMAT=(TYPE=PARQUET) ON_ERROR='ABORT_STATEMENT' PURGE=TRUE"
                )
                print(f"Loaded {len(files)} files into {table}")
        finally:
            conn.close()
        return manifest

    @task
    def archive(manifest: dict) -> None:
        s3 = s3_client()
        for files in manifest["files"].values():
            for f in files:
                s3.copy_object(
                    Bucket=BUCKET,
                    Key=PROCESSED_PREFIX + f["key"],
                    CopySource={"Bucket": BUCKET, "Key": f["key"]},
                )
                s3.delete_object(Bucket=BUCKET, Key=f["key"])
        shutil.rmtree(manifest["workdir"], ignore_errors=True)

    archive(load(download()))


s3_to_snowflake_banking()
