"""Shared connection helpers for DAGs and the Snowflake bootstrap job.

Everything is configured from environment variables (see .env.example).
Snowflake auth: key-pair when SNOWFLAKE_PRIVATE_KEY_PATH is set (recommended:
Snowflake is phasing out single-factor password logins), otherwise password.
"""
from __future__ import annotations

import os

TABLES = ["customers", "accounts", "transactions"]


def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=os.getenv("S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def snowflake_connect(schema: str | None = None):
    import snowflake.connector

    params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DB"],
        "schema": schema or os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    }
    if os.getenv("SNOWFLAKE_ROLE"):
        params["role"] = os.environ["SNOWFLAKE_ROLE"]

    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        params["private_key_file"] = key_path
        if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
            params["private_key_file_pwd"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]
    else:
        params["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return snowflake.connector.connect(**params)
