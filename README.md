# 🏦 Banking Modern Data Stack

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![DBT](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-EF3B2D?logo=apache&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?logo=git&logoColor=white)
![CI/CD](https://img.shields.io/badge/CI%2FCD-000000?logo=githubactions&logoColor=white)

---

## 📌 Project Overview
This project demonstrates an **end-to-end modern data stack pipeline** for a **Banking domain**.  
We simulate **customer, account, and transaction data**, stream changes in real time, transform them into analytics-ready models, and visualize insights — following **best practices of CI/CD and data warehousing**.

👉 Think of it as a **real-world banking data ecosystem** built on modern data tools.  

---

## 🏗️ Architecture

<img width="5647" height="3107" alt="Architecture" src="https://github.com/user-attachments/assets/7521ea8a-451e-46ff-9db0-71dd6ddf8181" />


**Pipeline flow**

1. **Data generator** (Faker) writes customers, accounts and transactions into **PostgreSQL**.
2. **Debezium** (Kafka Connect) captures the Postgres WAL and publishes change events to **Kafka** (KRaft, no ZooKeeper).
3. A **consumer** batches the events into Parquet files in an **S3-compatible object store** (RustFS).
4. **Airflow** loads the Parquet files into **Snowflake** `RAW` tables and runs the dbt DAG.
5. **dbt** builds staging views, SCD Type-2 snapshots, dimensions and facts in `ANALYTICS`.
6. **GitHub Actions** lints, unit-tests, builds the images and deploys dbt.

---

## 🚀 Run it on an Ubuntu cloud VM (one compose file)

**Sizing:** Ubuntu 22.04/24.04, **4 vCPU / 8 GB RAM / 40 GB disk** minimum.

```bash
# 1. once per VM: Docker Engine + Compose v2, 4 GB swap, firewall (SSH only)
bash scripts/setup-ubuntu.sh          # then log out and back in

# 2. generate .env with random secrets (Postgres, object store, Airflow, Kafka cluster id)
scripts/init-env.sh
nano .env                             # fill in the SNOWFLAKE_* values (see below)

# 3. preflight (optional, 1 min): fails fast if any pinned image tag does not exist
docker compose pull --ignore-buildable

# 4. start everything
docker compose up -d --build
docker compose ps
```

What starts, in dependency order: `postgres` (schema loaded automatically) → `kafka` → `connect` →
`connector-init` (registers the Debezium connector, idempotent) → `consumer` → `data-generator`;
alongside `rustfs`, `airflow-postgres` → `airflow-init` (DB migration) → `airflow-apiserver`,
`airflow-scheduler`, `airflow-dag-processor`; and the one-shot `snowflake-init` (creates warehouse /
database / schemas / RAW tables).

### Reaching the web UIs
Every port is bound to `127.0.0.1` (Docker-published ports bypass UFW, so exposing them is risky).
Use an SSH tunnel from your laptop:

```bash
ssh -L 8080:localhost:8080 -L 9001:localhost:9001 -L 8083:localhost:8083 user@<vm-ip>
```

| UI | URL | Login |
|----|-----|-------|
| Airflow | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| RustFS console | http://localhost:9001 | `S3_ACCESS_KEY` / `S3_SECRET_KEY` |
| Kafka Connect REST | http://localhost:8083/connectors | none |

To serve Airflow publicly, put a TLS reverse proxy (Caddy / nginx) in front of it rather than setting `BIND_ADDRESS=0.0.0.0`.

### Snowflake (the only part that cannot run in Docker)
Fill in `.env`: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER` and **one** auth method. Snowflake is enforcing
MFA and blocking single-factor password sign-ins, so **key-pair auth is recommended**:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out secrets/rsa_key.p8 -nocrypt
openssl rsa -in secrets/rsa_key.p8 -pubout -out secrets/rsa_key.pub
sudo chgrp 0 secrets/rsa_key.p8 && chmod 640 secrets/rsa_key.p8     # Airflow runs as uid 50000 / gid 0
# in Snowflake:  ALTER USER <user> SET RSA_PUBLIC_KEY='<contents of rsa_key.pub without the header/footer>';
# in .env:       SNOWFLAKE_PRIVATE_KEY_PATH=/run/secrets/snowflake/rsa_key.p8
```

The role in `SNOWFLAKE_ROLE` needs `CREATE WAREHOUSE`/`CREATE DATABASE` for the one-time bootstrap
(`docker compose up snowflake-init`; safe to re-run). Until Snowflake is configured the rest of the
pipeline still runs: Postgres → Kafka → Parquet in the object store.

### Day-2 operations
```bash
docker compose logs -f consumer connector-init        # CDC health
docker compose exec airflow-scheduler airflow dags list
docker compose down                                   # stop, keep data
docker compose down -v                                # stop and DELETE all volumes
docker compose up -d --build                          # redeploy after code changes
```
Cost / growth knobs (all in `.env`): `GEN_SLEEP_SECONDS` (the generator inserts forever),
`INGEST_SCHEDULE` (each run that finds files wakes the Snowflake warehouse),
`KAFKA_RETENTION_HOURS`.

---

## ⚡ Tech stack and versions

| Component | Before | Now | Notes |
|-----------|--------|-----|-------|
| Apache Airflow | 2.9.3 | **3.3.2** | 2.x reached EOL on 22 Apr 2026. Airflow has no LTS; 3.3 is the only maintained line. DAGs ported to the Airflow 3 SDK |
| Apache Kafka | Confluent cp-kafka 7.4.1 + ZooKeeper | **apache/kafka 4.3.1 (KRaft)** | ZooKeeper removed. 4.3.1 is the broker version Debezium 3.7 is tested against |
| Debezium Connect | `debezium/connect:2.2` (Docker Hub) | **quay.io/debezium/connect:3.7.1.Final** | Docker Hub images are no longer published |
| PostgreSQL (source) | 15 | **18.6** | supported until Nov 2030 |
| PostgreSQL (Airflow metadata) | 15 | **17.11** | supported until Nov 2029 |
| Object store | `minio/minio:latest` | **rustfs/rustfs:1.0.0** | MinIO stopped publishing community images (Oct 2025), repo archived (Apr 2026) |
| dbt | unpinned `dbt-core`/`dbt-snowflake` | **dbt-core / dbt-snowflake 1.12.x** (own venv) | 1.12 supported to Jul 2027 |
| Python services | unpinned, `python:` n/a | **python:3.13-slim**, non-root | |
| Kafka client | `kafka-python` | **confluent-kafka** | manual commits, at-least-once |
| Parquet | `fastparquet` + `pandas` | **pyarrow** | one dependency instead of two |
| Snowflake connector | unpinned | **snowflake-connector-python 4.x** | key-pair auth supported |
| GitHub Actions | checkout@v4, setup-python@v4, Python 3.11 | checkout@v5, setup-python@v6, Python 3.13 | |

Python dependencies use compatible-release ranges (e.g. `>=4,<5`), so a rebuild picks up patch/minor
fixes within the major version. For fully reproducible builds, generate lock files with `pip-compile`.

---

## 🛠️ What changed in the pipeline (bugs found while containerising)

- **Postgres schema was never applied**: `postgres/schema.sql` is now mounted into `docker-entrypoint-initdb.d`.
- **DAG task order**: `dbt_run_marts` was never wired into the DAG, and snapshots/marts ran before the staging views existed. Order is now `staging → snapshot → marts → test`.
- **Airflow 3 API changes**: `schedule_interval`, `provide_context` and the old operator import paths are gone; DAGs use `airflow.sdk` and the standard provider.
- **Data loss on failure**: the consumer auto-committed offsets and only flushed full 50-row batches. It now commits after the upload and flushes on a timer.
- **Ingest re-downloaded everything every minute**: files are now moved to `processed/` after a confirmed `COPY`, and `ON_ERROR` is `ABORT_STATEMENT` (the old `CONTINUE` silently dropped bad rows).
- **Duplicate-key MERGE failures**: staging models de-duplicate on the business key and order by the Debezium event time (`_ts_ms`), so row updates replace older versions.
- **Generator**: `faker.unique.email()` eventually raises on a long-running loop, and `finally: sys.exit(0)` swallowed crashes. Both fixed.
- **Secrets**: per-service `.env` files and hardcoded defaults are replaced by one generated `.env`; `ACCOUNTADMIN` is no longer used by CI.

---

## 🧪 Tests

```bash
pip install -r requirements.txt pyyaml
pytest tests/ -q        # batching logic + static checks on docker-compose.yml
```

---

## 📂 Repository structure
```text
├── docker-compose.yml          # the whole stack
├── .env.example                # all configuration (scripts/init-env.sh fills in secrets)
├── scripts/                    # setup-ubuntu.sh, init-env.sh
├── airflow/                    # Dockerfile (Airflow 3 + dbt venv), shared lib, dbt profile, requirements
├── docker/dags/                # DAGs: s3_to_snowflake_banking, SCD2_snapshots
├── banking_dbt/                # dbt project (staging, snapshots, marts, tests)
├── snowflake/                  # idempotent warehouse/database/RAW-table bootstrap
├── consumer/                   # Kafka -> Parquet -> object store
├── data-generator/             # Faker-based simulator
├── kafka-debezium/             # idempotent connector registration
├── postgres/schema.sql         # OLTP DDL (loaded on first start)
├── secrets/                    # Snowflake private key (git-ignored)
├── tests/                      # unit tests
└── .github/workflows/          # ci.yml, cd.yml
```

---

**Author**: *Jaya Chandra Kadiveti*  
**LinkedIn**: [jayachandrakadiveti](https://www.linkedin.com/in/jayachandrakadiveti/)  
**Contact**: [datawithjay1@gmail.com](mailto:datawithjay1@gmail.com)  


## 1. Generate the key pair (on the VM, in the project folder)

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out secrets/rsa_key.p8 -nocrypt
openssl rsa -in secrets/rsa_key.p8 -pubout -out secrets/rsa_key.pub
sudo chgrp 0 secrets/rsa_key.p8
chmod 640 secrets/rsa_key.p8
cat secrets/rsa_key.pub

SNOWFLAKE_PRIVATE_KEY= cat secrets/rsa_key.p8 (github actions repo secrets)
```


## 2. Create the role and user in Snowflake

```bash
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS BANKING_ROLE;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE BANKING_ROLE;
GRANT CREATE DATABASE  ON ACCOUNT TO ROLE BANKING_ROLE;

CREATE USER IF NOT EXISTS BANKING_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = BANKING_ROLE
  RSA_PUBLIC_KEY = '<paste the key rsa_key.pub here>';

GRANT ROLE BANKING_ROLE TO USER BANKING_SVC;
```
