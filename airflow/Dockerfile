# syntax=docker/dockerfile:1
# Build context: repository root.
FROM apache/airflow:3.3.2

USER airflow

# dbt lives in its own virtualenv (no dependency conflicts with Airflow)
COPY airflow/requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# libraries used by DAG code (Snowflake connector); Airflow's own env
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:root airflow/lib /opt/airflow/lib
COPY --chown=airflow:root airflow/dbt_profiles /opt/airflow/dbt_profiles
COPY --chown=airflow:root snowflake /opt/airflow/snowflake
COPY --chown=airflow:root banking_dbt /opt/airflow/banking_dbt
COPY --chown=airflow:root airflow/start-api-server.sh /opt/airflow/start-api-server.sh

ENV PYTHONPATH=/opt/airflow/lib \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
