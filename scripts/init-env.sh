#!/usr/bin/env bash
# Creates .env from .env.example, replacing every __TOKEN__ with a fresh random secret.
# Refuses to overwrite an existing .env.
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] && { echo ".env already exists; refusing to overwrite it." >&2; exit 1; }
command -v openssl >/dev/null || { echo "openssl is required" >&2; exit 1; }

alnum()  { openssl rand -base64 96 | tr -dc 'A-Za-z0-9' | head -c "$1"; }
upper()  { openssl rand -base64 96 | tr -dc 'A-Z0-9' | head -c "$1"; }
hex()    { openssl rand -hex 32; }
fernet() { openssl rand -base64 32 | tr '+/' '-_'; }            # url-safe base64 of 32 bytes
kafka_id() {                                                    # 22-char url-safe base64 UUID
  local id
  while :; do
    id="$(openssl rand -base64 16 | tr '+/' '-_' | tr -d '=')"
    [[ "$id" != -* ]] && { echo "$id"; return; }
  done
}

umask 077
: > .env
while IFS= read -r line || [[ -n "$line" ]]; do
  while [[ "$line" == *__PASSWORD__* ]];         do line="${line/__PASSWORD__/$(alnum 32)}"; done
  while [[ "$line" == *__UPPER_KEY__* ]];        do line="${line/__UPPER_KEY__/$(upper 20)}"; done
  while [[ "$line" == *__HEX__* ]];              do line="${line/__HEX__/$(hex)}"; done
  while [[ "$line" == *__FERNET__* ]];           do line="${line/__FERNET__/$(fernet)}"; done
  while [[ "$line" == *__KAFKA_CLUSTER_ID__* ]]; do line="${line/__KAFKA_CLUSTER_ID__/$(kafka_id)}"; done
  printf '%s\n' "$line" >> .env
done < .env.example

echo "Created .env (mode 600)."
echo "Next: fill in the SNOWFLAKE_* values, then run: docker compose up -d --build"
echo "Airflow login: user=$(grep '^AIRFLOW_ADMIN_USER=' .env | cut -d= -f2)  password in .env (AIRFLOW_ADMIN_PASSWORD)"
