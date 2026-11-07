#!/usr/bin/env bash
# Writes the SimpleAuthManager password file from env at start-up (so no secret is baked
# into the image), then execs the API server / UI.
set -euo pipefail
: "${AIRFLOW_ADMIN_USER:?}" "${AIRFLOW_ADMIN_PASSWORD:?}"
python - <<'PY'
import json, os
with open("/tmp/airflow_simple_auth_passwords.json", "w") as f:
    json.dump({os.environ["AIRFLOW_ADMIN_USER"]: os.environ["AIRFLOW_ADMIN_PASSWORD"]}, f)
os.chmod("/tmp/airflow_simple_auth_passwords.json", 0o600)
PY
exec airflow api-server
