Place the Snowflake private key (PKCS#8 `.p8`) here for key-pair auth, e.g. `rsa_key.p8`.
The containers read it as `/run/secrets/snowflake/rsa_key.p8` (set SNOWFLAKE_PRIVATE_KEY_PATH accordingly).
The Airflow user is uid 50000 / gid 0, so: `sudo chgrp 0 secrets/rsa_key.p8 && chmod 640 secrets/rsa_key.p8`.
Nothing in this folder is committed (see .gitignore).
