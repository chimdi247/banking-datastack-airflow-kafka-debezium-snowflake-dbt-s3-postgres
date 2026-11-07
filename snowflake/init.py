"""Create the warehouse / database / schemas / RAW tables if they do not exist."""
import os
import re
import sys
from pathlib import Path

from banking_common import snowflake_connect

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def ident(name: str) -> str:
    value = os.environ[name]
    if not IDENT.match(value):
        raise SystemExit(f"{name} must be a plain identifier (letters, digits, underscore), got {value!r}")
    return value


def main() -> int:
    sql = (Path(__file__).parent / "setup.sql").read_text().format(
        warehouse=ident("SNOWFLAKE_WAREHOUSE"), database=ident("SNOWFLAKE_DB")
    )
    conn = snowflake_connect(schema="PUBLIC")
    try:
        cur = conn.cursor()
        for stmt in (s.strip() for s in sql.split(";")):
            if stmt:
                print("->", stmt.splitlines()[0][:90])
                cur.execute(stmt)
    finally:
        conn.close()
    print("Snowflake bootstrap complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
