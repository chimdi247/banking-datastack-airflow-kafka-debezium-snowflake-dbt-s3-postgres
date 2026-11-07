"""Synthetic banking data generator (customers -> accounts -> transactions).

All knobs are environment variables so the container needs no code changes:
  POSTGRES_HOST/PORT/DB/USER/PASSWORD   connection
  GEN_NUM_CUSTOMERS (10)  GEN_ACCOUNTS_PER_CUSTOMER (2)  GEN_NUM_TRANSACTIONS (50)
  GEN_MAX_TXN_AMOUNT (1000)  GEN_SLEEP_SECONDS (2)  GEN_CURRENCY (USD)
Use --once to run a single iteration and exit.
"""
import argparse
import logging
import os
import random
import signal
import sys
import time
import uuid
from decimal import ROUND_DOWN, Decimal

import psycopg2
from faker import Faker

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("generator")

NUM_CUSTOMERS = int(os.getenv("GEN_NUM_CUSTOMERS", "10"))
ACCOUNTS_PER_CUSTOMER = int(os.getenv("GEN_ACCOUNTS_PER_CUSTOMER", "2"))
NUM_TRANSACTIONS = int(os.getenv("GEN_NUM_TRANSACTIONS", "50"))
MAX_TXN_AMOUNT = float(os.getenv("GEN_MAX_TXN_AMOUNT", "1000.00"))
CURRENCY = os.getenv("GEN_CURRENCY", "USD")
SLEEP_SECONDS = float(os.getenv("GEN_SLEEP_SECONDS", "2"))

INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")

fake = Faker()
_stop = False


def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def unique_email(first: str, last: str) -> str:
    # A UUID suffix keeps emails unique forever (faker.unique eventually exhausts).
    return f"{first}.{last}.{uuid.uuid4().hex[:10]}@example.com".lower()


def connect(attempts: int = 60):
    for attempt in range(1, attempts + 1):
        try:
            conn = psycopg2.connect(
                host=os.environ["POSTGRES_HOST"],
                port=os.getenv("POSTGRES_PORT", "5432"),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("Postgres not ready (%s) attempt %d/%d", str(exc).strip(), attempt, attempts)
            time.sleep(2)
    raise SystemExit("Postgres never became ready")


def run_iteration(cur) -> None:
    customers = []
    for _ in range(NUM_CUSTOMERS):
        first_name, last_name = fake.first_name(), fake.last_name()
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
            (first_name, last_name, unique_email(first_name, last_name)),
        )
        customers.append(cur.fetchone()[0])

    accounts = []
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            cur.execute(
                "INSERT INTO accounts (customer_id, account_type, balance, currency) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (
                    customer_id,
                    random.choice(["SAVINGS", "CHECKING"]),
                    random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX),
                    CURRENCY,
                ),
            )
            accounts.append(cur.fetchone()[0])

    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)
        txn_type = random.choice(txn_types)
        amount = round(random.uniform(1, MAX_TXN_AMOUNT), 2)
        related = None
        if txn_type == "TRANSFER" and len(accounts) > 1:
            related = random.choice([a for a in accounts if a != account_id])
        cur.execute(
            "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) "
            "VALUES (%s, %s, %s, %s, 'COMPLETED')",
            (account_id, txn_type, amount, related),
        )

    log.info("Generated %d customers, %d accounts, %d transactions", len(customers), len(accounts), NUM_TRANSACTIONS)


def main() -> int:
    global _stop
    parser = argparse.ArgumentParser(description="Run fake data generator")
    parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
    args = parser.parse_args()

    def handle_stop(*_):
        global _stop
        _stop = True

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    conn = connect()
    cur = conn.cursor()
    try:
        iteration = 0
        while not _stop:
            iteration += 1
            log.info("Iteration %d started", iteration)
            run_iteration(cur)
            if args.once:
                break
            # sleep in small slices so SIGTERM is honoured promptly
            deadline = time.monotonic() + SLEEP_SECONDS
            while not _stop and time.monotonic() < deadline:
                time.sleep(0.2)
    finally:
        cur.close()
        conn.close()
    log.info("Generator stopped")
    return 0  # unhandled errors propagate as a non-zero exit so Docker restarts us


if __name__ == "__main__":
    sys.exit(main())
