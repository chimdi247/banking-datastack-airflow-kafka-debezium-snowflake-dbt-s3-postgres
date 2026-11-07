import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "consumer"))
from batching import BatchBuffer, object_key, parse_event, table_from_topic  # noqa: E402

AFTER = {"id": 7, "first_name": "Ada", "balance": 12.5}


class ParseEventTests(unittest.TestCase):
    def test_bare_envelope(self):
        row = parse_event(json.dumps({"op": "c", "ts_ms": 111, "after": AFTER}).encode())
        self.assertEqual(row["id"], 7)
        self.assertEqual((row["_op"], row["_ts_ms"]), ("c", 111))

    def test_schema_envelope(self):
        raw = json.dumps({"schema": {}, "payload": {"op": "u", "ts_ms": 5, "after": AFTER}})
        self.assertEqual(parse_event(raw)["_op"], "u")

    def test_skips_deletes_and_tombstones(self):
        self.assertIsNone(parse_event(json.dumps({"op": "d", "after": None})))
        self.assertIsNone(parse_event(None))
        self.assertIsNone(parse_event(b"null"))
        self.assertIsNone(parse_event(json.dumps({"payload": None})))

    def test_does_not_mutate_input(self):
        parse_event(json.dumps({"op": "c", "after": AFTER}))
        self.assertNotIn("_op", AFTER)

    def test_invalid_json_raises(self):
        with self.assertRaises(json.JSONDecodeError):
            parse_event(b"{not json")


class NamingTests(unittest.TestCase):
    def test_table_from_topic(self):
        self.assertEqual(table_from_topic("banking_server.public.customers"), "customers")

    def test_object_key_is_partitioned_and_unique(self):
        now = datetime(2026, 11, 6, 10, 15, 30, 123456, tzinfo=timezone.utc)
        k1 = object_key("accounts", now, "aaaa1111")
        self.assertEqual(k1, "accounts/date=2026-11-06/accounts_101530123456_aaaa1111.parquet")
        self.assertNotEqual(object_key("accounts", now), object_key("accounts", now))


class BatchBufferTests(unittest.TestCase):
    def test_flush_on_size(self):
        b = BatchBuffer(batch_size=3, flush_interval=100)
        for i in range(2):
            b.add("customers", {"i": i}, now=0)
        self.assertFalse(b.should_flush(1))
        b.add("customers", {"i": 2}, now=0)
        self.assertTrue(b.should_flush(1))

    def test_flush_on_age_only_when_non_empty(self):
        b = BatchBuffer(batch_size=100, flush_interval=10)
        self.assertFalse(b.should_flush(1000))  # empty buffer never flushes
        b.add("accounts", {"i": 1}, now=5)
        self.assertFalse(b.should_flush(14.9))
        self.assertTrue(b.should_flush(15))

    def test_size_is_per_table(self):
        b = BatchBuffer(batch_size=2, flush_interval=100)
        b.add("a", {}, 0)
        b.add("b", {}, 0)
        self.assertFalse(b.should_flush(1))
        b.add("b", {}, 0)
        self.assertTrue(b.should_flush(1))

    def test_drain_resets_everything(self):
        b = BatchBuffer(batch_size=1, flush_interval=1)
        b.add("a", {"x": 1}, 0)
        self.assertEqual(b.drain(), {"a": [{"x": 1}]})
        self.assertEqual(len(b), 0)
        self.assertFalse(b.should_flush(999))
        b.add("a", {"x": 2}, now=500)  # age restarts from the new first row, not the old one
        b.batch_size = 100
        b.flush_interval = 10
        self.assertFalse(b.should_flush(505))
        self.assertTrue(b.should_flush(510))

    def test_rejects_bad_batch_size(self):
        with self.assertRaises(ValueError):
            BatchBuffer(batch_size=0)


if __name__ == "__main__":
    unittest.main()
