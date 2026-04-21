from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from denotary_db_agent.adapters.redis import RedisAdapter
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import SourceCheckpoint


class FakeRedisClient:
    def __init__(self, data: dict[str, tuple[str, object]]):
        self.data = data
        self.closed = False

    def ping(self) -> bool:
        return True

    def scan(self, cursor: int = 0, match: str | None = None, count: int = 100):
        keys = sorted(key for key in self.data if match is None or self._matches(key, match))
        encoded = [key.encode("utf-8") for key in keys]
        return 0, encoded

    def type(self, key: str):
        return self.data[key][0].encode("utf-8")

    def get(self, key: str):
        return self.data[key][1]

    def hgetall(self, key: str):
        return self.data[key][1]

    def lrange(self, key: str, start: int, stop: int):
        return self.data[key][1]

    def smembers(self, key: str):
        return self.data[key][1]

    def zrange(self, key: str, start: int, stop: int, withscores: bool = False):
        return self.data[key][1]

    def dump(self, key: str):
        value = self.data[key][1]
        if isinstance(value, bytes):
            return value
        return json.dumps(value, sort_keys=True).encode("utf-8")

    def close(self) -> None:
        self.closed = True

    def _matches(self, key: str, pattern: str) -> bool:
        if pattern.endswith("*"):
            return key.startswith(pattern[:-1])
        return key == pattern


class FakeRedisModule:
    def __init__(self, datasets: dict[int, dict[str, tuple[str, object]]]):
        self.datasets = datasets

    class RedisFactory:
        def __init__(self, datasets: dict[int, dict[str, tuple[str, object]]]):
            self.datasets = datasets

        def __call__(self, *args, **kwargs):
            return FakeRedisClient(self.datasets[int(kwargs.get("db", 0))])

        def from_url(self, url: str, **kwargs):
            return FakeRedisClient(self.datasets[int(kwargs.get("db", 0))])

    @property
    def Redis(self):
        return self.RedisFactory(self.datasets)


def build_source_config(connection: dict[str, object] | None = None, options: dict[str, object] | None = None) -> SourceConfig:
    return SourceConfig(
        id="redis-source",
        adapter="redis",
        enabled=True,
        source_instance="cache-eu-1",
        database_name="db0",
        backfill_mode="full",
        include={"0": ["orders:*"]},
        connection=connection if connection is not None else {
            "host": "127.0.0.1",
            "port": 6379,
        },
        options=options if options is not None else {"capture_mode": "scan"},
    )


class RedisAdapterTest(unittest.TestCase):
    def test_capabilities_declare_wave_two_snapshot_baseline(self) -> None:
        adapter = RedisAdapter(build_source_config())
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "redis")
        self.assertFalse(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("scan",))
        self.assertEqual(capabilities.cdc_modes, ())
        self.assertEqual(capabilities.default_capture_mode, "scan")
        self.assertEqual(capabilities.checkpoint_strategy, "key_lexicographic")
        self.assertEqual(capabilities.activity_model, "polling")
        self.assertIn("Wave 2", capabilities.notes)

    def test_validate_connection_requires_host_or_url_and_port(self) -> None:
        adapter = RedisAdapter(build_source_config(connection={}))
        with self.assertRaisesRegex(ValueError, "host or url"):
            adapter.validate_connection()

        adapter = RedisAdapter(build_source_config(connection={"host": "127.0.0.1"}))
        with self.assertRaisesRegex(ValueError, "port"):
            adapter.validate_connection()

    def test_validate_connection_requires_connector_for_live_use(self) -> None:
        adapter = RedisAdapter(build_source_config())
        with patch("denotary_db_agent.adapters.redis.redis_lib", None):
            with self.assertRaisesRegex(RuntimeError, "redis is required"):
                adapter.validate_connection()

    def test_bootstrap_and_inspect_report_tracked_keys(self) -> None:
        adapter = RedisAdapter(build_source_config())
        fake_redis = FakeRedisModule({0: {"orders:101": ("string", b"issued")}})

        with patch("denotary_db_agent.adapters.redis.redis_lib", fake_redis):
            bootstrap = adapter.bootstrap()
            inspect_payload = adapter.inspect()

        self.assertEqual(bootstrap["capture_mode"], "scan")
        self.assertEqual(bootstrap["tracked_keys"][0]["database_number"], 0)
        self.assertEqual(bootstrap["tracked_keys"][0]["key_pattern"], "orders:*")
        self.assertEqual(bootstrap["cdc"]["configured_capture_mode"], "scan")
        self.assertEqual(inspect_payload["checkpoint_strategy"], "key_lexicographic")
        self.assertEqual(inspect_payload["activity_model"], "polling")
        self.assertEqual(inspect_payload["cdc"]["runtime"]["transport"], "polling")

    def test_runtime_signature_is_deterministic_for_configured_patterns(self) -> None:
        adapter = RedisAdapter(build_source_config(options={"capture_mode": "scan", "dry_run_events": []}))
        signature = json.loads(adapter.runtime_signature())

        self.assertEqual(signature["adapter"], "redis")
        self.assertEqual(signature["capture_mode"], "scan")
        self.assertEqual(signature["tracked_keys"][0]["database_name"], "db0")
        self.assertEqual(signature["tracked_keys"][0]["key_pattern"], "orders:*")

    def test_read_snapshot_replays_dry_run_events(self) -> None:
        adapter = RedisAdapter(
            build_source_config(
                options={
                    "capture_mode": "scan",
                    "dry_run_events": [
                        {
                            "schema_or_namespace": "db0",
                            "table_or_collection": "orders:*",
                            "operation": "snapshot",
                            "primary_key": {"key": "orders:101"},
                            "change_version": "db0:orders:101",
                            "commit_timestamp": "2026-04-20T10:00:00Z",
                            "after": {"key": "orders:101", "type": "string", "value": "issued"},
                            "checkpoint_token": '{"db0:orders:*":{"last_key":"orders:101"}}',
                        }
                    ],
                }
            )
        )

        events = list(adapter.read_snapshot(SourceCheckpoint(source_id="redis-source", token="{}", updated_at="2026-04-20T10:00:01Z")))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].primary_key, {"key": "orders:101"})
        self.assertEqual(events[0].after["value"], "issued")
        self.assertIn('"db0:orders:*"', events[0].checkpoint_token)

    def test_read_snapshot_emits_keys_and_checkpoint_state(self) -> None:
        adapter = RedisAdapter(build_source_config(options={"capture_mode": "scan", "row_limit": 100}))
        fake_redis = FakeRedisModule(
            {
                0: {
                    "orders:101": ("string", b"issued"),
                    "orders:102": ("hash", {b"status": b"paid", b"currency": b"EUR"}),
                }
            }
        )

        with patch("denotary_db_agent.adapters.redis.redis_lib", fake_redis):
            events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].primary_key, {"key": "orders:101"})
        self.assertEqual(events[0].after["type"], "string")
        self.assertEqual(events[1].after["value"]["status"], "paid")
        self.assertIn('"db0:orders:*"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = RedisAdapter(build_source_config(options={"capture_mode": "scan", "row_limit": 100}))
        fake_redis = FakeRedisModule(
            {
                0: {
                    "orders:101": ("string", b"issued"),
                    "orders:102": ("string", b"paid"),
                }
            }
        )
        checkpoint = SourceCheckpoint(
            source_id="redis-source",
            token='{"db0:orders:*":{"last_key":"orders:101"}}',
            updated_at="2026-04-20T10:05:00Z",
        )

        with patch("denotary_db_agent.adapters.redis.redis_lib", fake_redis):
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].primary_key, {"key": "orders:102"})


if __name__ == "__main__":
    unittest.main()
