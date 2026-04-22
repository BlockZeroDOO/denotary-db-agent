from __future__ import annotations

import json
import socket
import threading
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest.mock import patch

from denotary_db_agent.transport import (
    ChainClient,
    CleosWalletChainClient,
    WatcherClient,
    build_chain_client,
    derive_public_key_candidates,
    inspect_secret_file_permissions,
    resolve_private_key,
)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


class MockChainHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    abi_calls = 0
    push_payloads: list[dict[str, Any]] = []

    def do_POST(self) -> None:  # noqa: N802
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length).decode("utf-8") if content_length else ""
        if self.path == "/v1/chain/get_info":
            payload = {
                "chain_id": "1" * 64,
                "last_irreversible_block_id": "0" * 64,
                "server_version": "mock-chain",
            }
            self._send_json(payload)
            return

        if self.path == "/v1/chain/abi_json_to_bin":
            MockChainHandler.abi_calls += 1
            self._send_json(
                {
                    "code": HTTPStatus.NOT_FOUND,
                    "message": "Not Found",
                    "error": {"what": "abi_json_to_bin is unavailable in this mock"},
                },
                status=HTTPStatus.NOT_FOUND,
            )
            return

        if self.path == "/v1/chain/push_transaction":
            payload = json.loads(body)
            MockChainHandler.push_payloads.append(payload)
            self._send_json(
                {
                    "transaction_id": "c" * 64,
                    "processed": {"block_num": 777},
                }
            )
            return

        self.send_error(HTTPStatus.NOT_FOUND)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class FlakyWatcherHandler(BaseHTTPRequestHandler):
    poll_calls = 0

    def do_POST(self) -> None:  # noqa: N802
        if self.path.endswith("/poll"):
            FlakyWatcherHandler.poll_calls += 1
            if FlakyWatcherHandler.poll_calls == 1:
                self._send_json(
                    {"error": "rpc call failed: <urlopen error The read operation timed out>"},
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return
            self._send_json({"status": "finalized", "inclusion_verified": True})
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class ChainClientLocalPackingTest(unittest.TestCase):
    def setUp(self) -> None:
        MockChainHandler.abi_calls = 0
        MockChainHandler.push_payloads = []
        self.port = free_port()
        self.server = ThreadingHTTPServer(("127.0.0.1", self.port), MockChainHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.client = ChainClient(
            rpc_url=f"http://127.0.0.1:{self.port}",
            submitter="dbagentstest",
            permission="dnanchor",
            private_key="5HpHagT65TZzG1PH3CSu63k8DbpvD8s5ip4nEB3kEsreAnchuDf",
        )

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_push_prepared_action_falls_back_to_local_submit_packing(self) -> None:
        prepared_action = {
            "contract": "vadim1111111",
            "action": "submit",
            "data": {
                "payer": "verification",
                "submitter": "verification",
                "schema_id": 1776342316,
                "policy_id": 1776343316,
                "object_hash": "1" * 64,
                "external_ref": "2" * 64,
            },
        }

        result = self.client.push_prepared_action(prepared_action)

        self.assertEqual(result.tx_id, "c" * 64)
        self.assertEqual(result.block_num, 777)
        self.assertEqual(MockChainHandler.abi_calls, 1)
        self.assertEqual(len(MockChainHandler.push_payloads), 1)
        pushed = MockChainHandler.push_payloads[0]
        self.assertEqual(len(pushed["signatures"]), 1)
        self.assertTrue(pushed["packed_trx"])

    def test_cleos_wallet_chain_client_pushes_submit_action(self) -> None:
        client = CleosWalletChainClient(
            rpc_url="https://history.denotary.io",
            submitter="dbagentstest",
            permission="dnanchor",
            command=["wsl", "cleos"],
        )

        with patch("subprocess.run") as run:
            run.return_value = SimpleNamespace(
                returncode=0,
                stdout=json.dumps(
                    {
                        "transaction_id": "d" * 64,
                        "processed": {"block_num": 123},
                    }
                ),
                stderr="",
            )
            result = client.push_prepared_action(
                {
                    "contract": "verifbill",
                    "action": "submit",
                    "data": {
                        "payer": "dbagentstest",
                        "submitter": "dbagentstest",
                        "schema_id": 1,
                        "policy_id": 1,
                        "object_hash": "1" * 64,
                        "external_ref": "2" * 64,
                    },
                }
            )

        self.assertEqual(result.tx_id, "d" * 64)
        self.assertEqual(result.block_num, 123)
        command = run.call_args.args[0]
        self.assertEqual(command[:7], ["wsl", "cleos", "-u", "https://history.denotary.io", "push", "action", "verifbill"])
        self.assertIn("dbagentstest@dnanchor", command)
        self.assertIn("-j", command)

    def test_cleos_wallet_probe_reports_unlocked_wallets(self) -> None:
        client = CleosWalletChainClient(
            rpc_url="https://history.denotary.io",
            submitter="dbagentstest",
            permission="dnanchor",
            command=["wsl", "cleos"],
        )

        with patch("subprocess.run") as run:
            run.return_value = SimpleNamespace(
                returncode=0,
                stdout='Wallets:\n[\n  "dbagentstest *",\n  "denotary"\n]\n',
                stderr="",
            )
            status = client.probe_wallet()

        self.assertTrue(status["ok"])
        self.assertTrue(status["has_unlocked_wallet"])
        self.assertEqual(status["unlocked_wallet_count"], 1)

    def test_cleos_wallet_accepts_subjective_cpu_response_when_tx_was_included(self) -> None:
        client = CleosWalletChainClient(
            rpc_url="https://history.denotary.io",
            submitter="dbagentstest",
            permission="dnanchor",
            command=["wsl", "cleos"],
        )

        payload = {
            "transaction_id": "e" * 64,
            "processed": {
                "block_num": 321,
                "except": {"name": "tx_cpu_usage_exceeded"},
                "action_traces": [
                    {"except": None},
                    {"except": None},
                ],
            },
        }

        with patch("subprocess.run") as run:
            run.return_value = SimpleNamespace(
                returncode=1,
                stdout=json.dumps(payload),
                stderr="subjective cpu error",
            )
            result = client.push_prepared_action(
                {
                    "contract": "verifbill",
                    "action": "submit",
                    "data": {
                        "payer": "dbagentstest",
                        "submitter": "dbagentstest",
                        "schema_id": 1,
                        "policy_id": 1,
                        "object_hash": "1" * 64,
                        "external_ref": "2" * 64,
                    },
                }
            )

        self.assertEqual(result.tx_id, "e" * 64)
        self.assertEqual(result.block_num, 321)

    def test_cleos_wallet_still_raises_on_non_recoverable_error_output(self) -> None:
        client = CleosWalletChainClient(
            rpc_url="https://history.denotary.io",
            submitter="dbagentstest",
            permission="dnanchor",
            command=["wsl", "cleos"],
        )

        payload = {
            "transaction_id": "f" * 64,
            "processed": {
                "block_num": 456,
                "except": {"name": "eosio_assert_message_exception"},
                "action_traces": [
                    {"except": None},
                    {"except": {"name": "eosio_assert_message_exception"}},
                ],
            },
        }

        with patch("subprocess.run") as run:
            run.return_value = SimpleNamespace(
                returncode=1,
                stdout=json.dumps(payload),
                stderr="duplicate request for submitter",
            )
            with self.assertRaisesRegex(RuntimeError, "duplicate request for submitter"):
                client.push_prepared_action(
                    {
                        "contract": "verifbill",
                        "action": "submit",
                        "data": {
                            "payer": "dbagentstest",
                            "submitter": "dbagentstest",
                            "schema_id": 1,
                            "policy_id": 1,
                            "object_hash": "1" * 64,
                            "external_ref": "2" * 64,
                        },
                    }
                )

    def test_build_chain_client_uses_explicit_cleos_wallet_backend(self) -> None:
        config = SimpleNamespace(
            chain_rpc_url="https://history.denotary.io",
            submitter="dbagentstest",
            submitter_permission="dnanchor",
            submitter_private_key="",
            broadcast_backend="cleos_wallet",
            wallet_command=["wsl", "cleos"],
        )

        client = build_chain_client(config)

        self.assertIsInstance(client, CleosWalletChainClient)

    def test_resolve_private_key_reads_from_env_file(self) -> None:
        with self.subTest("env file"):
            with patch.dict("os.environ", {}, clear=True):
                import tempfile
                from pathlib import Path

                with tempfile.TemporaryDirectory() as temp_dir:
                    env_file = Path(temp_dir) / "agent.env"
                    env_file.write_text("DENOTARY_SUBMITTER_PRIVATE_KEY=test-wif\n", encoding="utf-8")
                    info = resolve_private_key(
                        SimpleNamespace(
                            submitter_private_key="",
                            submitter_private_key_env="DENOTARY_SUBMITTER_PRIVATE_KEY",
                            env_file=str(env_file),
                        )
                    )

        self.assertEqual(info["private_key"], "test-wif")
        self.assertEqual(info["source"], "env")
        self.assertTrue(info["env_file_exists"])
        self.assertTrue(info["env_value_present"])

    def test_build_chain_client_uses_private_key_env_backend(self) -> None:
        with patch.dict("os.environ", {"DENOTARY_SUBMITTER_PRIVATE_KEY": "test-wif"}, clear=True):
            config = SimpleNamespace(
                chain_rpc_url="https://history.denotary.io",
                submitter="dbagentstest",
                submitter_permission="dnanchor",
                submitter_private_key="",
                submitter_private_key_env="DENOTARY_SUBMITTER_PRIVATE_KEY",
                env_file="",
                broadcast_backend="private_key_env",
                wallet_command=[],
            )

            with patch.object(ChainClient, "__init__", return_value=None) as init:
                client = build_chain_client(config)

        self.assertIsInstance(client, ChainClient)
        init.assert_called_once_with("https://history.denotary.io", "dbagentstest", "dnanchor", "test-wif")

    def test_inspect_secret_file_permissions_accepts_strict_posix_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_file = Path(temp_dir) / "agent.env"
            env_file.write_text("DENOTARY_SUBMITTER_PRIVATE_KEY=test-wif\n", encoding="utf-8")
            with patch("denotary_db_agent.transport.os.name", "posix"), patch.object(
                Path,
                "stat",
                return_value=SimpleNamespace(st_mode=0o100600),
            ):
                info = inspect_secret_file_permissions(env_file)

        self.assertTrue(info["checked"])
        self.assertTrue(info["ok"])
        self.assertEqual(info["severity"], "healthy")
        self.assertEqual(info["mode_octal"], "0600")
        self.assertEqual(info["issues"], [])

    def test_inspect_secret_file_permissions_flags_world_readable_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_file = Path(temp_dir) / "agent.env"
            env_file.write_text("DENOTARY_SUBMITTER_PRIVATE_KEY=test-wif\n", encoding="utf-8")
            with patch("denotary_db_agent.transport.os.name", "posix"), patch.object(
                Path,
                "stat",
                return_value=SimpleNamespace(st_mode=0o100644),
            ):
                info = inspect_secret_file_permissions(env_file)

        self.assertTrue(info["checked"])
        self.assertFalse(info["ok"])
        self.assertEqual(info["severity"], "degraded")
        self.assertEqual(info["mode_octal"], "0644")
        self.assertIn("env_file is world-readable or executable", info["issues"])

    def test_derive_public_key_candidates_returns_legacy_and_modern_formats(self) -> None:
        with patch("denotary_db_agent.transport.privtopub", return_value=("pub-x", "pub-y")), patch(
            "denotary_db_agent.transport.encode_pubkey",
            return_value=b"\x02" + (b"\x11" * 32),
        ), patch("denotary_db_agent.transport.changebase", side_effect=["legacy58", "modern58"]):
            info = derive_public_key_candidates("test-wif")

        self.assertEqual(
            info,
            {
                "EOS": "EOSlegacy58",
                "PUB_K1": "PUB_K1_modern58",
            },
        )

    def test_push_prepared_action_falls_back_to_local_submitroot_packing(self) -> None:
        prepared_action = {
            "contract": "vadim1111111",
            "action": "submitroot",
            "data": {
                "payer": "verification",
                "submitter": "verification",
                "schema_id": 1776342316,
                "policy_id": 1776344316,
                "root_hash": "3" * 64,
                "leaf_count": 2,
                "manifest_hash": "4" * 64,
                "external_ref": "5" * 64,
            },
        }

        result = self.client.push_prepared_action(prepared_action)

        self.assertEqual(result.tx_id, "c" * 64)
        self.assertEqual(result.block_num, 777)
        self.assertEqual(MockChainHandler.abi_calls, 1)
        self.assertEqual(len(MockChainHandler.push_payloads), 1)
        pushed = MockChainHandler.push_payloads[0]
        self.assertEqual(len(pushed["signatures"]), 1)
        self.assertTrue(pushed["packed_trx"])


class WatcherClientRetryTest(unittest.TestCase):
    def setUp(self) -> None:
        FlakyWatcherHandler.poll_calls = 0
        self.port = free_port()
        self.server = ThreadingHTTPServer(("127.0.0.1", self.port), FlakyWatcherHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_wait_for_finalized_retries_after_transient_poll_gateway_error(self) -> None:
        client = WatcherClient(f"http://127.0.0.1:{self.port}")

        payload = client.wait_for_finalized("request-1", timeout_sec=2, poll_interval_sec=0.01)

        self.assertEqual(payload["status"], "finalized")
        self.assertTrue(payload["inclusion_verified"])
        self.assertEqual(FlakyWatcherHandler.poll_calls, 2)
