from __future__ import annotations

import json
import socket
import threading
import unittest
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from denotary_db_agent.transport import ChainClient


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


class MockChainHandler(BaseHTTPRequestHandler):
    abi_calls = 0
    push_payloads: list[dict[str, Any]] = []

    def do_POST(self) -> None:  # noqa: N802
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
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        if self.path == "/v1/chain/push_transaction":
            body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
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

    def _send_json(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(HTTPStatus.OK)
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
