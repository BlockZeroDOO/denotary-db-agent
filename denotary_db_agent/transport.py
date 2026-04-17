from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from eosapi import EosApi

from denotary_db_agent.models import CanonicalEnvelope


def _request_json(
    url: str,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    expected_status: int | None = None,
) -> tuple[int, dict[str, Any]]:
    request_headers = dict(headers or {})
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            status_code = response.status
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        status_code = exc.code
        payload_data = json.loads(body) if body else {}
        if expected_status is not None and status_code == expected_status:
            return status_code, payload_data
        raise RuntimeError(f"http request failed: {status_code} {payload_data}") from exc

    payload_data = json.loads(body) if body else {}
    if expected_status is not None and status_code != expected_status:
        raise RuntimeError(f"http request failed: expected {expected_status}, got {status_code}: {payload_data}")
    return status_code, payload_data


@dataclass
class PreparedRequest:
    request_id: str
    trace_id: str
    external_ref_hash: str
    object_hash: str | None
    root_hash: str | None
    manifest_hash: str | None
    leaf_count: int | None
    verification_account: str
    prepared_action: dict[str, Any]
    raw: dict[str, Any]


@dataclass
class BroadcastResult:
    tx_id: str
    block_num: int
    raw: dict[str, Any]


class IngressClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def prepare_single(self, payload: dict[str, Any]) -> PreparedRequest:
        response = self._post_json("/v1/single/prepare", payload)
        return PreparedRequest(
            request_id=str(response["request_id"]),
            trace_id=str(response["trace_id"]),
            external_ref_hash=str(response["external_ref_hash"]),
            object_hash=str(response.get("object_hash")) if response.get("object_hash") is not None else None,
            root_hash=None,
            manifest_hash=None,
            leaf_count=None,
            verification_account=str(response["verification_account"]),
            prepared_action=dict(response["prepared_action"]),
            raw=response,
        )

    def prepare_batch(self, payload: dict[str, Any]) -> PreparedRequest:
        response = self._post_json("/v1/batch/prepare", payload)
        return PreparedRequest(
            request_id=str(response["request_id"]),
            trace_id=str(response["trace_id"]),
            external_ref_hash=str(response["external_ref_hash"]),
            object_hash=None,
            root_hash=str(response["root_hash"]),
            manifest_hash=str(response["manifest_hash"]),
            leaf_count=int(response["leaf_count"]),
            verification_account=str(response["verification_account"]),
            prepared_action=dict(response["prepared_action"]),
            raw=response,
        )

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        _, body = _request_json(f"{self.base_url}{path}", method="POST", payload=payload, expected_status=200)
        return body


class ChainClient:
    def __init__(self, rpc_url: str, submitter: str, permission: str, private_key: str):
        self.rpc_url = rpc_url.rstrip("/")
        self.submitter = submitter
        self.permission = permission
        self.private_key = private_key
        self.api = EosApi(rpc_host=self.rpc_url)
        self.api.import_key(self.submitter, self.private_key, self.permission)

    def health(self) -> dict[str, Any]:
        return dict(self.api.get_info())

    def push_prepared_action(self, prepared_action: dict[str, Any]) -> BroadcastResult:
        contract = str(prepared_action.get("contract") or prepared_action.get("account") or "")
        action = str(prepared_action.get("action") or prepared_action.get("name") or "")
        data = prepared_action.get("data")
        if not contract or not action or not isinstance(data, dict):
            raise RuntimeError(f"prepared_action is incomplete: {prepared_action}")

        result = self.api.push_transaction(
            {
                "actions": [
                    {
                        "account": contract,
                        "name": action,
                        "authorization": [
                            {
                                "actor": self.submitter,
                                "permission": self.permission,
                            }
                        ],
                        "data": data,
                    }
                ]
            }
        )
        tx_id = str(result.get("transaction_id") or "").lower()
        block_num = int(result.get("processed", {}).get("block_num") or 0)
        if len(tx_id) != 64 or block_num <= 0:
            raise RuntimeError(f"unexpected push_transaction response: {result}")
        return BroadcastResult(tx_id=tx_id, block_num=block_num, raw=result)


class WatcherClient:
    def __init__(self, base_url: str, auth_token: str = "", rpc_url: str = ""):
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self.rpc_url = rpc_url

    def headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.auth_token:
            headers["X-DeNotary-Token"] = self.auth_token
        return headers

    def register(self, prepared: PreparedRequest, envelope: CanonicalEnvelope) -> dict[str, Any]:
        anchor: dict[str, Any] = {
            "external_ref_hash": prepared.external_ref_hash,
        }
        if prepared.object_hash:
            anchor["object_hash"] = prepared.object_hash
        if prepared.root_hash:
            anchor["root_hash"] = prepared.root_hash
        if prepared.manifest_hash:
            anchor["manifest_hash"] = prepared.manifest_hash
        if prepared.leaf_count is not None:
            anchor["leaf_count"] = prepared.leaf_count

        payload = {
            "request_id": prepared.request_id,
            "trace_id": prepared.trace_id,
            "mode": "single" if prepared.object_hash else "batch",
            "submitter": prepared.prepared_action.get("data", {}).get("submitter"),
            "contract": prepared.verification_account,
            "anchor": anchor,
        }
        if self.rpc_url:
            payload["rpc_url"] = self.rpc_url
        return self._post_json("/v1/watch/register", payload)

    def mark_included(self, request_id: str, tx_id: str, block_num: int) -> dict[str, Any]:
        payload: dict[str, Any] = {"tx_id": tx_id, "block_num": block_num}
        if self.rpc_url:
            payload["rpc_url"] = self.rpc_url
        return self._post_json(f"/v1/watch/{request_id}/included", payload)

    def mark_failed(self, request_id: str, reason: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"reason": reason}
        if details:
            payload["details"] = details
        return self._post_json(f"/v1/watch/{request_id}/failed", payload)

    def poll_request(self, request_id: str) -> dict[str, Any]:
        return self._post_json(f"/v1/watch/{request_id}/poll", {})

    def get_request(self, request_id: str) -> dict[str, Any]:
        _, body = _request_json(
            f"{self.base_url}/v1/watch/{request_id}",
            method="GET",
            headers=self.headers(),
            expected_status=200,
        )
        return body

    def wait_for_finalized(self, request_id: str, timeout_sec: int, poll_interval_sec: float) -> dict[str, Any]:
        deadline = time.time() + timeout_sec
        last_status: dict[str, Any] | None = None
        while time.time() < deadline:
            last_status = self.poll_request(request_id)
            status = str(last_status.get("status") or "")
            if status == "failed":
                raise RuntimeError(f"watcher marked request failed: {last_status}")
            if status == "finalized" and last_status.get("inclusion_verified") is True:
                return last_status
            time.sleep(poll_interval_sec)
        raise TimeoutError(f"request {request_id} did not finalize in time: {last_status}")

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        _, body = _request_json(
            f"{self.base_url}{path}",
            method="POST",
            payload=payload,
            headers=self.headers(),
            expected_status=200,
        )
        return body


class ReceiptClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def health(self) -> dict[str, Any]:
        _, body = _request_json(f"{self.base_url}/healthz", method="GET", expected_status=200)
        return body

    def get_receipt(self, request_id: str) -> dict[str, Any]:
        _, body = _request_json(
            f"{self.base_url}/v1/receipts/{request_id}",
            method="GET",
            expected_status=200,
        )
        return body


class AuditClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def health(self) -> dict[str, Any]:
        _, body = _request_json(f"{self.base_url}/healthz", method="GET", expected_status=200)
        return body

    def get_chain(self, request_id: str) -> dict[str, Any]:
        _, body = _request_json(
            f"{self.base_url}/v1/audit/chain/{request_id}",
            method="GET",
            expected_status=200,
        )
        return body


def export_proof_bundle(
    proof_dir: str,
    source_id: str,
    request_id: str,
    envelope: CanonicalEnvelope,
    prepared: PreparedRequest,
    broadcast: BroadcastResult,
    receipt: dict[str, Any],
    audit_chain: dict[str, Any],
) -> str:
    export_root = Path(proof_dir)
    export_root.mkdir(parents=True, exist_ok=True)
    export_path = export_root / source_id / f"{request_id}.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "request_id": request_id,
        "trace_id": prepared.trace_id,
        "canonical_envelope": {
            "source_type": envelope.source_type,
            "source_instance": envelope.source_instance,
            "database_name": envelope.database_name,
            "schema_or_namespace": envelope.schema_or_namespace,
            "table_or_collection": envelope.table_or_collection,
            "operation": envelope.operation,
            "primary_key": envelope.primary_key,
            "change_version": envelope.change_version,
            "commit_timestamp": envelope.commit_timestamp,
            "before_hash": envelope.before_hash,
            "after_hash": envelope.after_hash,
            "metadata_hash": envelope.metadata_hash,
            "external_ref": envelope.external_ref,
            "trace_id": envelope.trace_id,
        },
        "prepared_action": prepared.prepared_action,
        "broadcast": {
            "tx_id": broadcast.tx_id,
            "block_num": broadcast.block_num,
        },
        "receipt": receipt,
        "audit_chain": audit_chain,
    }
    export_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return str(export_path)
