from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from eosapi import EosApi
from eosapi.exceptions import NodeException
from eosapi.packer import Name, Uint32, Uint64
from eosapi.transaction import Action, Authorization, Transaction
from cryptos import changebase, encode_pubkey, privtopub

from denotary_db_agent.models import CanonicalEnvelope


def _parse_env_file(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    env_path = Path(path)
    if not path or not env_path.exists():
        return values
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key] = value
    return values


def inspect_secret_file_permissions(path: str | os.PathLike[str]) -> dict[str, Any]:
    file_path = path if isinstance(path, Path) else Path(path)
    result: dict[str, Any] = {
        "path": str(file_path) if path else "",
        "exists": bool(path and file_path.exists()),
        "checked": False,
        "ok": None,
        "platform": os.name,
        "mode_octal": None,
        "severity": "unknown",
        "issues": [],
    }
    if not path or not file_path.exists():
        return result
    if os.name != "posix":
        return result

    mode = stat.S_IMODE(file_path.stat().st_mode)
    issues: list[str] = []
    severity = "healthy"

    if mode & 0o022:
        severity = "critical"
        if mode & 0o020:
            issues.append("env_file is group-writable")
        if mode & 0o002:
            issues.append("env_file is world-writable")

    if mode & 0o055:
        if severity != "critical":
            severity = "degraded"
        if mode & 0o050:
            issues.append("env_file is group-readable or executable")
        if mode & 0o005:
            issues.append("env_file is world-readable or executable")

    result.update(
        {
            "checked": True,
            "ok": severity == "healthy",
            "mode_octal": f"{mode:04o}",
            "severity": severity,
            "issues": issues,
        }
    )
    return result


def derive_public_key_candidates(private_key: str) -> dict[str, str]:
    wif = str(private_key or "").strip()
    if not wif:
        return {}

    compressed = encode_pubkey(privtopub(wif), "bin_compressed")
    legacy_checksum = hashlib.new("ripemd160", compressed).digest()[:4]
    modern_checksum = hashlib.new("ripemd160", compressed + b"K1").digest()[:4]

    return {
        "EOS": "EOS" + changebase(compressed + legacy_checksum, 256, 58),
        "PUB_K1": "PUB_K1_" + changebase(compressed + modern_checksum, 256, 58),
    }


def resolve_private_key(config: Any) -> dict[str, Any]:
    inline_key = str(getattr(config, "submitter_private_key", "") or "").strip()
    env_var_name = str(getattr(config, "submitter_private_key_env", "") or "DENOTARY_SUBMITTER_PRIVATE_KEY").strip()
    env_file_path = str(getattr(config, "env_file", "") or "").strip()
    env_file_values = _parse_env_file(env_file_path) if env_file_path else {}
    env_value = ""
    if env_var_name:
        env_value = str(os.environ.get(env_var_name, "") or env_file_values.get(env_var_name, "") or "").strip()

    if inline_key:
        return {
            "private_key": inline_key,
            "source": "inline",
            "env_var": env_var_name,
            "env_file": env_file_path,
            "env_file_exists": bool(env_file_path and Path(env_file_path).exists()),
            "env_value_present": bool(env_value),
        }
    if env_value:
        return {
            "private_key": env_value,
            "source": "env",
            "env_var": env_var_name,
            "env_file": env_file_path,
            "env_file_exists": bool(env_file_path and Path(env_file_path).exists()),
            "env_value_present": True,
        }
    return {
        "private_key": "",
        "source": "",
        "env_var": env_var_name,
        "env_file": env_file_path,
        "env_file_exists": bool(env_file_path and Path(env_file_path).exists()),
        "env_value_present": False,
    }


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

    def get_account(self, account: str) -> dict[str, Any]:
        _, body = _request_json(
            f"{self.rpc_url}/v1/chain/get_account",
            method="POST",
            payload={"account_name": account},
            expected_status=200,
        )
        return body

    def push_prepared_action(self, prepared_action: dict[str, Any]) -> BroadcastResult:
        contract = str(prepared_action.get("contract") or prepared_action.get("account") or "")
        action = str(prepared_action.get("action") or prepared_action.get("name") or "")
        data = prepared_action.get("data")
        if not contract or not action or not isinstance(data, dict):
            raise RuntimeError(f"prepared_action is incomplete: {prepared_action}")

        transaction = {
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
        try:
            result = self.api.push_transaction(transaction)
        except NodeException as exc:
            if not self._should_fallback_to_local_packing(exc, action, data):
                raise
            result = self._push_with_local_packing(contract, action, data)
        tx_id = str(result.get("transaction_id") or "").lower()
        block_num = int(result.get("processed", {}).get("block_num") or 0)
        if len(tx_id) != 64 or block_num <= 0:
            raise RuntimeError(f"unexpected push_transaction response: {result}")
        return BroadcastResult(tx_id=tx_id, block_num=block_num, raw=result)

    def _push_with_local_packing(self, contract: str, action: str, data: dict[str, Any]) -> dict[str, Any]:
        authorization = [Authorization(actor=self.submitter, permission=self.permission)]
        packed = self._pack_known_action(action, data)
        trx_action = Action(account=contract, name=action, authorization=authorization, data=data)
        trx_action.link(packed)
        trx = Transaction(actions=[trx_action])

        net_info = self.api.get_info()
        trx.link(net_info["last_irreversible_block_id"], net_info["chain_id"])
        trx.sign(self.private_key)
        return self.api.post_transaction(trx)

    def _should_fallback_to_local_packing(self, exc: NodeException, action: str, data: dict[str, Any]) -> bool:
        if not self._supports_local_packing(action, data):
            return False
        response = getattr(exc, "resp", None)
        if response is None:
            return False
        status_code = int(getattr(response, "status_code", 0) or 0)
        request_path = str(getattr(getattr(response, "request", None), "path_url", "") or "")
        if "/v1/chain/abi_json_to_bin" not in request_path:
            return False
        return status_code in {404, 405, 410, 501}

    def _supports_local_packing(self, action: str, data: dict[str, Any]) -> bool:
        try:
            self._pack_known_action(action, data)
        except Exception:
            return False
        return True

    def _pack_known_action(self, action: str, data: dict[str, Any]) -> bytes:
        if action == "submit":
            return b"".join(
                [
                    self._pack_name(data, "payer"),
                    self._pack_name(data, "submitter"),
                    self._pack_uint64(data, "schema_id"),
                    self._pack_uint64(data, "policy_id"),
                    self._pack_checksum256(data, "object_hash"),
                    self._pack_checksum256(data, "external_ref"),
                ]
            )
        if action == "submitroot":
            return b"".join(
                [
                    self._pack_name(data, "payer"),
                    self._pack_name(data, "submitter"),
                    self._pack_uint64(data, "schema_id"),
                    self._pack_uint64(data, "policy_id"),
                    self._pack_checksum256(data, "root_hash"),
                    self._pack_uint32(data, "leaf_count"),
                    self._pack_checksum256(data, "manifest_hash"),
                    self._pack_checksum256(data, "external_ref"),
                ]
            )
        raise RuntimeError(f"local packing is not supported for action {action}")

    def _pack_name(self, data: dict[str, Any], field: str) -> bytes:
        value = str(data[field] or "")
        return Name.pack(value)

    def _pack_uint64(self, data: dict[str, Any], field: str) -> bytes:
        return Uint64.pack(int(data[field]))

    def _pack_uint32(self, data: dict[str, Any], field: str) -> bytes:
        return Uint32.pack(int(data[field]))

    def _pack_checksum256(self, data: dict[str, Any], field: str) -> bytes:
        value = str(data[field] or "")
        packed = bytes.fromhex(value)
        if len(packed) != 32:
            raise RuntimeError(f"{field} must be a 32-byte checksum256 hex string")
        return packed


class CleosWalletChainClient:
    def __init__(self, rpc_url: str, submitter: str, permission: str, command: list[str] | None = None):
        self.rpc_url = rpc_url.rstrip("/")
        self.submitter = submitter
        self.permission = permission
        self.command = list(command or ["cleos"])
        if not self.command:
            raise RuntimeError("wallet command must not be empty")

    def health(self) -> dict[str, Any]:
        _, body = _request_json(f"{self.rpc_url}/v1/chain/get_info", method="POST", payload={}, expected_status=200)
        return body

    def get_account(self, account: str) -> dict[str, Any]:
        _, body = _request_json(
            f"{self.rpc_url}/v1/chain/get_account",
            method="POST",
            payload={"account_name": account},
            expected_status=200,
        )
        return body

    def probe_wallet(self) -> dict[str, Any]:
        result = subprocess.run(
            [*self.command, "wallet", "list"],
            capture_output=True,
            text=True,
            check=False,
        )
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        if result.returncode != 0:
            return {
                "ok": False,
                "error": stderr.strip() or stdout.strip() or f"wallet command exited with {result.returncode}",
                "command": self.command,
            }
        unlocked = [line.strip() for line in stdout.splitlines() if "*" in line]
        return {
            "ok": True,
            "command": self.command,
            "unlocked_wallet_count": len(unlocked),
            "has_unlocked_wallet": bool(unlocked),
            "raw": stdout.strip(),
        }

    def push_prepared_action(self, prepared_action: dict[str, Any]) -> BroadcastResult:
        contract = str(prepared_action.get("contract") or prepared_action.get("account") or "")
        action = str(prepared_action.get("action") or prepared_action.get("name") or "")
        data = prepared_action.get("data")
        if not contract or not action or not isinstance(data, dict):
            raise RuntimeError(f"prepared_action is incomplete: {prepared_action}")

        ordered_args = self._ordered_action_args(action, data)
        command = [
            *self.command,
            "-u",
            self.rpc_url,
            "push",
            "action",
            contract,
            action,
            json.dumps(ordered_args, separators=(",", ":")),
            "-p",
            f"{self.submitter}@{self.permission}",
            "-j",
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            if result.returncode != 0:
                raise RuntimeError(
                    result.stderr.strip() or result.stdout.strip() or f"wallet broadcast failed with exit code {result.returncode}"
                ) from exc
            raise RuntimeError(f"wallet broadcast returned non-JSON output: {result.stdout}") from exc
        if result.returncode != 0 and not self._is_recoverable_subjective_success(payload):
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"wallet broadcast failed with exit code {result.returncode}")
        tx_id = str(payload.get("transaction_id") or "").lower()
        block_num = int(payload.get("processed", {}).get("block_num") or 0)
        if len(tx_id) != 64 or block_num <= 0:
            raise RuntimeError(f"unexpected wallet push output: {payload}")
        return BroadcastResult(tx_id=tx_id, block_num=block_num, raw=payload)

    def _is_recoverable_subjective_success(self, payload: dict[str, Any]) -> bool:
        tx_id = str(payload.get("transaction_id") or "").lower()
        processed = dict(payload.get("processed") or {})
        block_num = int(processed.get("block_num") or 0)
        action_traces = list(processed.get("action_traces") or [])
        processed_except = processed.get("except")

        if len(tx_id) != 64 or block_num <= 0 or not action_traces:
            return False
        if not isinstance(processed_except, dict):
            return False
        if str(processed_except.get("name") or "") != "tx_cpu_usage_exceeded":
            return False
        return all(not isinstance(trace, dict) or trace.get("except") is None for trace in action_traces)

    def _ordered_action_args(self, action: str, data: dict[str, Any]) -> list[Any]:
        if action == "submit":
            return [
                data["payer"],
                data["submitter"],
                int(data["schema_id"]),
                int(data["policy_id"]),
                data["object_hash"],
                data["external_ref"],
            ]
        if action == "submitroot":
            return [
                data["payer"],
                data["submitter"],
                int(data["schema_id"]),
                int(data["policy_id"]),
                data["root_hash"],
                int(data["leaf_count"]),
                data["manifest_hash"],
                data["external_ref"],
            ]
        raise RuntimeError(f"wallet broadcast is not supported for action {action}")


def build_chain_client(config: Any) -> ChainClient | CleosWalletChainClient | None:
    rpc_url = str(getattr(config, "chain_rpc_url", "") or "")
    submitter = str(getattr(config, "submitter", "") or "")
    permission = str(getattr(config, "submitter_permission", "") or "")
    backend = str(getattr(config, "broadcast_backend", "auto") or "auto")
    wallet_command = list(getattr(config, "wallet_command", []) or [])
    key_info = resolve_private_key(config)
    private_key = str(key_info["private_key"] or "")

    if not rpc_url or not submitter or not permission:
        return None
    if backend == "auto":
        backend = "private_key_env" if private_key else "private_key"
    if backend == "private_key":
        if not private_key:
            return None
        return ChainClient(rpc_url, submitter, permission, private_key)
    if backend == "private_key_env":
        if not private_key:
            return None
        return ChainClient(rpc_url, submitter, permission, private_key)
    if backend == "cleos_wallet":
        return CleosWalletChainClient(rpc_url, submitter, permission, wallet_command or ["cleos"])
    return None


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

    @staticmethod
    def _is_retryable_poll_error(error_text: str) -> bool:
        text = str(error_text or "").lower()
        if not text:
            return False
        retryable_markers = (
            "http request failed: 502",
            "http request failed: 503",
            "http request failed: 504",
            "rpc call failed:",
            "timed out",
            "temporarily unavailable",
            "connection reset",
            "connection aborted",
            "connection refused",
        )
        return any(marker in text for marker in retryable_markers)

    def wait_for_finalized(self, request_id: str, timeout_sec: int, poll_interval_sec: float) -> dict[str, Any]:
        deadline = time.time() + timeout_sec
        last_status: dict[str, Any] | None = None
        last_error: str | None = None
        while time.time() < deadline:
            try:
                last_status = self.poll_request(request_id)
            except RuntimeError as exc:
                if not self._is_retryable_poll_error(str(exc)):
                    raise
                last_error = str(exc)
                time.sleep(poll_interval_sec)
                continue
            status = str(last_status.get("status") or "")
            if status == "failed":
                raise RuntimeError(f"watcher marked request failed: {last_status}")
            if status == "finalized" and last_status.get("inclusion_verified") is True:
                return last_status
            time.sleep(poll_interval_sec)
        timeout_state: object = last_status if last_status is not None else last_error
        raise TimeoutError(f"request {request_id} did not finalize in time: {timeout_state}")

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


def export_batch_proof_bundle(
    proof_dir: str,
    source_id: str,
    request_id: str,
    envelopes: list[CanonicalEnvelope],
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
        "mode": "batch",
        "members": [
            {
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
            }
            for envelope in envelopes
        ],
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
