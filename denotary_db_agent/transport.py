from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from denotary_db_agent.models import CanonicalEnvelope


@dataclass
class PreparedRequest:
    request_id: str
    trace_id: str
    external_ref_hash: str
    object_hash: str
    prepared_action: dict[str, Any]
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
            object_hash=str(response["object_hash"]),
            prepared_action=dict(response["prepared_action"]),
            raw=response,
        )

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8")
            raise RuntimeError(f"ingress request failed: {exc.code} {body}") from exc
        return json.loads(body) if body else {}


class WatcherClient:
    def __init__(self, base_url: str, auth_token: str = ""):
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token

    def register(self, prepared: PreparedRequest, envelope: CanonicalEnvelope) -> dict[str, Any]:
        payload = {
            "request_id": prepared.request_id,
            "trace_id": prepared.trace_id,
            "mode": "single",
            "submitter": prepared.prepared_action.get("data", {}).get("submitter"),
            "tx_id": None,
            "chain_account": prepared.prepared_action.get("account"),
            "prepared_action": prepared.prepared_action,
            "hashes": {
                "object_hash": prepared.object_hash,
                "external_ref_hash": prepared.external_ref_hash,
                "canonical_after_hash": envelope.after_hash,
                "metadata_hash": envelope.metadata_hash,
            },
            "source": {
                "source_type": envelope.source_type,
                "source_instance": envelope.source_instance,
                "database_name": envelope.database_name,
                "schema_or_namespace": envelope.schema_or_namespace,
                "table_or_collection": envelope.table_or_collection,
                "external_ref": envelope.external_ref,
            },
        }
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["X-DeNotary-Token"] = self.auth_token
        request = urllib.request.Request(
            f"{self.base_url}/v1/watch/register",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8")
            raise RuntimeError(f"watcher request failed: {exc.code} {body}") from exc
        return json.loads(body) if body else {}
