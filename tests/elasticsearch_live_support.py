from __future__ import annotations

import os
import socket
import time
from pathlib import Path

try:
    from elasticsearch import Elasticsearch
except ImportError:  # pragma: no cover - live test dependency gate
    Elasticsearch = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ELASTICSEARCH_REQUIRED_ENV_VARS = ("DENOTARY_ELASTICSEARCH_URL",)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def missing_elasticsearch_env() -> list[str]:
    missing = [name for name in ELASTICSEARCH_REQUIRED_ENV_VARS if not os.environ.get(name)]
    if Elasticsearch is None:
        missing.append("elasticsearch")
    return missing


def create_client():
    if Elasticsearch is None:
        raise RuntimeError("elasticsearch is required for live Elasticsearch validation")
    kwargs: dict[str, object] = {"hosts": [os.environ["DENOTARY_ELASTICSEARCH_URL"]]}
    username = os.environ.get("DENOTARY_ELASTICSEARCH_USERNAME")
    password = os.environ.get("DENOTARY_ELASTICSEARCH_PASSWORD")
    if username and password:
        kwargs["basic_auth"] = (username, password)
    verify_certs = os.environ.get("DENOTARY_ELASTICSEARCH_VERIFY_CERTS")
    if verify_certs is not None:
        kwargs["verify_certs"] = verify_certs.strip().lower() in {"1", "true", "yes", "on"}
    return Elasticsearch(**kwargs)


def agent_connection_config() -> dict[str, object]:
    payload: dict[str, object] = {
        "url": os.environ["DENOTARY_ELASTICSEARCH_URL"],
    }
    if os.environ.get("DENOTARY_ELASTICSEARCH_USERNAME"):
        payload["username"] = os.environ["DENOTARY_ELASTICSEARCH_USERNAME"]
    if os.environ.get("DENOTARY_ELASTICSEARCH_PASSWORD"):
        payload["password"] = os.environ["DENOTARY_ELASTICSEARCH_PASSWORD"]
    if os.environ.get("DENOTARY_ELASTICSEARCH_VERIFY_CERTS"):
        payload["verify_certs"] = os.environ["DENOTARY_ELASTICSEARCH_VERIFY_CERTS"].strip().lower() in {"1", "true", "yes", "on"}
    return payload


def wait_for_index_visibility(index_name: str, timeout_sec: float = 30.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        client = create_client()
        try:
            if client.indices.exists(index=index_name):
                return
        finally:
            client.close()
        time.sleep(1)
    raise RuntimeError(f"elasticsearch index {index_name} did not become visible in time")
