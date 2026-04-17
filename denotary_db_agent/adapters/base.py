from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable

from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint


@dataclass
class AdapterCapabilities:
    source_type: str
    minimum_version: str
    supports_cdc: bool
    supports_snapshot: bool
    operations: tuple[str, ...]
    notes: str


class BaseAdapter(ABC):
    source_type = "base"

    def __init__(self, config: SourceConfig):
        self.config = config

    @abstractmethod
    def discover_capabilities(self) -> AdapterCapabilities:
        raise NotImplementedError

    @abstractmethod
    def validate_connection(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def start_stream(self, checkpoint: SourceCheckpoint | None) -> Iterable[ChangeEvent]:
        raise NotImplementedError

    @abstractmethod
    def stop_stream(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def read_snapshot(self, checkpoint: SourceCheckpoint | None = None) -> Iterable[ChangeEvent]:
        raise NotImplementedError

    @abstractmethod
    def serialize_checkpoint(self, event: ChangeEvent) -> str:
        raise NotImplementedError

    @abstractmethod
    def resume_from_checkpoint(self, checkpoint: SourceCheckpoint | None) -> None:
        raise NotImplementedError

    def wait_for_changes(self, timeout_sec: float) -> bool:
        if timeout_sec > 0:
            time.sleep(timeout_sec)
        return False

    def after_checkpoint_advanced(self, token: str) -> None:
        return None

    def bootstrap(self) -> dict:
        self.validate_connection()
        return {
            "source_id": self.config.id,
            "adapter": self.config.adapter,
            "bootstrap": "validated",
        }

    def inspect(self) -> dict:
        capabilities = self.discover_capabilities()
        return {
            "source_id": self.config.id,
            "adapter": self.config.adapter,
            "source_type": capabilities.source_type,
            "supports_cdc": capabilities.supports_cdc,
            "supports_snapshot": capabilities.supports_snapshot,
            "operations": list(capabilities.operations),
            "notes": capabilities.notes,
        }

    def runtime_signature(self) -> str:
        payload = {
            "adapter": self.config.adapter,
            "source_id": self.config.id,
            "include": self.config.include,
            "exclude": self.config.exclude,
            "options": self.config.options,
        }
        return json.dumps(payload, sort_keys=True)

    def refresh_runtime(self) -> dict:
        return self.bootstrap()
