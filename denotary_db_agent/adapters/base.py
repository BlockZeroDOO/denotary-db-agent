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
    capture_modes: tuple[str, ...] = ()
    bootstrap_requirements: tuple[str, ...] = ()


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
            "capture_modes": list(capabilities.capture_modes),
            "bootstrap_requirements": list(capabilities.bootstrap_requirements),
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


class ScaffoldCdcAdapter(BaseAdapter):
    required_connection_fields: tuple[str, ...] = ()

    def validate_connection(self) -> None:
        missing = [name for name in self.required_connection_fields if not self.config.connection.get(name)]
        if missing:
            raise ValueError(self._missing_connection_error(missing))

    def start_stream(self, checkpoint: SourceCheckpoint | None) -> Iterable[ChangeEvent]:
        raise NotImplementedError(f"{self.source_type} CDC streaming is not implemented in the scaffold yet")

    def stop_stream(self) -> None:
        return None

    def read_snapshot(self, checkpoint: SourceCheckpoint | None = None) -> Iterable[ChangeEvent]:
        return iter(())

    def serialize_checkpoint(self, event: ChangeEvent) -> str:
        return event.checkpoint_token or event.change_version

    def resume_from_checkpoint(self, checkpoint: SourceCheckpoint | None) -> None:
        return None

    def _missing_connection_error(self, missing: list[str]) -> str:
        field_label = "field" if len(missing) == 1 else "fields"
        return f"{self.source_type} connection is missing required {field_label}: {', '.join(missing)}"
