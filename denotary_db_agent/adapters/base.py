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
    cdc_modes: tuple[str, ...] = ()
    default_capture_mode: str = "watermark"
    bootstrap_requirements: tuple[str, ...] = ()
    checkpoint_strategy: str = "opaque"
    activity_model: str = "polling"


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

    def capture_mode(self) -> str:
        configured = self.config.options.get("capture_mode")
        if isinstance(configured, str) and configured.strip():
            return configured.strip().lower()
        capabilities = self.discover_capabilities()
        if capabilities.default_capture_mode:
            return capabilities.default_capture_mode
        if capabilities.capture_modes:
            return str(capabilities.capture_modes[0]).lower()
        return "watermark"

    def is_cdc_mode(self) -> bool:
        capabilities = self.discover_capabilities()
        if capabilities.cdc_modes:
            return self.capture_mode() in capabilities.cdc_modes
        if not capabilities.supports_cdc:
            return False
        return self.capture_mode() != capabilities.default_capture_mode

    def iter_events(self, checkpoint: SourceCheckpoint | None) -> Iterable[ChangeEvent]:
        if self.is_cdc_mode():
            return self.start_stream(checkpoint)
        return self.read_snapshot(checkpoint)

    def should_wait_for_activity(self) -> bool:
        return self.is_cdc_mode()

    def wait_for_changes(self, timeout_sec: float) -> bool:
        if timeout_sec > 0:
            time.sleep(timeout_sec)
        return False

    def after_checkpoint_advanced(self, token: str) -> None:
        return None

    def checkpoint_strategy(self) -> str:
        return self.discover_capabilities().checkpoint_strategy

    def activity_model(self) -> str:
        return self.discover_capabilities().activity_model

    def build_cdc_summary(self, extra: dict[str, object] | None = None) -> dict[str, object]:
        capabilities = self.discover_capabilities()
        summary: dict[str, object] = {
            "configured_capture_mode": self.capture_mode(),
            "supports_cdc": capabilities.supports_cdc,
            "is_cdc_mode": self.is_cdc_mode(),
            "checkpoint_strategy": capabilities.checkpoint_strategy,
            "activity_model": capabilities.activity_model,
            "default_capture_mode": capabilities.default_capture_mode,
            "cdc_modes": list(capabilities.cdc_modes),
        }
        if extra:
            summary.update(extra)
        return summary

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
            "cdc_modes": list(capabilities.cdc_modes),
            "default_capture_mode": capabilities.default_capture_mode,
            "bootstrap_requirements": list(capabilities.bootstrap_requirements),
            "checkpoint_strategy": capabilities.checkpoint_strategy,
            "activity_model": capabilities.activity_model,
            "is_cdc_mode": self.is_cdc_mode(),
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
    minimum_version: str = ""
    scaffold_supports_cdc: bool = True
    scaffold_supports_snapshot: bool = True
    scaffold_operations: tuple[str, ...] = ("insert", "update", "delete")
    scaffold_capture_modes: tuple[str, ...] = ()
    scaffold_cdc_modes: tuple[str, ...] = ()
    scaffold_default_capture_mode: str = "watermark"
    scaffold_bootstrap_requirements: tuple[str, ...] = ()
    scaffold_checkpoint_strategy: str = "opaque"
    scaffold_activity_model: str = "polling"
    scaffold_notes: str = ""

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version=self.minimum_version,
            supports_cdc=self.scaffold_supports_cdc,
            supports_snapshot=self.scaffold_supports_snapshot,
            operations=self.scaffold_operations,
            capture_modes=self.scaffold_capture_modes,
            cdc_modes=self.scaffold_cdc_modes,
            default_capture_mode=self.scaffold_default_capture_mode,
            bootstrap_requirements=self.scaffold_bootstrap_requirements,
            checkpoint_strategy=self.scaffold_checkpoint_strategy,
            activity_model=self.scaffold_activity_model,
            notes=self.scaffold_notes,
        )

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
