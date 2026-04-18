from __future__ import annotations

from dataclasses import dataclass

from denotary_db_agent.adapters.base import BaseAdapter
from denotary_db_agent.adapters.registry import build_adapter
from denotary_db_agent.config import AgentConfig, SourceConfig


@dataclass
class SourceRuntime:
    config: SourceConfig
    adapter: BaseAdapter


class SourceRuntimeRegistry:
    def __init__(self, config: AgentConfig):
        self.config = config
        self._runtime_cache: dict[str, SourceRuntime] = {}

    def close(self) -> None:
        for runtime in self._runtime_cache.values():
            runtime.adapter.stop_stream()
        self._runtime_cache.clear()

    def runtimes(self) -> list[SourceRuntime]:
        enabled_ids = {item.id for item in self.config.sources if item.enabled}
        stale_ids = [source_id for source_id in self._runtime_cache if source_id not in enabled_ids]
        for source_id in stale_ids:
            runtime = self._runtime_cache.pop(source_id)
            runtime.adapter.stop_stream()

        runtimes: list[SourceRuntime] = []
        for item in self.config.sources:
            if not item.enabled:
                continue
            runtime = self._runtime_cache.get(item.id)
            if runtime is None or runtime.config is not item:
                if runtime is not None:
                    runtime.adapter.stop_stream()
                runtime = SourceRuntime(config=item, adapter=build_adapter(item))
                self._runtime_cache[item.id] = runtime
            runtimes.append(runtime)
        return runtimes
