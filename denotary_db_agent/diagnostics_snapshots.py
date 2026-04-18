from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def default_snapshot_path(state_db: str, source_id: str | None, prefix: str = "diagnostics") -> Path:
    base_dir = Path(state_db).resolve().parent / "diagnostics"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = source_id or "all"
    return base_dir / f"{prefix}-{suffix}-{timestamp}.json"


def default_diagnostics_snapshot_path(state_db: str, source_id: str | None) -> Path:
    return default_snapshot_path(state_db, source_id, "diagnostics")


def write_json_snapshot(payload: dict, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def prune_snapshot_files(
    snapshot_path: str | Path,
    keep_count: int,
    source_id: str | None,
    prefix: str = "diagnostics",
) -> list[Path]:
    if keep_count < 1:
        raise ValueError("snapshot retention must be at least 1")
    path = Path(snapshot_path)
    suffix = source_id or "all"
    matches = sorted(path.parent.glob(f"{prefix}-{suffix}-*.json"), key=lambda item: item.name, reverse=True)
    removed: list[Path] = []
    for old_path in matches[keep_count:]:
        if old_path.exists():
            old_path.unlink()
            removed.append(old_path)
    return removed


def prune_diagnostics_snapshots(snapshot_path: str | Path, keep_count: int, source_id: str | None) -> list[Path]:
    return prune_snapshot_files(snapshot_path, keep_count, source_id, "diagnostics")


def export_diagnostics_snapshot(
    payload: dict,
    *,
    state_db: str,
    source_id: str | None,
    output_path: str | Path | None = None,
    retention: int = 20,
) -> tuple[Path, list[Path]]:
    snapshot_path = write_json_snapshot(
        payload,
        output_path if output_path is not None else default_diagnostics_snapshot_path(state_db, source_id),
    )
    removed = prune_diagnostics_snapshots(snapshot_path, retention, source_id)
    return snapshot_path, removed


def export_named_snapshot(
    payload: dict,
    *,
    state_db: str,
    source_id: str | None,
    prefix: str,
    output_path: str | Path | None = None,
    retention: int = 20,
) -> tuple[Path, list[Path]]:
    snapshot_path = write_json_snapshot(
        payload,
        output_path if output_path is not None else default_snapshot_path(state_db, source_id, prefix),
    )
    removed = prune_snapshot_files(snapshot_path, retention, source_id, prefix)
    return snapshot_path, removed
