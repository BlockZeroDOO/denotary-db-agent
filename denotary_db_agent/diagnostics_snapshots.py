from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def default_snapshot_path(state_db: str, source_id: str | None, prefix: str = "diagnostics") -> Path:
    base_dir = Path(state_db).resolve().parent / "diagnostics"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = source_id or "all"
    return base_dir / f"{prefix}-{suffix}-{timestamp}.json"


def default_evidence_manifest_path(state_db: str) -> Path:
    base_dir = Path(state_db).resolve().parent / "diagnostics"
    return base_dir / "evidence-manifest.json"


def default_diagnostics_snapshot_path(state_db: str, source_id: str | None) -> Path:
    return default_snapshot_path(state_db, source_id, "diagnostics")


def write_json_snapshot(payload: dict, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def update_evidence_manifest(
    *,
    state_db: str,
    prefix: str,
    source_id: str | None,
    snapshot_path: str | Path,
    payload: dict,
    removed_paths: list[Path] | None = None,
) -> Path:
    manifest_path = default_evidence_manifest_path(state_db)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict = {"artifacts": []}
    if manifest_path.exists():
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = {"artifacts": []}
    artifacts = existing.get("artifacts")
    if not isinstance(artifacts, list):
        artifacts = []

    removed_set = {str(Path(item).resolve()) for item in (removed_paths or [])}
    artifacts = [
        item
        for item in artifacts
        if isinstance(item, dict)
        and str(item.get("path") or "") not in removed_set
        and Path(str(item.get("path") or "")).exists()
    ]

    path = Path(snapshot_path).resolve()
    severity = None
    if isinstance(payload.get("overall"), dict):
        severity = payload["overall"].get("severity")
    elif isinstance(payload.get("doctor"), dict):
        doctor_overall = payload["doctor"].get("overall")
        if isinstance(doctor_overall, dict):
            severity = doctor_overall.get("severity")

    entry = {
        "kind": prefix,
        "source_id": source_id or payload.get("source_filter") or "all",
        "path": str(path),
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "agent_name": payload.get("agent_name"),
        "size_bytes": path.stat().st_size if path.exists() else 0,
        "severity": severity,
    }
    artifacts.append(entry)
    existing["artifacts"] = artifacts
    manifest_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    return manifest_path


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
    return export_named_snapshot(
        payload,
        state_db=state_db,
        source_id=source_id,
        prefix="diagnostics",
        output_path=output_path,
        retention=retention,
    )


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
    update_evidence_manifest(
        state_db=state_db,
        prefix=prefix,
        source_id=source_id,
        snapshot_path=snapshot_path,
        payload=payload,
        removed_paths=removed,
    )
    return snapshot_path, removed
