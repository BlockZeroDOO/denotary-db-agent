from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from denotary_db_agent.models import DeliveryAttempt, ProofArtifact, SourceCheckpoint


class CheckpointStore:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path)
        try:
            connection.row_factory = sqlite3.Row
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                create table if not exists checkpoints (
                    source_id text primary key,
                    token text not null,
                    updated_at text not null
                );

                create table if not exists deliveries (
                    request_id text primary key,
                    trace_id text not null,
                    source_id text not null,
                    external_ref text not null,
                    tx_id text,
                    status text not null,
                    prepared_action_json text,
                    last_error text,
                    updated_at text not null
                );

                create table if not exists dlq (
                    id integer primary key autoincrement,
                    source_id text not null,
                    reason text not null,
                    payload_json text not null,
                    created_at text not null
                );

                create table if not exists proofs (
                    request_id text primary key,
                    source_id text not null,
                    receipt_json text,
                    audit_chain_json text,
                    export_path text,
                    updated_at text not null
                );

                create table if not exists source_controls (
                    source_id text primary key,
                    paused integer not null default 0,
                    updated_at text not null
                );

                create table if not exists source_runtime (
                    source_id text primary key,
                    signature text not null,
                    updated_at text not null
                );
                """
            )

    def get_checkpoint(self, source_id: str) -> SourceCheckpoint | None:
        with self._connect() as connection:
            row = connection.execute(
                "select source_id, token, updated_at from checkpoints where source_id = ?",
                (source_id,),
            ).fetchone()
        if row is None:
            return None
        return SourceCheckpoint(source_id=row["source_id"], token=row["token"], updated_at=row["updated_at"])

    def set_checkpoint(self, source_id: str, token: str, updated_at: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                insert into checkpoints (source_id, token, updated_at)
                values (?, ?, ?)
                on conflict(source_id) do update set
                    token = excluded.token,
                    updated_at = excluded.updated_at
                """,
                (source_id, token, updated_at),
            )

    def reset_checkpoint(self, source_id: str) -> None:
        with self._connect() as connection:
            connection.execute("delete from checkpoints where source_id = ?", (source_id,))

    def list_checkpoints(self) -> list[SourceCheckpoint]:
        with self._connect() as connection:
            rows = connection.execute(
                "select source_id, token, updated_at from checkpoints order by source_id"
            ).fetchall()
        return [SourceCheckpoint(source_id=row["source_id"], token=row["token"], updated_at=row["updated_at"]) for row in rows]

    def upsert_delivery(self, attempt: DeliveryAttempt) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                insert into deliveries (
                    request_id, trace_id, source_id, external_ref, tx_id, status, prepared_action_json, last_error, updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(request_id) do update set
                    trace_id = excluded.trace_id,
                    source_id = excluded.source_id,
                    external_ref = excluded.external_ref,
                    tx_id = excluded.tx_id,
                    status = excluded.status,
                    prepared_action_json = excluded.prepared_action_json,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    attempt.request_id,
                    attempt.trace_id,
                    attempt.source_id,
                    attempt.external_ref,
                    attempt.tx_id,
                    attempt.status,
                    json.dumps(attempt.prepared_action, sort_keys=True) if attempt.prepared_action else None,
                    attempt.last_error,
                    attempt.updated_at,
                ),
            )

    def list_deliveries(self, source_id: str | None = None) -> list[dict[str, str | None]]:
        with self._connect() as connection:
            if source_id:
                rows = connection.execute(
                    """
                    select request_id, trace_id, source_id, external_ref, tx_id, status, last_error, updated_at
                    from deliveries
                    where source_id = ?
                    order by updated_at desc
                    """,
                    (source_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    select request_id, trace_id, source_id, external_ref, tx_id, status, last_error, updated_at
                    from deliveries
                    order by updated_at desc
                    """
                ).fetchall()
        return [dict(row) for row in rows]

    def list_deliveries_by_external_ref(self, source_id: str, external_ref: str) -> list[dict[str, object | None]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                select request_id, trace_id, source_id, external_ref, tx_id, status, prepared_action_json, last_error, updated_at
                from deliveries
                where source_id = ? and external_ref = ?
                order by updated_at desc, request_id desc
                """,
                (source_id, external_ref),
            ).fetchall()
        deliveries: list[dict[str, object | None]] = []
        for row in rows:
            entry = dict(row)
            prepared_action_raw = entry.pop("prepared_action_json", None)
            entry["prepared_action"] = json.loads(prepared_action_raw) if prepared_action_raw else None
            deliveries.append(entry)
        return deliveries

    def prune_deliveries(self, source_id: str, retain: int) -> int:
        if retain <= 0:
            return 0
        with self._connect() as connection:
            rows = connection.execute(
                """
                select request_id
                from deliveries
                where source_id = ?
                order by updated_at desc, request_id desc
                limit -1 offset ?
                """,
                (source_id, retain),
            ).fetchall()
            removed = [str(row["request_id"]) for row in rows]
            if removed:
                connection.executemany(
                    "delete from deliveries where request_id = ?",
                    [(request_id,) for request_id in removed],
                )
        return len(removed)

    def upsert_proof(self, artifact: ProofArtifact) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                insert into proofs (
                    request_id, source_id, receipt_json, audit_chain_json, export_path, updated_at
                ) values (?, ?, ?, ?, ?, ?)
                on conflict(request_id) do update set
                    source_id = excluded.source_id,
                    receipt_json = excluded.receipt_json,
                    audit_chain_json = excluded.audit_chain_json,
                    export_path = excluded.export_path,
                    updated_at = excluded.updated_at
                """,
                (
                    artifact.request_id,
                    artifact.source_id,
                    json.dumps(artifact.receipt, sort_keys=True) if artifact.receipt else None,
                    json.dumps(artifact.audit_chain, sort_keys=True) if artifact.audit_chain else None,
                    artifact.export_path,
                    artifact.updated_at,
                ),
            )

    def get_proof(self, request_id: str) -> dict[str, str | None] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                select request_id, source_id, receipt_json, audit_chain_json, export_path, updated_at
                from proofs
                where request_id = ?
                """,
                (request_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def list_proofs(self, source_id: str | None = None) -> list[dict[str, str | None]]:
        with self._connect() as connection:
            if source_id:
                rows = connection.execute(
                    """
                    select request_id, source_id, export_path, updated_at
                    from proofs
                    where source_id = ?
                    order by updated_at desc
                    """,
                    (source_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    select request_id, source_id, export_path, updated_at
                    from proofs
                    order by updated_at desc
                    """
                ).fetchall()
        return [dict(row) for row in rows]

    def prune_proofs(self, source_id: str, retain: int) -> list[dict[str, str | None]]:
        if retain <= 0:
            return []
        with self._connect() as connection:
            rows = connection.execute(
                """
                select request_id, source_id, export_path, updated_at
                from proofs
                where source_id = ?
                order by updated_at desc, request_id desc
                limit -1 offset ?
                """,
                (source_id, retain),
            ).fetchall()
            removed = [dict(row) for row in rows]
            if removed:
                connection.executemany(
                    "delete from proofs where request_id = ?",
                    [(str(row["request_id"]),) for row in removed],
                )
        return removed

    def push_dlq(self, source_id: str, reason: str, payload: dict, created_at: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "insert into dlq (source_id, reason, payload_json, created_at) values (?, ?, ?, ?)",
                (source_id, reason, json.dumps(payload, sort_keys=True), created_at),
            )

    def list_dlq(self, source_id: str | None = None) -> list[dict[str, str]]:
        with self._connect() as connection:
            if source_id:
                rows = connection.execute(
                    "select id, source_id, reason, payload_json, created_at from dlq where source_id = ? order by id desc",
                    (source_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "select id, source_id, reason, payload_json, created_at from dlq order by id desc"
                ).fetchall()
        return [dict(row) for row in rows]

    def prune_dlq(self, source_id: str, retain: int) -> int:
        if retain <= 0:
            return 0
        with self._connect() as connection:
            rows = connection.execute(
                """
                select id
                from dlq
                where source_id = ?
                order by id desc
                limit -1 offset ?
                """,
                (source_id, retain),
            ).fetchall()
            removed = [int(row["id"]) for row in rows]
            if removed:
                connection.executemany(
                    "delete from dlq where id = ?",
                    [(row_id,) for row_id in removed],
                )
        return len(removed)

    def set_source_paused(self, source_id: str, paused: bool, updated_at: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                insert into source_controls (source_id, paused, updated_at)
                values (?, ?, ?)
                on conflict(source_id) do update set
                    paused = excluded.paused,
                    updated_at = excluded.updated_at
                """,
                (source_id, 1 if paused else 0, updated_at),
            )

    def is_source_paused(self, source_id: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "select paused from source_controls where source_id = ?",
                (source_id,),
            ).fetchone()
        return bool(row["paused"]) if row is not None else False

    def list_source_controls(self) -> list[dict[str, str | int]]:
        with self._connect() as connection:
            rows = connection.execute(
                "select source_id, paused, updated_at from source_controls order by source_id"
            ).fetchall()
        return [dict(row) for row in rows]

    def get_runtime_signature(self, source_id: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "select signature from source_runtime where source_id = ?",
                (source_id,),
            ).fetchone()
        return str(row["signature"]) if row is not None else None

    def set_runtime_signature(self, source_id: str, signature: str, updated_at: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                insert into source_runtime (source_id, signature, updated_at)
                values (?, ?, ?)
                on conflict(source_id) do update set
                    signature = excluded.signature,
                    updated_at = excluded.updated_at
                """,
                (source_id, signature, updated_at),
            )

    def list_runtime_signatures(self) -> list[dict[str, str]]:
        with self._connect() as connection:
            rows = connection.execute(
                "select source_id, signature, updated_at from source_runtime order by source_id"
            ).fetchall()
        return [dict(row) for row in rows]
