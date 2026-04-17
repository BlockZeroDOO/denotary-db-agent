from __future__ import annotations

import select
import struct
import time
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.pq import ExecStatus


@dataclass
class PostgresReplicationMessage:
    kind: str
    wal_start_lsn: str
    wal_end_lsn: str
    payload: bytes | None
    reply_requested: bool = False


class PostgresReplicationSession:
    def __init__(
        self,
        conninfo: str,
        slot_name: str,
        publication_name: str,
        start_lsn: str = "0/0",
    ) -> None:
        self.conninfo = conninfo
        self.slot_name = slot_name
        self.publication_name = publication_name
        self.start_lsn = start_lsn or "0/0"
        self._connection: Any | None = None
        self._pgconn: Any | None = None

    def __enter__(self) -> "PostgresReplicationSession":
        self._connection = psycopg.connect(f"{self.conninfo} replication=database", autocommit=True)
        self._pgconn = self._connection.pgconn
        command = (
            f"START_REPLICATION SLOT {self.slot_name} LOGICAL {self.start_lsn} "
            f"(proto_version '1', publication_names '{self.publication_name}')"
        )
        self._pgconn.send_query(command.encode("utf-8"))
        socket_fd = self._pgconn.socket
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            remaining = max(0.0, deadline - time.monotonic())
            ready, _, _ = select.select([socket_fd], [], [], remaining)
            if not ready:
                break
            self._pgconn.consume_input()
            if not self._pgconn.is_busy():
                result = self._pgconn.get_result()
                if result is None:
                    break
                if result.status != ExecStatus.COPY_BOTH:
                    raise RuntimeError(f"postgres replication did not enter COPY_BOTH mode: {result.status!r}")
                return self
        raise RuntimeError("postgres replication session did not reach COPY_BOTH mode in time")

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._connection is not None:
            self._connection.close()
        self._connection = None
        self._pgconn = None

    def read_messages(
        self,
        *,
        max_messages: int,
        total_timeout_sec: float,
        idle_timeout_sec: float,
    ) -> list[PostgresReplicationMessage]:
        if self._pgconn is None:
            raise RuntimeError("replication session is not open")
        messages: list[PostgresReplicationMessage] = []
        socket_fd = self._pgconn.socket
        deadline = time.monotonic() + max(total_timeout_sec, 0.0)
        idle_deadline = deadline

        while len(messages) < max_messages and time.monotonic() < deadline:
            wait_deadline = min(deadline, idle_deadline)
            timeout = max(0.0, wait_deadline - time.monotonic())
            ready, _, _ = select.select([socket_fd], [], [], timeout)
            if not ready:
                break
            self._pgconn.consume_input()
            while len(messages) < max_messages:
                nbytes, data = self._pgconn.get_copy_data(1)
                if nbytes == 0:
                    break
                if nbytes < 0:
                    return messages
                idle_deadline = time.monotonic() + max(idle_timeout_sec, 0.0)
                messages.append(parse_copy_data_frame(bytes(data)))
        return messages


def parse_copy_data_frame(data: bytes) -> PostgresReplicationMessage:
    if not data:
        raise ValueError("postgres replication frame is empty")
    tag = chr(data[0])
    cursor = 1
    if tag == "w":
        wal_start = _format_pg_lsn(struct.unpack_from("!Q", data, cursor)[0])
        cursor += 8
        wal_end = _format_pg_lsn(struct.unpack_from("!Q", data, cursor)[0])
        cursor += 8
        cursor += 8  # server timestamp
        return PostgresReplicationMessage(
            kind="xlogdata",
            wal_start_lsn=wal_start,
            wal_end_lsn=wal_end,
            payload=data[cursor:],
        )
    if tag == "k":
        wal_end = _format_pg_lsn(struct.unpack_from("!Q", data, cursor)[0])
        cursor += 8
        cursor += 8  # server timestamp
        reply_requested = bool(struct.unpack_from("!B", data, cursor)[0])
        return PostgresReplicationMessage(
            kind="keepalive",
            wal_start_lsn="",
            wal_end_lsn=wal_end,
            payload=None,
            reply_requested=reply_requested,
        )
    raise ValueError(f"unsupported postgres replication frame tag: {tag!r}")


def _format_pg_lsn(value: int) -> str:
    upper = value >> 32
    lower = value & 0xFFFFFFFF
    return f"{upper:X}/{lower:X}"
