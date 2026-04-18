from __future__ import annotations

import argparse
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg


@dataclass
class LoadStats:
    invoices_inserted: int = 0
    payments_inserted: int = 0
    invoices_updated: int = 0
    payments_updated: int = 0
    payments_deleted: int = 0
    burst_transactions: int = 0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate PostgreSQL soak load for denotary-db-agent.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=55432)
    parser.add_argument("--user", default="denotary")
    parser.add_argument("--password", default="denotarypw")
    parser.add_argument("--database", default="ledger")
    parser.add_argument("--duration-sec", type=int, default=14400, help="How long to run the soak load.")
    parser.add_argument("--interval-sec", type=float, default=1.0, help="Base interval between normal iterations.")
    parser.add_argument("--burst-every", type=int, default=30, help="Run one multi-row burst every N iterations.")
    parser.add_argument("--burst-size", type=int, default=10, help="Rows to generate inside one burst transaction.")
    parser.add_argument("--delete-every", type=int, default=5, help="Delete one recent payment every N iterations.")
    parser.add_argument("--id-base", type=int, default=int(time.time()), help="Base integer for generated ids.")
    parser.add_argument("--jitter-sec", type=float, default=0.25, help="Random jitter added to the sleep interval.")
    return parser


def next_amount() -> float:
    return round(random.uniform(10.0, 5000.0), 2)


def insert_invoice(cursor: psycopg.Cursor, invoice_id: int) -> None:
    cursor.execute(
        """
        insert into public.invoices (id, status, amount, updated_at)
        values (%s, %s, %s, %s)
        """,
        (invoice_id, "issued", next_amount(), utc_now()),
    )


def insert_payment(cursor: psycopg.Cursor, payment_id: int, invoice_id: int) -> None:
    cursor.execute(
        """
        insert into public.payments (id, invoice_id, amount, updated_at)
        values (%s, %s, %s, %s)
        """,
        (payment_id, invoice_id, next_amount(), utc_now()),
    )


def update_recent_invoice(cursor: psycopg.Cursor) -> bool:
    cursor.execute(
        """
        select id
        from public.invoices
        order by updated_at desc, id desc
        limit 1
        """
    )
    row = cursor.fetchone()
    if not row:
        return False
    cursor.execute(
        """
        update public.invoices
        set status = %s,
            amount = %s,
            updated_at = %s
        where id = %s
        """,
        (random.choice(["issued", "paid", "settled", "review"]), next_amount(), utc_now(), row[0]),
    )
    return True


def update_recent_payment(cursor: psycopg.Cursor) -> bool:
    cursor.execute(
        """
        select id
        from public.payments
        order by updated_at desc, id desc
        limit 1
        """
    )
    row = cursor.fetchone()
    if not row:
        return False
    cursor.execute(
        """
        update public.payments
        set amount = %s,
            updated_at = %s
        where id = %s
        """,
        (next_amount(), utc_now(), row[0]),
    )
    return True


def delete_recent_payment(cursor: psycopg.Cursor) -> bool:
    cursor.execute(
        """
        select id
        from public.payments
        order by updated_at desc, id desc
        limit 1
        """
    )
    row = cursor.fetchone()
    if not row:
        return False
    cursor.execute("delete from public.payments where id = %s", (row[0],))
    return cursor.rowcount > 0


def run_burst(cursor: psycopg.Cursor, invoice_id_start: int, payment_id_start: int, burst_size: int) -> tuple[int, int]:
    for offset in range(burst_size):
        invoice_id = invoice_id_start + offset
        payment_id = payment_id_start + offset
        insert_invoice(cursor, invoice_id)
        insert_payment(cursor, payment_id, invoice_id)
    return invoice_id_start + burst_size, payment_id_start + burst_size


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    invoice_id = args.id_base * 10
    payment_id = args.id_base * 20
    stats = LoadStats()
    start = time.time()
    iteration = 0

    with psycopg.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.database,
        autocommit=False,
    ) as connection:
        print(
            f"postgres soak load started host={args.host} port={args.port} db={args.database} "
            f"duration_sec={args.duration_sec} interval_sec={args.interval_sec}"
        )
        while time.time() - start < args.duration_sec:
            iteration += 1
            with connection.cursor() as cursor:
                if args.burst_every > 0 and iteration % args.burst_every == 0:
                    invoice_id, payment_id = run_burst(cursor, invoice_id, payment_id, args.burst_size)
                    stats.invoices_inserted += args.burst_size
                    stats.payments_inserted += args.burst_size
                    stats.burst_transactions += 1
                else:
                    insert_invoice(cursor, invoice_id)
                    stats.invoices_inserted += 1
                    if iteration % 2 == 0:
                        insert_payment(cursor, payment_id, invoice_id)
                        stats.payments_inserted += 1
                        payment_id += 1
                    invoice_id += 1

                if update_recent_invoice(cursor):
                    stats.invoices_updated += 1
                if iteration % 3 == 0 and update_recent_payment(cursor):
                    stats.payments_updated += 1
                if args.delete_every > 0 and iteration % args.delete_every == 0 and delete_recent_payment(cursor):
                    stats.payments_deleted += 1

            connection.commit()

            if iteration % 25 == 0:
                elapsed = round(time.time() - start, 1)
                print(
                    f"iteration={iteration} elapsed_sec={elapsed} "
                    f"invoices_inserted={stats.invoices_inserted} payments_inserted={stats.payments_inserted} "
                    f"invoices_updated={stats.invoices_updated} payments_updated={stats.payments_updated} "
                    f"payments_deleted={stats.payments_deleted} burst_transactions={stats.burst_transactions}"
                )

            sleep_for = max(0.0, args.interval_sec + random.uniform(0.0, args.jitter_sec))
            time.sleep(sleep_for)

    elapsed = round(time.time() - start, 1)
    print("postgres soak load finished")
    print(
        f"elapsed_sec={elapsed} iterations={iteration} invoices_inserted={stats.invoices_inserted} "
        f"payments_inserted={stats.payments_inserted} invoices_updated={stats.invoices_updated} "
        f"payments_updated={stats.payments_updated} payments_deleted={stats.payments_deleted} "
        f"burst_transactions={stats.burst_transactions}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
