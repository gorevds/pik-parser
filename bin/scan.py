"""Однопроходный сканер: api.pik.ru → SQLite."""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from pik.client import PikClient, PikApiError
from pik.mapping import to_flat_row, to_snapshot_row
from pik.store import apply_schema, upsert


MSK = timezone(timedelta(hours=3))
NARVIN_BLOCK_ID = 1165


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
    )


def run_once(db_path: Path, block_id: int = NARVIN_BLOCK_ID) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("pik.scan")
    client = PikClient()

    now = datetime.now(MSK)
    scan_date = now.strftime("%Y-%m-%d")
    scan_ts = now.isoformat(timespec="seconds")

    log.info("scanning block_id=%s scan_date=%s", block_id, scan_date)
    items = client.fetch_block_flats(block_id=block_id, types=(1,))
    log.info("api returned %d items", len(items))

    flats = [to_flat_row(it, first_seen=scan_date) for it in items]
    snaps = [to_snapshot_row(it, scan_date=scan_date, scan_ts=scan_ts) for it in items]

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        apply_schema(conn)
        upsert(conn, flats=flats, snapshots=snaps)
        one_room = conn.execute(
            "SELECT COUNT(*) FROM flats f JOIN snapshots s ON s.flat_id=f.id "
            "WHERE s.scan_date=? AND f.rooms='1'",
            (scan_date,),
        ).fetchone()[0]

    log.info("stored %d flats; 1-room на витрине: %d", len(items), one_room)
    return one_room


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default="data/pik.db",
        type=Path,
        help="Path to SQLite DB (default: data/pik.db)",
    )
    parser.add_argument(
        "--block-id",
        default=NARVIN_BLOCK_ID,
        type=int,
        help="PIK block id (default: 1165 for Narvin)",
    )
    args = parser.parse_args(argv)
    try:
        run_once(args.db, args.block_id)
    except PikApiError as exc:
        logging.error("PIK API error: %s", exc)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
