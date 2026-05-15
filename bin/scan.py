"""Однопроходный сканер: api.pik.ru → SQLite.

По умолчанию сканирует ЖК Нарвин (block_id=1165). Любой другой блок —
через --block-id или env-var PIK_BLOCK_ID.
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from pik.client import PikClient, PikApiError
from pik.mapping import to_flat_row, to_snapshot_row
from pik.store import apply_schema, upsert
from pik.geo import extract_block_meta
from pik.blocks_meta import upsert_block_meta


MSK = timezone(timedelta(hours=3))
DEFAULT_BLOCK_ID = int(os.environ.get("PIK_BLOCK_ID", 1165))  # Нарвин


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
    )


def run_once(db_path: Path, block_id: int) -> int:
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

    # Имя ЖК + slug + гео-метаданные берём из первого item
    block_name = None
    block_slug = None
    block_meta = {}
    if items:
        b = items[0].get("block")
        if isinstance(b, dict):
            block_name = b.get("name")
            block_slug = (b.get("url") or "").strip("/") or None
        block_meta = extract_block_meta(items[0], slug=block_slug)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        apply_schema(conn)
        upsert(conn, flats=flats, snapshots=snaps)
        if block_name:
            upsert_block_meta(
                conn, block_id=block_id, name=block_name,
                slug=block_slug, meta=block_meta, scan_ts=scan_ts,
            )
        one_room = conn.execute(
            "SELECT COUNT(*) FROM flats f JOIN snapshots s ON s.flat_id=f.id "
            "WHERE s.scan_date=? AND f.rooms='1' AND f.block_id=?",
            (scan_date, block_id),
        ).fetchone()[0]

    log.info("stored %d flats for block %d (%s); 1-room: %d",
             len(items), block_id, block_name or "?", one_room)
    return one_room


def _parse_block_ids(raw: str) -> list[int]:
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


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
        default=str(DEFAULT_BLOCK_ID),
        help=(
            "PIK block id, comma-separated for multiple "
            "(default: $PIK_BLOCK_ID or 1165 for Narvin)"
        ),
    )
    args = parser.parse_args(argv)
    block_ids = _parse_block_ids(args.block_id)
    if not block_ids:
        parser.error("--block-id must contain at least one id")
    rc = 0
    for bid in block_ids:
        try:
            run_once(args.db, bid)
        except PikApiError as exc:
            logging.error("PIK API error for block %d: %s", bid, exc)
            rc = 2
    return rc


if __name__ == "__main__":
    sys.exit(main())
