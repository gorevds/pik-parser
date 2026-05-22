"""Сканер не-PIK застройщиков → SQLite.

У ПИК свой сканер (bin/scan.py) c постранично-параллельным обходом. Прочие
застройщики отдают данные иначе (REST/GraphQL/HTML), но единообразно: каждый
модуль pik/sources/* возвращает CollectResult, который складывается в те же
таблицы blocks/flats/snapshots с глобально-уникальными id.

  python -m bin.scan_dev --db data/pik.db --developer "ГК ФСК"
  python -m bin.scan_dev --db data/pik.db --all
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from pik.blocks_meta import upsert_block_meta
from pik.sources import fsk
from pik.sources.base import CollectResult, SourceError, build_rows
from pik.store import apply_schema, upsert


MSK = timezone(timedelta(hours=3))

# Реестр источников: имя застройщика → функция обхода.
SOURCES: dict[str, Callable[[], CollectResult]] = {
    fsk.DEVELOPER: fsk.collect,
}


def run_developer(
    db_path: Path, developer: str, *, scan_date: str, scan_ts: str
) -> tuple[int, int]:
    """Обойти одного застройщика и записать результат. → (n_blocks, n_flats)."""
    log = logging.getLogger("pik.scan_dev")
    collect = SOURCES[developer]
    result = collect()
    block_payloads, flat_rows, snap_rows = build_rows(
        developer, result, scan_date=scan_date, scan_ts=scan_ts
    )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout=30000")
        apply_schema(conn)
        for bp in block_payloads:
            upsert_block_meta(
                conn, block_id=bp["block_id"], name=bp["name"],
                slug=bp["slug"], meta=bp["meta"], developer=bp["developer"],
                scan_ts=scan_ts,
            )
        upsert(conn, flats=flat_rows, snapshots=snap_rows)
    finally:
        conn.close()

    log.info("%s: записано %d ЖК, %d квартир", developer, len(block_payloads),
             len(flat_rows))
    return len(block_payloads), len(flat_rows)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
    )
    log = logging.getLogger("pik.scan_dev")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/pik.db", type=Path)
    parser.add_argument(
        "--developer",
        help=f"Один застройщик (доступно: {', '.join(SOURCES)})",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Обойти всех застройщиков из реестра",
    )
    args = parser.parse_args(argv)

    if args.all:
        developers = list(SOURCES)
    elif args.developer:
        if args.developer not in SOURCES:
            parser.error(
                f"неизвестный застройщик {args.developer!r}; "
                f"доступно: {', '.join(SOURCES)}"
            )
        developers = [args.developer]
    else:
        parser.error("укажите --developer NAME или --all")

    now = datetime.now(MSK)
    scan_date = now.strftime("%Y-%m-%d")
    scan_ts = now.isoformat(timespec="seconds")

    failed = 0
    for dev in developers:
        try:
            run_developer(args.db, dev, scan_date=scan_date, scan_ts=scan_ts)
        except SourceError as exc:
            log.error("%s: источник недоступен: %s", dev, exc)
            failed += 1
        except Exception:
            log.exception("%s: непредвиденный сбой", dev)
            failed += 1

    return 1 if failed * 2 > len(developers) else 0


if __name__ == "__main__":
    sys.exit(main())
