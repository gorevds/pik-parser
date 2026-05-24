"""Сканер не-PIK застройщиков → SQLite.

У ПИК свой сканер (bin/scan.py) c постранично-параллельным обходом. Прочие
застройщики отдают данные иначе (REST/GraphQL/HTML), но единообразно: каждый
модуль pik/sources/* возвращает CollectResult, который складывается в те же
таблицы blocks/flats/snapshots с глобально-уникальными id.

  python -m bin.scan_dev --db data/pik.db --developer "ГК ФСК"
  python -m bin.scan_dev --db data/pik.db --all          # все застройщики параллельно
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from pik.blocks_meta import upsert_block_meta
from pik.developers import DEVELOPERS
from pik.sources import a101, absolut, donstroy, fsk, granel, ingrad, level, mrgroup
from pik.sources.base import CollectResult, SourceError, build_rows
from pik.store import apply_schema, upsert


MSK = timezone(timedelta(hours=3))

# Реестр источников: имя застройщика → функция обхода.
SOURCES: dict[str, Callable[[], CollectResult]] = {
    fsk.DEVELOPER: fsk.collect,
    donstroy.DEVELOPER: donstroy.collect,
    a101.DEVELOPER: a101.collect,
    level.DEVELOPER: level.collect,
    absolut.DEVELOPER: absolut.collect,
    mrgroup.DEVELOPER: mrgroup.collect,
    granel.DEVELOPER: granel.collect,
    ingrad.DEVELOPER: ingrad.collect,
}

# Имя каждого источника обязано быть в реестре pik.developers — иначе
# build_rows → namespaced_id упадёт лишь в проде. Ловим опечатку при импорте.
_UNKNOWN_SOURCES = set(SOURCES) - set(DEVELOPERS)
assert not _UNKNOWN_SOURCES, (
    f"источники вне реестра застройщиков pik.developers: {_UNKNOWN_SOURCES}"
)


def _ensure_schema(db_path: Path) -> None:
    """Создаёт/мигрирует схему один раз — до параллельной записи застройщиков.

    DDL под конкурентной записью лучше не гонять: применяем схему заранее,
    дальше потоки только пишут строки (WAL + busy_timeout это выдерживают).
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        apply_schema(conn)
    finally:
        conn.close()


def run_developer(
    db_path: Path, developer: str, *, scan_date: str, scan_ts: str
) -> tuple[int, int]:
    """Обойти одного застройщика и записать результат. → (n_blocks, n_flats).

    Схема должна быть уже применена (_ensure_schema). Соединение своё —
    функция безопасна для запуска в отдельном потоке.
    """
    log = logging.getLogger("pik.scan_dev")
    result = SOURCES[developer]()
    block_payloads, flat_rows, snap_rows = build_rows(
        developer, result, scan_date=scan_date, scan_ts=scan_ts
    )

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout=30000")
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


def run_sweep(
    db_path: Path, developers: list[str], *, scan_date: str, scan_ts: str,
    workers: int,
) -> int:
    """Параллельно обходит застройщиков. Возвращает число упавших источников."""
    log = logging.getLogger("pik.scan_dev")
    _ensure_schema(db_path)
    n_workers = max(1, min(workers, len(developers)))
    failed = 0
    started = time.monotonic()

    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = {
            ex.submit(run_developer, db_path, dev,
                      scan_date=scan_date, scan_ts=scan_ts): dev
            for dev in developers
        }
        for fut in as_completed(futures):
            dev = futures[fut]
            try:
                fut.result()
            except SourceError as exc:
                log.error("%s: источник недоступен: %s", dev, exc)
                failed += 1
            except Exception:
                log.exception("%s: непредвиденный сбой", dev)
                failed += 1

    elapsed = time.monotonic() - started
    log.info("обход завершён: %d застройщик(ов), %d воркер(ов), %d сбой(ев), "
             "за %.0f с", len(developers), n_workers, failed, elapsed)
    return failed


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/pik.db", type=Path)
    parser.add_argument(
        "--developer",
        help=f"Один застройщик (доступно: {', '.join(SOURCES)})",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Обойти всех застройщиков из реестра (параллельно)",
    )
    parser.add_argument(
        "--workers", type=int, default=6,
        help="Число параллельных воркеров для --all (default: 6)",
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
    failed = run_sweep(
        args.db, developers,
        scan_date=now.strftime("%Y-%m-%d"),
        scan_ts=now.isoformat(timespec="seconds"),
        workers=args.workers,
    )
    # Источников всего ~6, и сбой одного — это потеря данных по целому
    # застройщику за день. В отличие от bin/scan.py (десятки ЖК ПИК, где
    # один флапнувший — не беда) здесь любой сбой обязан пометить юнит failed.
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
