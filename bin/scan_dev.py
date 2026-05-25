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
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pik.blocks_meta import upsert_block_meta
from pik.developers import DEVELOPERS
from pik.sources import a101, absolut, brusnika, donstroy, fsk, granel, ingrad, level, mrgroup
from pik.sources.base import CollectResult, SourceError, build_rows
from pik.store import apply_schema, record_scan_run, refresh_materialized, upsert

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
    brusnika.DEVELOPER: brusnika.collect,
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
    функция безопасна для запуска в отдельном потоке. Завершение пишет
    запись в scan_runs независимо от исхода (success/exception).
    """
    log = logging.getLogger("pik.scan_dev")
    started = time.monotonic()
    err_msg = None
    n_blocks = n_flats = 0
    try:
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
        n_blocks, n_flats = len(block_payloads), len(flat_rows)
        log.info("%s: записано %d ЖК, %d квартир", developer, n_blocks, n_flats)
    except Exception as exc:
        # СОХРАНИМ запись scan_runs со статусом error, дальше пробросим.
        # На уровне run_sweep исключение уже логируется и считается в failed,
        # но без scan_runs мы не могли бы потом ответить «какой именно
        # застройщик умер и когда» из БД (без journalctl).
        err_msg = f"{type(exc).__name__}: {exc}"[:500]
        raise
    finally:
        duration = round(time.monotonic() - started, 1)
        # Отдельное соединение для лога — main connection может быть в bad state
        # (поэтому-то мы и логируем тут, в finally).
        try:
            log_conn = sqlite3.connect(db_path)
            log_conn.execute("PRAGMA busy_timeout=30000")
            record_scan_run(
                log_conn, developer=developer,
                scan_date=scan_date, scan_ts=scan_ts,
                n_blocks=n_blocks, n_flats=n_flats, duration_s=duration,
                status=("ok" if err_msg is None else "error"),
                error_msg=err_msg,
            )
            log_conn.close()
        except Exception:
            log.exception("%s: не удалось записать scan_runs", developer)
    return n_blocks, n_flats


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

    # Материализуем view'и сразу после параллельных upsert'ов — теперь, когда
    # все 9 застройщиков отписались, можно построить today_all одним проходом.
    # scan_dev стартует ИЗ pik-scan.service через OnSuccess, т.е. этот refresh
    # — последний шаг ночного цикла. До 2026-05-25 эти view'и считались на
    # каждый GET от Datasette и грузили БД на 3-5с/запрос.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        refresh_materialized(conn)
    finally:
        conn.close()

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
    # При 10+ источниках 1 флапнувший (типично Инград или MR Group с
    # ServicePipe) — не повод пометить юнит failed: 9 из 10 успешно
    # = вчерашний снимок остаётся, аналитика не страдает. Но 20%+
    # сбоев = действительно что-то не так с сетью/прокси, ловим.
    threshold = max(1, len(developers) // 5)
    return 1 if failed > threshold else 0


if __name__ == "__main__":
    sys.exit(main())
