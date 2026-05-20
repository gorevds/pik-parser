"""Wayback Machine → SQLite. Ретро-история цен для любого ЖК PIK.

Один ЖК: --slug/--block-id. Все сразу: --all-blocks обходит каждый ЖК с
известным slug из таблицы blocks, фетч идёт параллельно (--workers).
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from pik.backfill_wayback import backfill


def _blocks_with_slug(db_path: Path) -> list[tuple[int, str]]:
    """(block_id, slug) для всех ЖК с непустым slug — источник для --all-blocks."""
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, slug FROM blocks "
            "WHERE slug IS NOT NULL AND TRIM(slug) != '' ORDER BY id"
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _run_all_blocks(
    db_path: Path,
    blocks: list[tuple[int, str]],
    *,
    from_date: str,
    to_date: str,
    sleep: float,
    workers: int,
) -> int:
    """Параллельный бэкфилл по списку (block_id, slug). Возвращает число упавших ЖК.

    Каждый backfill() сам открывает соединение и пишет короткой транзакцией;
    WAL + busy_timeout делают одновременные записи безопасными.
    """
    log = logging.getLogger("pik.backfill")
    n_workers = max(1, min(workers, len(blocks)))
    totals = {"snapshots": 0, "unique_flats": 0, "errors": 0}
    ok = bad = 0
    log.info("backfill all-blocks: %d block(s), %d worker(s)", len(blocks), n_workers)

    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = {
            ex.submit(
                backfill, db_path, slug=slug, block_id=bid,
                from_yyyymmdd=from_date, to_yyyymmdd=to_date, sleep_sec=sleep,
            ): (bid, slug)
            for bid, slug in blocks
        }
        for fut in as_completed(futures):
            bid, slug = futures[fut]
            try:
                st = fut.result()
            except Exception:
                log.exception("backfill failed for block %d (%s)", bid, slug)
                bad += 1
                continue
            ok += 1
            for key in totals:
                totals[key] += st[key]
            log.info(
                "block %d (%s): snapshots=%d unique_flats=%d dates=%d errors=%d",
                bid, slug, st["snapshots"], st["unique_flats"],
                st["dates"], st["errors"],
            )

    log.info(
        "backfill all-blocks done: %d ok, %d failed; "
        "total snapshots=%d unique_flats=%d errors=%d",
        ok, bad, totals["snapshots"], totals["unique_flats"], totals["errors"],
    )
    return bad


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/pik.db", type=Path)
    parser.add_argument(
        "--slug",
        default=os.environ.get("PIK_SLUG", "narvin"),
        help="URL-сегмент ЖК на pik.ru (default: $PIK_SLUG or 'narvin')",
    )
    parser.add_argument(
        "--block-id",
        type=int,
        default=int(os.environ.get("PIK_BLOCK_ID", 1165)),
        help="PIK block id (default: $PIK_BLOCK_ID or 1165 for Narvin)",
    )
    parser.add_argument(
        "--all-blocks",
        action="store_true",
        help="Backfill every block with a known slug (ignores --slug/--block-id)",
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Parallel backfill workers for --all-blocks (default: 4)",
    )
    parser.add_argument("--from", dest="from_date", default="20250601")
    parser.add_argument("--to", dest="to_date", default="20260601")
    parser.add_argument("--sleep", type=float, default=1.5,
                        help="пауза между запросами к Wayback, секунды")
    args = parser.parse_args(argv)

    if args.all_blocks:
        blocks = _blocks_with_slug(args.db)
        if not blocks:
            parser.error("--all-blocks: no blocks with a slug in DB yet")
        bad = _run_all_blocks(
            args.db, blocks, from_date=args.from_date, to_date=args.to_date,
            sleep=args.sleep, workers=args.workers,
        )
        return 1 if bad * 2 > len(blocks) else 0

    stats = backfill(
        args.db,
        slug=args.slug,
        block_id=args.block_id,
        from_yyyymmdd=args.from_date,
        to_yyyymmdd=args.to_date,
        sleep_sec=args.sleep,
    )
    print(
        f"OK: slug={args.slug} block={args.block_id} "
        f"snapshots={stats['snapshots']} unique_flats={stats['unique_flats']} "
        f"dates={stats['dates']} errors={stats['errors']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
