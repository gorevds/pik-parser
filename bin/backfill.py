"""Wayback Machine → SQLite. Ретро-история цен Narvin."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pik.backfill_wayback import backfill


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/pik.db", type=Path)
    parser.add_argument("--from", dest="from_date", default="20250601",
                        help="YYYYMMDD, ниж. граница CDX (default 20250601)")
    parser.add_argument("--to", dest="to_date", default="20260601",
                        help="YYYYMMDD, верх. граница CDX (default 20260601)")
    parser.add_argument("--sleep", type=float, default=1.5,
                        help="пауза между запросами к Wayback, секунды")
    args = parser.parse_args(argv)

    stats = backfill(
        args.db,
        from_yyyymmdd=args.from_date,
        to_yyyymmdd=args.to_date,
        sleep_sec=args.sleep,
    )
    print(f"OK: snapshots={stats['snapshots']} unique_flats={stats['unique_flats']} "
          f"dates={stats['dates']} errors={stats['errors']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
