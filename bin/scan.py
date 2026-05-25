"""DEPRECATED shim: оставлен для обратной совместимости с pik-scan.service'ом
старых деплоев и привычных command-line вызовов. Сразу делегирует в
`bin.scan_dev`, который теперь — единая точка входа для всех 10
застройщиков, включая ПИК.

ЛОГИКА:
  python -m bin.scan --all-blocks            → bin.scan_dev --developer "ПИК"
  python -m bin.scan --block-id 1165         → bin.scan_dev --developer "ПИК"
                                                (block_id PIK адаптер берёт
                                                 из таблицы blocks)
  python -m bin.scan --workers N             → передаётся как --workers N

После 2026-05-25 (R1+R2 рефактора) PIK-парсинг живёт в pik/sources/pik.py
и driven from bin/scan_dev. Этот файл нужен только для не-сломать
существующий pik-scan.service до его обновления через install.sh.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from bin import scan_dev


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
    )
    log = logging.getLogger("pik.scan")
    log.warning(
        "bin.scan — DEPRECATED. Используйте `python -m bin.scan_dev "
        "--developer 'ПИК'` напрямую. См. docs/refactor-de-pik-plan.md."
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/pik.db", type=Path)
    parser.add_argument(
        "--block-id", default=os.environ.get("PIK_BLOCK_ID", "1165"),
        help="Не используется в shim-режиме — список блоков PIK адаптер "
             "берёт из таблицы blocks. Параметр оставлен для совместимости.",
    )
    parser.add_argument("--all-blocks", action="store_true",
                        help="Игнорируется: PIK всегда сканирует все известные блоки.")
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args(argv)

    return scan_dev.main([
        "--db", str(args.db),
        "--developer", "ПИК",
        "--workers", str(args.workers),
    ])


if __name__ == "__main__":
    sys.exit(main())
