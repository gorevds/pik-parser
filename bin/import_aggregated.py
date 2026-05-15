"""CLI: импорт history_aggregated из JSON-файла (Cian, mskguru, новости и т.п.)."""
import argparse
import logging
import sqlite3
import sys
from pathlib import Path

from pik.aggregated import import_file
from pik.store import apply_schema


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", required=True, type=Path)
    p.add_argument("--block-id", required=True, type=int,
                   help="К какому ЖК относятся эти агрегаты (например, 1165 для Нарвина)")
    p.add_argument("json", type=Path, help="JSON-файл от research-агента")
    args = p.parse_args(argv)

    args.db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(args.db) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        apply_schema(conn)
        n = import_file(conn, path=args.json, block_id=args.block_id)
    print(f"OK: imported {n} aggregated records for block_id={args.block_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
