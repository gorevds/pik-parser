"""Слияние БД от агентов / других источников в основную через ATTACH + UPSERT.

Использование:
    from pik.merge import merge_databases
    merge_databases(main_path="data/pik.db", source_paths=["/tmp/pik_a.db", ...])
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Iterable

from .store import _FLAT_COLS, _SNAP_COLS, _FLATS_INSERT, _SNAP_INSERT, apply_schema


log = logging.getLogger("pik.merge")


def merge_databases(
    *, main_path: str | Path, source_paths: Iterable[str | Path]
) -> dict[str, dict]:
    """ATTACH каждую source-БД, апсёртит её flats+snapshots в main."""
    main_path = Path(main_path)
    main_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(main_path)
    conn.execute("PRAGMA foreign_keys = ON")
    apply_schema(conn)

    flat_select = f"SELECT {', '.join(_FLAT_COLS)} FROM src.flats"
    snap_select = f"SELECT {', '.join(_SNAP_COLS)} FROM src.snapshots"
    flat_dict = ", ".join(_FLAT_COLS)
    snap_dict = ", ".join(_SNAP_COLS)

    summary: dict[str, dict] = {}
    for src in source_paths:
        src = Path(src)
        if not src.exists():
            log.warning("source %s not found, skipping", src)
            continue
        conn.execute(f"ATTACH DATABASE '{src}' AS src")
        try:
            cur = conn.cursor()
            cur.execute("BEGIN")
            try:
                # Превратим row → dict для использования existing INSERT строк
                conn.row_factory = sqlite3.Row
                src_cur = conn.cursor()

                src_cur.execute(flat_select)
                flat_rows = [dict(r) for r in src_cur.fetchall()]
                cur.executemany(_FLATS_INSERT, flat_rows)

                src_cur.execute(snap_select)
                snap_rows = [dict(r) for r in src_cur.fetchall()]
                cur.executemany(_SNAP_INSERT, snap_rows)

                conn.commit()
                conn.row_factory = None

                # post-merge stats
                summary[str(src)] = {
                    "flats_in_source": len(flat_rows),
                    "snapshots_in_source": len(snap_rows),
                }
                log.info(
                    "merged %s: +%d flats, +%d snapshots",
                    src.name, len(flat_rows), len(snap_rows),
                )
            except Exception:
                conn.rollback()
                raise
        finally:
            conn.execute("DETACH DATABASE src")
    conn.close()
    return summary
