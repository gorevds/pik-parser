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

    summary: dict[str, dict] = {}
    for src in source_paths:
        src = Path(src)
        if not src.exists():
            log.warning("source %s not found, skipping", src)
            continue
        # Путь — связанный параметр (не f-string): защита от кавычек/инъекции.
        conn.execute("ATTACH DATABASE ? AS src", (str(src),))
        try:
            cur = conn.cursor()
            cur.execute("BEGIN")
            try:
                # tuple → dict через известный порядок колонок: не трогаем
                # conn.row_factory (его мутация на всё соединение хрупка).
                cur.execute(flat_select)
                flat_rows = [dict(zip(_FLAT_COLS, r)) for r in cur.fetchall()]
                cur.executemany(_FLATS_INSERT, flat_rows)

                cur.execute(snap_select)
                snap_rows = [dict(zip(_SNAP_COLS, r)) for r in cur.fetchall()]
                cur.executemany(_SNAP_INSERT, snap_rows)

                conn.commit()

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
