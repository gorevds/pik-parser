"""Слияние БД от агентов / других источников в основную через ATTACH + UPSERT.

Использование:
    from pik.merge import merge_databases
    merge_databases(main_path="data/pik.db", source_paths=["/tmp/pik_a.db", ...])
"""
from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .blocks_meta import _BLOCK_META_COLS, upsert_block_meta
from .store import (
    _FLAT_COLS,
    _FLAT_DEFAULTS,
    _FLATS_INSERT,
    _SNAP_COLS,
    _SNAP_DEFAULTS,
    _SNAP_INSERT,
    apply_schema,
)

_MSK = timezone(timedelta(hours=3))


log = logging.getLogger("pik.merge")


def _src_table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    """Колонки таблицы из ATTACH'нутой src БД. Пусто если таблицы нет."""
    return {row[1] for row in conn.execute(f"PRAGMA src.table_info({table})")}


def merge_databases(
    *, main_path: str | Path, source_paths: Iterable[str | Path]
) -> dict[str, dict]:
    """ATTACH каждую source-БД, апсёртит её blocks+flats+snapshots в main.

    Толерантно к легаси-схемам: если у источника нет какой-то колонки
    (is_apartment появилась только в 2026-05-25, developer/city — в 2026-05-22…23),
    она подставляется DEFAULT'ом из миграции. Без этого мерж старой БД ронялся
    с OperationalError: no such column.
    """
    main_path = Path(main_path)
    main_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(main_path)
    conn.execute("PRAGMA foreign_keys = ON")
    apply_schema(conn)

    summary: dict[str, dict] = {}
    for src in source_paths:
        src = Path(src)
        if not src.exists():
            log.warning("source %s not found, skipping", src)
            continue
        # Путь — связанный параметр (не f-string): защита от кавычек/инъекции.
        conn.execute("ATTACH DATABASE ? AS src", (str(src),))
        try:
            # 1. Blocks — без них квартиры осиротеют (build_rows/today_all
            #    отнесли бы их к 'ПИК' через COALESCE). Сначала перенос blocks
            #    одной транзакцией: либо вся группа блоков из этого источника
            #    приземлилась, либо ничего (откатываемся). upsert_block_meta(
            #    commit=False) разрешает батчинг.
            blocks_cols_src = _src_table_cols(conn, "blocks")
            n_blocks = 0
            if blocks_cols_src:
                meta_in_src = [c for c in _BLOCK_META_COLS if c in blocks_cols_src]
                dev_col = "developer" if "developer" in blocks_cols_src else "'ПИК' AS developer"
                cols = ["id", "name", dev_col, "slug"] + meta_in_src
                cur = conn.cursor()
                cur.execute(f"SELECT {', '.join(cols)} FROM src.blocks")
                rows = cur.fetchall()
                merge_ts = datetime.now(_MSK).isoformat(timespec="seconds")
                col_names = ["id", "name", "developer", "slug"] + meta_in_src
                cur.execute("BEGIN")
                try:
                    for r in rows:
                        rec = dict(zip(col_names, r, strict=False))
                        meta = {c: rec.get(c) for c in meta_in_src}
                        upsert_block_meta(
                            conn, block_id=rec["id"], name=rec["name"],
                            slug=rec.get("slug"), meta=meta,
                            developer=rec.get("developer") or "ПИК",
                            scan_ts=merge_ts, commit=False,
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                n_blocks = len(rows)

            cur = conn.cursor()
            cur.execute("BEGIN")
            try:
                # 2. Flats — берём только колонки, реально присутствующие у
                #    источника. Недостающие (is_apartment в БД до 2026-05-25
                #    отсутствует) дозаполняем дефолтами _FLAT_DEFAULTS, иначе
                #    executemany с :named-параметрами падает.
                flats_cols_src = _src_table_cols(conn, "flats")
                if not flats_cols_src:
                    flat_rows = []
                else:
                    cols_to_pull = [c for c in _FLAT_COLS if c in flats_cols_src]
                    cur.execute(
                        f"SELECT {', '.join(cols_to_pull)} FROM src.flats"
                    )
                    flat_rows = [dict(zip(cols_to_pull, r, strict=False)) for r in cur.fetchall()]
                    for row in flat_rows:
                        # 1) недостающие колонки — None (либо дефолт для NOT NULL)
                        for col in _FLAT_COLS:
                            row.setdefault(col, _FLAT_DEFAULTS.get(col))
                        # 2) явный NULL для NOT NULL DEFAULT-колонок → подставляем
                        #    дефолт (legacy src имел колонку, но с NULL-значением)
                        for col, dflt in _FLAT_DEFAULTS.items():
                            if row.get(col) is None:
                                row[col] = dflt
                cur.executemany(_FLATS_INSERT, flat_rows)

                # 3. Snapshots — аналогично, дополняем недостающие колонки
                #    None. promo_price/discount_pct и пр. появились в 0.2.0
                #    и в очень старых БД отсутствуют.
                snaps_cols_src = _src_table_cols(conn, "snapshots")
                if not snaps_cols_src:
                    snap_rows = []
                else:
                    cols_to_pull = [c for c in _SNAP_COLS if c in snaps_cols_src]
                    cur.execute(
                        f"SELECT {', '.join(cols_to_pull)} FROM src.snapshots"
                    )
                    snap_rows = [dict(zip(cols_to_pull, r, strict=False)) for r in cur.fetchall()]
                    for row in snap_rows:
                        for col in _SNAP_COLS:
                            row.setdefault(col, _SNAP_DEFAULTS.get(col))
                        for col, dflt in _SNAP_DEFAULTS.items():
                            if row.get(col) is None:
                                row[col] = dflt
                cur.executemany(_SNAP_INSERT, snap_rows)

                conn.commit()

                summary[str(src)] = {
                    "blocks_in_source": n_blocks,
                    "flats_in_source": len(flat_rows),
                    "snapshots_in_source": len(snap_rows),
                }
                log.info(
                    "merged %s: +%d blocks, +%d flats, +%d snapshots",
                    src.name, n_blocks, len(flat_rows), len(snap_rows),
                )
            except Exception:
                conn.rollback()
                raise
        finally:
            conn.execute("DETACH DATABASE src")
    conn.close()
    return summary
