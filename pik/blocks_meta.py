"""Запись расширенных метаданных ЖК в таблицу blocks."""
from __future__ import annotations

import sqlite3
from typing import Optional


_BLOCK_META_COLS = (
    "metro_name", "metro_line_name", "metro_line_type",
    "metro_time_foot", "metro_time_transport",
    "latitude", "longitude", "address",
    "distance_km", "floors_max",
)


def upsert_block_meta(
    conn: sqlite3.Connection, *,
    block_id: int, name: str, slug: Optional[str], meta: dict, scan_ts: str,
) -> None:
    """Upsert одной записи blocks с полной мета-информацией."""
    set_clause = ", ".join(f"{c}=excluded.{c}" for c in _BLOCK_META_COLS)
    cols = ("id", "name", "slug", "updated_at") + _BLOCK_META_COLS
    placeholders = ", ".join("?" for _ in cols)
    values = [block_id, name, slug, scan_ts] + [meta.get(c) for c in _BLOCK_META_COLS]
    sql = (
        f"INSERT INTO blocks ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET name=excluded.name, slug=COALESCE(excluded.slug, blocks.slug), "
        f"updated_at=excluded.updated_at, {set_clause}"
    )
    conn.execute(sql, values)
    conn.commit()
