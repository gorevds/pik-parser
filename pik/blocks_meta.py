"""Запись расширенных метаданных ЖК в таблицу blocks."""
from __future__ import annotations

import sqlite3
from typing import Optional


_BLOCK_META_COLS = (
    "metro_name", "metro_line_name", "metro_line_type",
    "metro_time_foot", "metro_time_transport",
    "latitude", "longitude", "address", "city",
    "distance_km", "floors_max",
)


def upsert_block_meta(
    conn: sqlite3.Connection, *,
    block_id: int, name: str, slug: Optional[str], meta: dict, scan_ts: str,
    developer: str = "ПИК",
) -> None:
    """Upsert одной записи blocks с полной мета-информацией.

    Для каждого meta-поля используем COALESCE(excluded, blocks): если источник
    в текущем скане НЕ отдал поле (частичный фетч, /api/projects/ упал, …),
    мы НЕ затираем уже накопленные metro/coords/address значением NULL.
    Источники по контракту перезаписывают только то, что реально достали.
    `developer`/`name`/`slug` всегда известны на момент upsert — затираем
    обычным excluded.X (slug всё равно через COALESCE, был исторически).

    `developer` по умолчанию 'ПИК' — обратная совместимость с PIK-сканером.
    """
    set_clause = ", ".join(
        f"{c}=COALESCE(excluded.{c}, blocks.{c})" for c in _BLOCK_META_COLS
    )
    cols = ("id", "name", "developer", "slug", "updated_at") + _BLOCK_META_COLS
    placeholders = ", ".join("?" for _ in cols)
    values = [block_id, name, developer, slug, scan_ts] + [
        meta.get(c) for c in _BLOCK_META_COLS
    ]
    sql = (
        f"INSERT INTO blocks ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET name=excluded.name, "
        f"developer=excluded.developer, "
        f"slug=COALESCE(excluded.slug, blocks.slug), "
        f"updated_at=excluded.updated_at, {set_clause}"
    )
    conn.execute(sql, values)
    conn.commit()
