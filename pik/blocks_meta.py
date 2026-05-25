"""Запись расширенных метаданных ЖК в таблицу blocks."""
from __future__ import annotations

import sqlite3

_BLOCK_META_COLS = (
    "metro_name", "metro_line_name", "metro_line_type",
    "metro_time_foot", "metro_time_transport",
    "latitude", "longitude", "address", "city",
    "distance_km", "floors_max",
)


def upsert_block_meta(
    conn: sqlite3.Connection, *,
    block_id: int, name: str, developer: str,
    slug: str | None, meta: dict, scan_ts: str,
    commit: bool = True,
) -> None:
    """Upsert одной записи blocks с полной мета-информацией.

    Для каждого meta-поля используем COALESCE(excluded, blocks): если источник
    в текущем скане НЕ отдал поле (частичный фетч, /api/projects/ упал, …),
    мы НЕ затираем уже накопленные metro/coords/address значением NULL.
    Источники по контракту перезаписывают только то, что реально достали.

    Trade-off: НЕТ способа явно очистить metro_name через эту функцию.
    Если источник захочет «забыть» старое значение (станция переименована,
    блок снесён), нужно отдельный DELETE/UPDATE. Сейчас ни один из 10
    источников такого сценария не порождает.

    `developer` — ОБЯЗАТЕЛЕН с R4 рефактора (2026-05-25). Дефолт 'ПИК' был
    тех-долгом, скрывавшим опечатки в новых источниках (квартиры тихо
    приписывались к ПИК и сбивали аналитику по застройщикам). Теперь все
    callers передают явно — опечатка ловится при typecheck/импорте, не в
    проде.

    `commit=False` позволяет вызывающему батчить N upsert'ов в одну транзакцию
    (merge.py, будущий рефактор scan_dev). Снижает writer contention в WAL
    под параллельной нагрузкой и даёт атомарность блоковому батчу.
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
    if commit:
        conn.commit()
