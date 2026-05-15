from importlib.resources import files
from typing import Iterable
import sqlite3


_SNAPSHOTS_NEW_COLS = (
    ("base_meter_price", "INTEGER"),
    ("promo_price",      "INTEGER"),
    ("discount_pct",     "REAL"),
    ("has_promo",        "INTEGER NOT NULL DEFAULT 0"),
)


def _migrate_snapshots(conn: sqlite3.Connection) -> None:
    """Добавляет недостающие колонки в snapshots для БД, созданных до 0.2.0."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(snapshots)")}
    if not existing:
        return  # таблицы ещё нет — schema.sql сейчас её создаст
    for col, ddl in _SNAPSHOTS_NEW_COLS:
        if col not in existing:
            conn.execute(f"ALTER TABLE snapshots ADD COLUMN {col} {ddl}")


def apply_schema(conn: sqlite3.Connection) -> None:
    _migrate_snapshots(conn)
    sql = files("pik").joinpath("schema.sql").read_text(encoding="utf-8")
    conn.executescript(sql)
    conn.commit()


_FLAT_COLS = (
    "id", "guid", "block_id", "bulk_id", "section_id", "layout_id",
    "bulk_name", "section_no", "floor", "rooms", "rooms_fact", "is_studio",
    "area", "area_kitchen", "area_living", "number", "name", "url",
    "pdf_url", "plan_url", "ceiling_height", "settlement_date", "first_seen",
)
_SNAP_COLS = (
    "flat_id", "scan_date", "scan_ts", "status",
    "price", "meter_price", "base_meter_price", "promo_price",
    "discount_pct", "has_promo",
    "old_price", "discount", "finish",
    "mortgage_min_rate", "mortgage_best_name", "updated_at",
)


def _insert_sql(table: str, cols: tuple[str, ...], on_conflict_do: str) -> str:
    placeholders = ", ".join(f":{c}" for c in cols)
    return (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT DO UPDATE SET {on_conflict_do}"
    )


_FLATS_INSERT = _insert_sql(
    "flats",
    _FLAT_COLS,
    ", ".join(f"{c}=excluded.{c}" for c in _FLAT_COLS if c not in ("id", "first_seen")),
)
_SNAP_INSERT = _insert_sql(
    "snapshots",
    _SNAP_COLS,
    ", ".join(f"{c}=excluded.{c}" for c in _SNAP_COLS if c not in ("flat_id", "scan_date")),
)


def upsert(
    conn: sqlite3.Connection,
    *,
    flats: Iterable[dict],
    snapshots: Iterable[dict],
) -> None:
    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        cur.executemany(_FLATS_INSERT, list(flats))
        cur.executemany(_SNAP_INSERT, list(snapshots))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
