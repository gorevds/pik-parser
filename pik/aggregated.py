"""Импорт записей history_aggregated из JSON (агрегаты из Cian/mskguru/новости/…)."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable


_COLS = (
    "block_id", "date", "source", "source_url", "rooms",
    "price_min", "price_max", "price_avg",
    "meter_price_min", "meter_price_max", "meter_price_avg",
    "notes",
)

_INSERT = (
    f"INSERT INTO history_aggregated ({', '.join(_COLS)}) "
    f"VALUES ({', '.join(':' + c for c in _COLS)}) "
    f"ON CONFLICT (block_id, date, source, rooms) DO UPDATE SET "
    + ", ".join(
        f"{c}=excluded.{c}"
        for c in _COLS
        if c not in ("block_id", "date", "source", "rooms")
    )
)


def normalize_record(rec: dict, *, block_id: int) -> dict:
    """JSON record → строка для вставки. rooms NULL → 'all', integer-кастинг."""
    def _int(v):
        if v is None or v == "":
            return None
        try:
            return int(round(float(v)))
        except (TypeError, ValueError):
            return None

    rooms = rec.get("rooms")
    if rooms in (None, "", "null"):
        rooms = "all"

    return {
        "block_id": block_id,
        "date": rec["date"],
        "source": rec["source"],
        "source_url": rec.get("source_url"),
        "rooms": str(rooms),
        "price_min": _int(rec.get("price_min")),
        "price_max": _int(rec.get("price_max")),
        "price_avg": _int(rec.get("price_avg")),
        "meter_price_min": _int(rec.get("meter_price_min")),
        "meter_price_max": _int(rec.get("meter_price_max")),
        "meter_price_avg": _int(rec.get("meter_price_avg")),
        "notes": rec.get("notes"),
    }


def import_records(
    conn: sqlite3.Connection, *, records: Iterable[dict], block_id: int
) -> int:
    rows = [normalize_record(r, block_id=block_id) for r in records]
    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        # Авто-создаём заглушку блока, если его ещё нет (для standalone-импорта)
        cur.execute(
            "INSERT INTO blocks (id, name) VALUES (?, ?) ON CONFLICT(id) DO NOTHING",
            (block_id, f"block {block_id}"),
        )
        cur.executemany(_INSERT, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(rows)


def import_file(conn: sqlite3.Connection, *, path: Path, block_id: int) -> int:
    data = json.loads(Path(path).read_text("utf-8"))
    return import_records(conn, records=data, block_id=block_id)
