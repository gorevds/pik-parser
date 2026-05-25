"""Тесты Sprint 2 — материализация view'ов / observability / прочее."""
from __future__ import annotations

import sqlite3
import time

from pik.store import apply_schema, refresh_materialized, upsert

SAMPLE_F = {
    "id": 100, "guid": "g-100", "block_id": 1165, "bulk_id": 10397,
    "section_id": 23643, "layout_id": 60971, "bulk_name": "Корпус 1.3",
    "section_no": 3, "floor": 7, "rooms": "1", "rooms_fact": 1, "is_studio": 0,
    "area": 33.5, "area_kitchen": 8.0, "area_living": 16.2, "number": "1",
    "name": "kv1", "url": "u", "pdf_url": None, "plan_url": None,
    "ceiling_height": 2.75, "settlement_date": "2027-10-31",
    "first_seen": "2026-05-15",
}
SAMPLE_S = {
    "flat_id": 100, "scan_date": "2026-05-25", "scan_ts": "2026-05-25T06:00",
    "status": "free", "price": 12_000_000, "meter_price": 358_209,
    "base_meter_price": 358_209, "promo_price": 12_000_000,
    "discount_pct": 0.0, "has_promo": 0, "old_price": None, "discount": 0,
    "finish": "X", "mortgage_min_rate": 6.0, "mortgage_best_name": "Y",
    "updated_at": "t",
}


def _types_for(conn, name):
    """Текущий type объекта (table/view/None)."""
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name=?", (name,)
    ).fetchone()
    return row[0] if row else None


def test_apply_schema_creates_views():
    """Fresh DB: today_all/today_one_room/flat_sparkline_30d должны быть VIEW."""
    c = sqlite3.connect(":memory:")
    apply_schema(c)
    assert _types_for(c, "today_all") == "view"
    assert _types_for(c, "today_one_room") == "view"
    assert _types_for(c, "flat_sparkline_30d") == "view"


def test_refresh_swaps_view_to_table_and_keeps_data():
    """refresh_materialized превращает view в table с теми же строками."""
    c = sqlite3.connect(":memory:")
    apply_schema(c)
    upsert(c, flats=[SAMPLE_F], snapshots=[SAMPLE_S])
    # view-форма
    rows_via_view = c.execute("SELECT id, базовая_цена FROM today_all").fetchall()
    assert len(rows_via_view) == 1

    refresh_materialized(c)
    assert _types_for(c, "today_all") == "table"
    assert _types_for(c, "today_one_room") == "table"
    assert _types_for(c, "flat_sparkline_30d") == "table"

    rows_via_table = c.execute("SELECT id, базовая_цена FROM today_all").fetchall()
    assert rows_via_table == rows_via_view


def test_refresh_is_idempotent_when_already_materialized():
    """Второй вызов подряд должен сработать (table → table, не table → view)."""
    c = sqlite3.connect(":memory:")
    apply_schema(c)
    upsert(c, flats=[SAMPLE_F], snapshots=[SAMPLE_S])
    refresh_materialized(c)
    refresh_materialized(c)   # не должно упасть
    assert _types_for(c, "today_all") == "table"


def test_apply_schema_after_refresh_recreates_views():
    """apply_schema на БД с уже материализованными table должен дропнуть
    table и вернуть view (важно для start-of-scan сценария)."""
    c = sqlite3.connect(":memory:")
    apply_schema(c)
    upsert(c, flats=[SAMPLE_F], snapshots=[SAMPLE_S])
    refresh_materialized(c)
    assert _types_for(c, "today_all") == "table"

    apply_schema(c)
    assert _types_for(c, "today_all") == "view"


def test_refresh_handles_today_one_room_dependency():
    """today_one_room SELECT'ит из today_all. refresh должен правильно
    обработать порядок: today_all сначала table, потом today_one_room
    из этой table."""
    c = sqlite3.connect(":memory:")
    apply_schema(c)
    upsert(c, flats=[SAMPLE_F], snapshots=[SAMPLE_S])
    refresh_materialized(c)
    rows = c.execute("SELECT COUNT(*) FROM today_one_room").fetchone()
    assert rows[0] == 1  # 1-к квартира


def test_materialized_is_faster_than_view_for_typical_query():
    """Sanity-check: materialized table < view query (порядки величины)."""
    c = sqlite3.connect(":memory:")
    apply_schema(c)
    # 100 квартир х 10 дней — небольшой набор, но достаточный для замера
    flats = []
    snaps = []
    for i in range(100):
        f = dict(SAMPLE_F, id=i + 1000, guid=f"g{i}")
        flats.append(f)
        for d in range(10):
            snaps.append(dict(
                SAMPLE_S, flat_id=i + 1000,
                scan_date=f"2026-05-{15 + d:02d}",
                price=10_000_000 + i * 1000 + d * 100,
            ))
    upsert(c, flats=flats, snapshots=snaps)

    t0 = time.perf_counter()
    for _ in range(10):
        c.execute("SELECT COUNT(*), AVG(базовая_цена) FROM today_all").fetchone()
    view_t = time.perf_counter() - t0

    refresh_materialized(c)

    t0 = time.perf_counter()
    for _ in range(10):
        c.execute("SELECT COUNT(*), AVG(базовая_цена) FROM today_all").fetchone()
    table_t = time.perf_counter() - t0

    # Не делаем strict ratio (на ничтожном датасете шум), но table должен
    # быть НЕ медленнее view'а заметно. В прод-датасете 41k+ квартир
    # ускорение порядка 10-50x.
    assert table_t <= view_t * 1.5, (
        f"Материализованная таблица не должна быть медленнее view: "
        f"view={view_t:.4f}s table={table_t:.4f}s"
    )
