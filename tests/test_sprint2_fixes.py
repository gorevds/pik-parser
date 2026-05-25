"""Тесты Sprint 2 — материализация view'ов / observability / прочее.
И рефактор-тесты: PIK как source, эквивалентность через build_rows."""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from pik.mapping import to_flat_row, to_snapshot_row
from pik.sources import pik as pik_source
from pik.sources.base import build_rows
from pik.store import apply_schema, refresh_materialized, upsert

FIXTURES = Path(__file__).parent / "fixtures"

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


# ====================== R1 — PIK как peer-source ======================


def test_pik_norm_flat_extracts_all_fields():
    """PIK API item → NormFlat: все ключевые поля переносятся, включая
    PIK-specific (mortgage_rate, ceiling_height, promo_price)."""
    item = json.loads((FIXTURES / "sample_flat.json").read_text("utf-8"))
    nf = pik_source._norm_flat(item)
    assert nf.native_id == item["id"]
    assert nf.native_block_id == item["block_id"]
    assert nf.rooms == item["rooms"]
    assert nf.area == item["area"]
    assert nf.floor == item["floor"]
    assert nf.price == item["price"]
    assert nf.meter_price == item["meterPrice"]
    assert nf.url == item["url"]
    assert nf.pdf_url == item["pdf"]
    # mortgage от _best_mortgage: фикстура содержит «Семейная ипотека 6%» как
    # isMain. _parse_rate в plausibility-range [0.5, 35] её принимает.
    assert nf.mortgage_min_rate == 6.0
    assert "Семейная" in (nf.mortgage_best_name or "")
    # PIK adapter ВСЕГДА считает promo_price (даже если discount=0): это
    # round(meter_price * area) = round(524800 * 35.2) = 18_473_960. У
    # тестовой квартиры promo == price ± округление, has_promo = 0.
    assert nf.promo_price is not None
    assert nf.discount_pct is not None


def test_pik_source_build_rows_produces_same_promo_as_legacy():
    """Регрессия: PIK → NormFlat → build_rows должен дать тот же promo_price/
    discount_pct/has_promo, что и старый to_snapshot_row напрямую.

    Если эта инвариантность ломается, материализованный today_all начнёт
    показывать другие цены чем production привык, и это сразу будет видно
    в инвест-аналитике.
    """
    item = json.loads((FIXTURES / "sample_flat.json").read_text("utf-8"))
    # Legacy path: pik.mapping.to_snapshot_row
    legacy = to_snapshot_row(item, scan_date="2026-05-25", scan_ts="t")
    # New path: NormFlat → build_rows
    from pik.sources.base import CollectResult, NormBlock
    nb = NormBlock(native_id=item["block_id"], name="Test", slug="test")
    nf = pik_source._norm_flat(item)
    result = CollectResult(blocks=[nb], flats=[nf])
    _bp, _fr, snap_rows = build_rows("ПИК", result, scan_date="2026-05-25", scan_ts="t")
    new = snap_rows[0]
    # Ключевые поля промо-семантики — должны совпадать (разница на ±1 от
    # округления допустима).
    assert new["price"] == legacy["price"]
    assert new["meter_price"] == legacy["meter_price"]
    assert new["base_meter_price"] == legacy["base_meter_price"]
    assert abs((new["promo_price"] or 0) - (legacy["promo_price"] or 0)) <= 1
    if legacy["discount_pct"] is not None:
        assert abs((new["discount_pct"] or 0) - legacy["discount_pct"]) < 0.1
    assert new["has_promo"] == legacy["has_promo"]
    assert new["mortgage_min_rate"] == legacy["mortgage_min_rate"]
    assert new["mortgage_best_name"] == legacy["mortgage_best_name"]


def test_pik_source_build_rows_flat_columns_match_legacy():
    """flat_row тоже должен совпадать с to_flat_row по основным полям."""
    item = json.loads((FIXTURES / "sample_flat.json").read_text("utf-8"))
    legacy = to_flat_row(item, first_seen="2026-05-25")
    from pik.sources.base import CollectResult, NormBlock
    nb = NormBlock(native_id=item["block_id"], name="Test", slug="test")
    nf = pik_source._norm_flat(item)
    result = CollectResult(blocks=[nb], flats=[nf])
    _bp, flat_rows, _sr = build_rows("ПИК", result, scan_date="2026-05-25", scan_ts="t")
    new = flat_rows[0]
    for col in ("id", "block_id", "floor", "area",
                "area_kitchen", "area_living", "ceiling_height",
                "url", "pdf_url", "plan_url", "settlement_date",
                "bulk_name", "section_no"):
        assert new[col] == legacy[col], (
            f"PIK adapter regression on `{col}`: new={new[col]!r} "
            f"legacy={legacy[col]!r}"
        )
    # rooms: legacy кладёт строку "1"; новый путь даёт rooms=1 (int → "1")
    assert new["rooms"] == legacy["rooms"]


def test_pik_collect_with_no_block_ids_returns_empty():
    """Защитный кейс: пустой list блоков → пустой CollectResult, без HTTP-обращений."""
    result = pik_source.collect(block_ids=[])
    assert result.blocks == []
    assert result.flats == []


def test_pik_collect_aggregates_blocks_in_parallel(monkeypatch):
    """Параллельный фетч через ThreadPool: _fetch_one замокан, всё свернётся
    в собранный CollectResult."""
    item = json.loads((FIXTURES / "sample_flat.json").read_text("utf-8"))
    # _fetch_one(bid, types) → items (block_id хранится в futures-dict caller'а)
    monkeypatch.setattr(
        pik_source, "_fetch_one",
        lambda bid, types: [dict(item, block_id=bid, id=item["id"] + bid)],
    )
    result = pik_source.collect(block_ids=[1165, 1166, 1167], workers=2)
    assert len(result.blocks) == 3
    assert len(result.flats) == 3
    block_ids = {b.native_id for b in result.blocks}
    assert block_ids == {1165, 1166, 1167}


def test_pik_source_promo_math_for_studio_with_discount():
    """Регрессия для PROMO-ветки: studio с meter_price < price → discount > 0.

    Базовый regression test использует sample_flat без скидки. Этот синтез
    проверяет promo-семантику (`promo_price = round(meter_price * area)`,
    `discount_pct = (price - promo_price) / price * 100`) на сценарии где
    цифры действительно дают has_promo=1.
    """
    base = json.loads((FIXTURES / "sample_flat.json").read_text("utf-8"))
    # Студия с явной 12% скидкой: price=10M base, meter_price дисконтированный
    item = dict(base, id=999001, rooms=0, is_studio=1,
                area=25.0, price=10_000_000, meterPrice=352_000)
    # promo_price = round(352_000 * 25.0) = 8_800_000
    # discount_pct = (10_000_000 - 8_800_000) / 10_000_000 * 100 = 12.0
    legacy = to_snapshot_row(item, scan_date="2026-05-25", scan_ts="t")
    from pik.sources.base import CollectResult, NormBlock
    nb = NormBlock(native_id=item["block_id"], name="Test", slug="test")
    nf = pik_source._norm_flat(item)
    _bp, _fr, snap_rows = build_rows("ПИК", CollectResult(blocks=[nb], flats=[nf]),
                                     scan_date="2026-05-25", scan_ts="t")
    new = snap_rows[0]
    assert new["promo_price"] == 8_800_000
    assert legacy["promo_price"] == 8_800_000
    assert new["has_promo"] == 1
    assert legacy["has_promo"] == 1
    assert abs(new["discount_pct"] - 12.0) < 0.1
    assert abs(legacy["discount_pct"] - 12.0) < 0.1


def test_pik_source_promo_math_for_two_room_no_discount():
    """Регрессия для NO-PROMO ветки на крупной квартире: meter_price * area ≈ price."""
    base = json.loads((FIXTURES / "sample_flat.json").read_text("utf-8"))
    item = dict(base, id=999002, rooms=2,
                area=65.0, price=24_700_000, meterPrice=380_000)
    # promo_price = round(380_000 * 65) = 24_700_000 (== price) → has_promo=0
    legacy = to_snapshot_row(item, scan_date="2026-05-25", scan_ts="t")
    from pik.sources.base import CollectResult, NormBlock
    nb = NormBlock(native_id=item["block_id"], name="Test", slug="test")
    nf = pik_source._norm_flat(item)
    _bp, _fr, snap_rows = build_rows("ПИК", CollectResult(blocks=[nb], flats=[nf]),
                                     scan_date="2026-05-25", scan_ts="t")
    new = snap_rows[0]
    assert new["promo_price"] == legacy["promo_price"]
    assert new["has_promo"] == 0 == legacy["has_promo"]
