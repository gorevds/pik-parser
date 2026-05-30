"""Тесты предикатов data-quality (pik.quality)."""
from pik.quality import (
    GEO_MAX_KM,
    PRICE_MAX,
    PRICE_MIN,
    DataQualityStats,
    geo_ok,
    price_ok,
)
from pik.sources.base import CollectResult, NormBlock, NormFlat, build_rows


def _block(native_id=1, *, meta=None):
    return NormBlock(native_id=native_id, name="ЖК Тест", slug="test",
                     meta=meta or {"city": "msk"})


def _flat(native_id=10, *, block_id=1, price=12_000_000, area=40.0):
    return NormFlat(native_id=native_id, native_block_id=block_id,
                    rooms=1, area=area, price=price)


def _build(blocks, flats, stats=None):
    return build_rows("ПИК", CollectResult(blocks=blocks, flats=flats),
                      scan_date="2026-05-30", scan_ts="t", stats=stats)


def test_price_none_rejected():
    assert price_ok(None) is False


def test_price_zero_rejected():
    assert price_ok(0) is False


def test_price_negative_rejected():
    assert price_ok(-1) is False


def test_price_below_floor_rejected():
    assert price_ok(PRICE_MIN - 1) is False


def test_price_at_floor_ok():
    assert price_ok(PRICE_MIN) is True


def test_price_typical_ok():
    assert price_ok(12_000_000) is True


def test_price_at_ceiling_ok():
    assert price_ok(PRICE_MAX) is True


def test_price_above_ceiling_rejected():
    assert price_ok(PRICE_MAX + 1) is False


def test_geo_none_is_ok():
    # координат нет — судить не о чем, не отбраковываем
    assert geo_ok(None) is True


def test_geo_within_threshold_ok():
    assert geo_ok(119.7) is True


def test_geo_at_threshold_ok():
    assert geo_ok(GEO_MAX_KM) is True


def test_geo_beyond_threshold_rejected():
    assert geo_ok(GEO_MAX_KM + 0.1) is False


def test_geo_wrong_city_rejected():
    # ЖК «в Москве», но за 600+ км — классическая логическая ошибка
    assert geo_ok(627.0) is False


# --- интеграция в build_rows -------------------------------------------------


def test_build_rows_drops_zero_price_flat():
    _bp, flat_rows, snap_rows = _build([_block()], [_flat(price=0)])
    assert flat_rows == []
    assert snap_rows == []


def test_build_rows_drops_null_price_flat():
    _bp, flat_rows, _sr = _build([_block()], [_flat(price=None)])
    assert flat_rows == []


def test_build_rows_drops_absurdly_cheap_flat():
    _bp, flat_rows, _sr = _build([_block()], [_flat(price=PRICE_MIN - 1)])
    assert flat_rows == []


def test_build_rows_drops_absurdly_expensive_flat():
    _bp, flat_rows, _sr = _build([_block()], [_flat(price=PRICE_MAX + 1)])
    assert flat_rows == []


def test_build_rows_keeps_valid_flat():
    _bp, flat_rows, snap_rows = _build([_block()], [_flat(price=12_000_000)])
    assert len(flat_rows) == 1
    assert len(snap_rows) == 1


def test_build_rows_drops_geo_bad_block_and_its_flats():
    bad = _block(native_id=2, meta={"city": "msk", "distance_km": 627.0})
    block_payloads, flat_rows, _sr = _build([bad], [_flat(block_id=2)])
    # сам ЖК не записан
    assert block_payloads == []
    # и его квартиры не записаны
    assert flat_rows == []


def test_build_rows_keeps_block_without_coords():
    # distance_km None (нет координат) — ЖК остаётся
    block_payloads, flat_rows, _sr = _build([_block()], [_flat()])
    assert len(block_payloads) == 1
    assert len(flat_rows) == 1


def test_build_rows_fills_stats():
    blocks = [
        _block(native_id=1, meta={"city": "msk"}),
        _block(native_id=2, meta={"city": "msk", "distance_km": 627.0}),
    ]
    flats = [
        _flat(native_id=10, block_id=1, price=12_000_000),  # ok
        _flat(native_id=11, block_id=1, price=0),           # rejected price
        _flat(native_id=12, block_id=2, price=9_000_000),   # rejected geo
    ]
    stats = DataQualityStats()
    _bp, flat_rows, _sr = _build(blocks, flats, stats=stats)
    assert len(flat_rows) == 1
    assert stats.rejected_price == 1
    assert stats.rejected_geo == 1
    assert stats.geo_bad_blocks == 1
    assert stats.total_rejected_flats == 2
