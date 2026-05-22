"""Тесты нормализованного слоя источников и застройщиков."""
import json
import sqlite3
from pathlib import Path

import pytest

from pik.developers import ID_NAMESPACE, namespaced_id
from pik.sources import fsk
from pik.sources.base import (
    CollectResult,
    NormBlock,
    NormFlat,
    _detect_discount,
    build_rows,
    to_global_id,
)
from pik.store import apply_schema

FIXTURES = Path(__file__).parent / "fixtures"


# --- base.to_global_id --------------------------------------------------

def test_to_global_id_numeric_native():
    gid = to_global_id("ГК ФСК", 138935)
    assert gid == namespaced_id("ГК ФСК", 138935)


def test_to_global_id_numeric_string():
    assert to_global_id("ГК ФСК", "138935") == to_global_id("ГК ФСК", 138935)


def test_to_global_id_non_numeric_is_hashed_and_in_range():
    gid = to_global_id("Донстрой", "10-39-1-260139017")
    dev_offset = gid // ID_NAMESPACE
    assert dev_offset == 3  # Донстрой
    assert 0 <= gid % ID_NAMESPACE < ID_NAMESPACE


def test_to_global_id_developers_never_collide():
    gids = {to_global_id(d, 5) for d in ("ПИК", "ГК ФСК", "Донстрой", "А101")}
    assert len(gids) == 4


# --- base._detect_discount ---------------------------------------------

def test_detect_discount_none_when_no_old_price():
    assert _detect_discount(10_000_000, None) == (0, 0.0, 0)


def test_detect_discount_none_when_old_not_higher():
    assert _detect_discount(10_000_000, 10_000_000) == (0, 0.0, 0)


def test_detect_discount_computes_percent_and_flag():
    abs_d, pct, promo = _detect_discount(8_000_000, 10_000_000)
    assert abs_d == 2_000_000
    assert pct == 20.0
    assert promo == 1


# --- base.build_rows ----------------------------------------------------

def test_build_rows_links_flat_to_block_and_namespaces_ids():
    result = CollectResult(
        blocks=[NormBlock(native_id="zhk-a", name="ЖК А", slug="zhk-a")],
        flats=[NormFlat(
            native_id=555, native_block_id="zhk-a", rooms=2, area=50.0,
            floor=7, price=10_000_000, meter_price=200_000, old_price=12_000_000,
            status="free", number="12",
        )],
    )
    blocks, flats, snaps = build_rows(
        "ГК ФСК", result, scan_date="2026-05-22", scan_ts="2026-05-22T06:00:00+03:00"
    )
    assert blocks[0]["developer"] == "ГК ФСК"
    assert flats[0]["id"] == namespaced_id("ГК ФСК", 555)
    # flat.block_id совпадает с id зарегистрированного блока
    assert flats[0]["block_id"] == blocks[0]["block_id"]
    assert flats[0]["rooms"] == "2"
    assert flats[0]["is_studio"] == 0
    assert snaps[0]["discount_pct"] == 16.67
    assert snaps[0]["has_promo"] == 1
    assert snaps[0]["base_meter_price"] == 200_000


def test_build_rows_marks_studio():
    result = CollectResult(
        blocks=[],
        flats=[NormFlat(native_id=1, native_block_id="b", rooms=0, area=25.0,
                        floor=3, price=5_000_000)],
    )
    _, flats, _ = build_rows("А101", result, scan_date="d", scan_ts="t")
    assert flats[0]["rooms"] == "studio"
    assert flats[0]["is_studio"] == 1


def test_build_rows_computes_meter_price_when_missing():
    result = CollectResult(
        blocks=[],
        flats=[NormFlat(native_id=1, native_block_id="b", rooms=1, area=40.0,
                        floor=2, price=8_000_000, meter_price=None)],
    )
    _, _, snaps = build_rows("Level", result, scan_date="d", scan_ts="t")
    assert snaps[0]["meter_price"] == 200_000


# --- fsk ----------------------------------------------------------------

def test_fsk_finish_label():
    assert fsk._finish_label({"finishing": 0}) == "Без отделки"
    assert fsk._finish_label({"finishing": 1}) == "С отделкой"
    assert fsk._finish_label({"finishing": 1, "furniture": True}) == "С отделкой и мебелью"


def test_fsk_to_norm_maps_real_fixture():
    fl = json.load(open(FIXTURES / "fsk_flat.json"))[0]
    norm = fsk._to_norm(fl, "arhitektor")
    assert norm.native_id == "138935"  # externalId
    assert norm.native_block_id == "arhitektor"
    assert norm.price == 120_332_800
    assert norm.meter_price == 940_100
    assert norm.area == 128
    assert norm.rooms == 6
    assert norm.floor == 27
    assert norm.status == "free"  # status 0
    assert norm.old_price is None  # priceWoDiscount == price


def test_fsk_collect_with_mocked_api(monkeypatch):
    fixture_flat = json.load(open(FIXTURES / "fsk_flat.json"))[0]

    def fake_request_json(session, method, url, **kw):
        if url.endswith("/complex/"):
            return [
                {"slug": "arhitektor", "title": "Архитектор", "city_id": 1,
                 "lat": 55.7, "lng": 37.6, "flats": {"all": 1}},
                {"slug": "kaluga-18", "title": "Молодежный", "city_id": 3,
                 "flats": {"all": 5}},  # другой регион — игнор
                {"slug": "sold-out", "title": "Распродан", "city_id": 1,
                 "flats": {"all": 0}},  # нет квартир — пропуск
            ]
        return [fixture_flat]

    monkeypatch.setattr("pik.sources.fsk.request_json", fake_request_json)
    result = fsk.collect()
    assert len(result.blocks) == 1
    assert result.blocks[0].slug == "arhitektor"
    assert len(result.flats) == 1
    assert result.flats[0].native_id == "138935"


def test_fsk_rows_apply_to_schema(monkeypatch):
    """Сквозной тест: ФСК-данные ложатся в реальную схему без ошибок FK."""
    fixture_flat = json.load(open(FIXTURES / "fsk_flat.json"))[0]
    result = CollectResult(
        blocks=[NormBlock(native_id="arhitektor", name="Архитектор", slug="arhitektor")],
        flats=[fsk._to_norm(fixture_flat, "arhitektor")],
    )
    blocks, flats, snaps = build_rows(
        fsk.DEVELOPER, result, scan_date="2026-05-22", scan_ts="t"
    )
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    apply_schema(conn)
    from pik.blocks_meta import upsert_block_meta
    from pik.store import upsert
    for bp in blocks:
        upsert_block_meta(conn, block_id=bp["block_id"], name=bp["name"],
                          slug=bp["slug"], meta=bp["meta"],
                          developer=bp["developer"], scan_ts="t")
    upsert(conn, flats=flats, snapshots=snaps)
    row = conn.execute(
        "SELECT застройщик, жк FROM today_all WHERE block_id=?",
        (blocks[0]["block_id"],),
    ).fetchone()
    assert row == ("ГК ФСК", "Архитектор")
    conn.close()


# --- donstroy -----------------------------------------------------------

from pik.sources import donstroy  # noqa: E402


def test_donstroy_slug_from_link():
    assert donstroy._slug_from_link("/objects/simvol/plans/quarter7/") == "simvol"
    assert donstroy._slug_from_link(None) is None
    assert donstroy._slug_from_link("/other/path/") is None


def test_donstroy_to_float_handles_comma_and_junk():
    assert donstroy._to_float("28.4") == 28.4
    assert donstroy._to_float("28,4") == 28.4
    assert donstroy._to_float(None) is None
    assert donstroy._to_float("") is None


def test_donstroy_to_norm_maps_real_fixture():
    fl = json.load(open(FIXTURES / "donstroy_flats.json"))[0]
    norm = donstroy._to_norm(fl)
    assert norm.native_id == "10-39-1-260139017"
    assert norm.native_block_id == "Символ"
    assert norm.rooms == 0  # студия
    assert norm.area == 28.4
    assert norm.price == 19_125_696
    assert norm.old_price == 20_788_800  # price_old > price
    assert norm.bulk_name == "Корпус 39"
    assert norm.section_no == 1
    assert norm.url == "https://donstroy.moscow/objects/simvol/plans/" \
        "quarter7/korpus39/section1/floor4/flat260139017/"


def test_donstroy_to_norm_hides_price_on_request():
    norm = donstroy._to_norm({"id": "x", "project": "P", "price": 999,
                              "price_request": True})
    assert norm.price is None


def test_donstroy_collect_paginates_and_dedups_blocks(monkeypatch):
    fixture = json.load(open(FIXTURES / "donstroy_flats.json"))
    full = fixture[:1] * donstroy._PAGE_SIZE  # полная страница → пагинация продолжится
    pages = {1: full, 2: fixture[:1]}         # стр.2 короткая → стоп

    def fake_request_json(session, method, url, *, json=None, **kw):
        return {"flats": pages.get(json["page"], [])}

    monkeypatch.setattr("pik.sources.donstroy.request_json", fake_request_json)
    result = donstroy.collect()
    # стр.1 — 12 квартир (полная), стр.2 — 1 (короткая, стоп) → 13 квартир
    assert len(result.flats) == donstroy._PAGE_SIZE + 1
    # один ЖК на все карточки — блок без дублей
    assert {b.name for b in result.blocks} == {"Символ"}
