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


def test_to_global_id_rehashes_out_of_range_numeric(caplog):
    """Числовой id вне [0, ID_NAMESPACE) хешируется — и это логируется."""
    from pik.developers import split_id

    huge = ID_NAMESPACE + 7
    with caplog.at_level("WARNING"):
        gid = to_global_id("Донстрой", huge)
    assert "вне" in caplog.text  # замена не прошла молча
    dev, native = split_id(gid)
    assert dev == "Донстрой"
    assert 0 <= native < ID_NAMESPACE


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
        blocks=[NormBlock(native_id="b", name="ЖК Б", slug="b")],
        flats=[NormFlat(native_id=1, native_block_id="b", rooms=0, area=25.0,
                        floor=3, price=5_000_000)],
    )
    _, flats, _ = build_rows("А101", result, scan_date="d", scan_ts="t")
    assert flats[0]["rooms"] == "studio"
    assert flats[0]["is_studio"] == 1


def test_build_rows_computes_meter_price_when_missing():
    result = CollectResult(
        blocks=[NormBlock(native_id="b", name="ЖК Б", slug="b")],
        flats=[NormFlat(native_id=1, native_block_id="b", rooms=1, area=40.0,
                        floor=2, price=8_000_000, meter_price=None)],
    )
    _, _, snaps = build_rows("Level", result, scan_date="d", scan_ts="t")
    assert snaps[0]["meter_price"] == 200_000


def test_build_rows_passes_plan_url_through_to_flats():
    result = CollectResult(
        blocks=[NormBlock(native_id="b", name="ЖК", slug="b")],
        flats=[NormFlat(native_id=1, native_block_id="b", rooms=1, area=40.0,
                        floor=2, price=8_000_000,
                        plan_url="https://cdn.example.com/plan.png")],
    )
    _, flats, _ = build_rows("Level", result, scan_date="d", scan_ts="t")
    assert flats[0]["plan_url"] == "https://cdn.example.com/plan.png"


def test_build_rows_drops_orphan_flat_without_registered_block():
    # квартира ссылается на ЖК, которого нет в blocks → её надо отбросить,
    # иначе в today_all она ошибочно прикинулась бы квартирой ПИК
    result = CollectResult(
        blocks=[NormBlock(native_id="real", name="ЖК", slug="real")],
        flats=[
            NormFlat(native_id=1, native_block_id="real", rooms=1,
                     area=40.0, floor=2, price=8_000_000),
            NormFlat(native_id=2, native_block_id="ghost", rooms=2,
                     area=60.0, floor=5, price=9_000_000),
        ],
    )
    _, flats, snaps = build_rows("Level", result, scan_date="d", scan_ts="t")
    assert len(flats) == 1 and len(snaps) == 1
    assert flats[0]["guid"] == "1"


def test_build_rows_drops_duplicate_and_idless_flats():
    result = CollectResult(
        blocks=[NormBlock(native_id="b", name="ЖК", slug="b")],
        flats=[
            NormFlat(native_id=7, native_block_id="b", rooms=1, area=40.0,
                     floor=2, price=8_000_000),
            NormFlat(native_id=7, native_block_id="b", rooms=2, area=50.0,
                     floor=3, price=9_000_000),   # дубль id → отброшен
            NormFlat(native_id=None, native_block_id="b", rooms=1, area=30.0,
                     floor=1, price=5_000_000),   # без id → отброшен
        ],
    )
    _, flats, _ = build_rows("Level", result, scan_date="d", scan_ts="t")
    assert len(flats) == 1


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


def test_fsk_to_norm_passes_plan_url_through():
    fl = {"externalId": 1, "price": 1, "areaTotal": 1.0,
          "plan": "https://cdn.fsk.ru/x.svg"}
    assert fsk._to_norm(fl, "z").plan_url == "https://cdn.fsk.ru/x.svg"


def test_fsk_collect_aggregates_floors_max_from_floor_numbers(monkeypatch):
    """FSK API не отдаёт floors_max — оцениваем как MAX(floorNumber)."""
    def fake(session, method, url, **kw):
        if url.endswith("/complex/"):
            return [{"slug": "z", "title": "Z", "city_id": 1, "flats": {"all": 3}}]
        return [
            {"externalId": 1, "price": 1, "areaTotal": 1, "floorNumber": 5},
            {"externalId": 2, "price": 1, "areaTotal": 1, "floorNumber": 27},
            {"externalId": 3, "price": 1, "areaTotal": 1, "floorNumber": 13},
        ]
    monkeypatch.setattr("pik.sources.fsk.request_json", fake)
    result = fsk.collect()
    assert result.blocks[0].meta.get("floors_max") == 27


def test_absolut_to_norm_extracts_plan_url():
    node = {"pk": "x", "project": {"slug": "p"}, "price": 1.0,
            "building": {}, "section": {}, "floor": {},
            "plan": "https://absrealty.ru/plan.png"}
    assert absolut._to_norm(node).plan_url == "https://absrealty.ru/plan.png"


def test_absolut_collect_aggregates_floors_from_buildingFloor(monkeypatch):
    """buildingFloor — объект {number}; floors_max = MAX(buildingFloor.number)."""
    def fake_node(pk, slug, bf):
        return {"node": {"pk": pk, "price": 1.0,
                         "buildingFloor": {"number": bf},
                         "project": {"slug": slug, "name": slug},
                         "building": {}, "section": {}, "floor": {}}}
    pages = [{"data": {"allFlats": {"edges": [
        fake_node("a", "alpha", 9), fake_node("b", "alpha", 22),
        fake_node("c", "beta",  14),
    ], "pageInfo": {"endCursor": None, "hasNextPage": False}}}}]
    monkeypatch.setattr("pik.sources.absolut.request_json",
                        lambda *a, **k: pages.pop(0))
    result = absolut.collect()
    by_slug = {b.slug: b.meta.get("floors_max") for b in result.blocks}
    assert by_slug == {"alpha": 22, "beta": 14}


def test_fsk_to_norm_detects_discount():
    base = {"externalId": 1, "price": 9_000_000, "areaTotal": 50.0, "rooms": 2}
    with_disc = fsk._to_norm({**base, "priceWoDiscount": 10_000_000}, "z")
    assert with_disc.old_price == 10_000_000
    no_disc = fsk._to_norm({**base, "priceWoDiscount": 9_000_000}, "z")
    assert no_disc.old_price is None


def test_fsk_status_missing_is_none_not_string():
    norm = fsk._to_norm({"externalId": 1, "price": 1, "areaTotal": 1.0}, "z")
    assert norm.status is None  # не мусорный литерал "None"


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


def test_donstroy_to_norm_builds_plan_url_from_relative_path():
    nf = donstroy._to_norm({"id": "x", "project": "P", "price": 1,
                            "plan": "/hydra/svg/apartment/10/b39/x.svg"})
    assert nf.plan_url == "https://donstroy.moscow/hydra/svg/apartment/10/b39/x.svg"


def test_donstroy_to_norm_passes_absolute_plan_url_through():
    nf = donstroy._to_norm({"id": "x", "project": "P", "price": 1,
                            "plan": "https://cdn.example.com/p.svg"})
    assert nf.plan_url == "https://cdn.example.com/p.svg"


def test_donstroy_collect_aggregates_floors_total_per_block(monkeypatch):
    pages = {
        1: [
            {"id": "a", "project": "Символ", "price": 1, "floors_total": "15"},
            {"id": "b", "project": "Символ", "price": 1, "floors_total": "27"},
            {"id": "c", "project": "Жизнь",  "price": 1, "floors_total": "32"},
        ],
        2: [],
    }
    monkeypatch.setattr("pik.sources.donstroy.request_json",
                        lambda s, m, u, *, json=None, **kw: {"flats": pages.get(json["page"], [])})
    result = donstroy.collect()
    floors = {b.name: b.meta.get("floors_max") for b in result.blocks}
    assert floors == {"Символ": 27, "Жизнь": 32}


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


# --- a101 ---------------------------------------------------------------

from pik.sources import a101  # noqa: E402


def test_a101_to_norm_maps_real_fixture():
    fl = json.load(open(FIXTURES / "a101_flats.json"))["results"][0]
    norm = a101._to_norm(fl)
    assert norm.native_id == 78303
    assert norm.native_block_id == "rodniye-kvartaly"
    assert norm.rooms == 0          # studio=true → студия, хотя room=1
    assert norm.price == 6_947_144  # actual_price
    assert norm.old_price == 8_793_728  # price (база) > actual
    assert norm.meter_price == 337_240  # actual_ppm
    assert norm.status == "free"    # status 4
    assert norm.bulk_name == "Корпус 2"
    assert norm.section_no == 6
    assert norm.floor == 5


def test_a101_to_norm_sets_per_flat_url_and_plan():
    nf = a101._to_norm({"id": 41849, "project_slug": "p", "actual_price": 5_000_000,
                        "floor_plan": "https://cdn.a101.ru/x/plan.png",
                        "max_floor": 14})
    assert nf.url == "https://a101.ru/kvartiry/41849/"
    assert nf.plan_url == "https://cdn.a101.ru/x/plan.png"


def test_a101_to_norm_plan_url_falls_back_to_big_layout():
    nf = a101._to_norm({"id": 1, "project_slug": "p", "actual_price": 1,
                        "floor_plan": None,
                        "big_layout_png": "https://cdn.a101.ru/y/big.png"})
    assert nf.plan_url == "https://cdn.a101.ru/y/big.png"


def test_a101_collect_aggregates_floors_max_per_block(monkeypatch):
    pages = [
        {"results": [
            {"id": 1, "project_slug": "родник", "project": "Родник",
             "actual_price": 5_000_000, "max_floor": 9, "room": 1, "studio": False},
            {"id": 2, "project_slug": "родник", "project": "Родник",
             "actual_price": 6_000_000, "max_floor": 17, "room": 2, "studio": False},
            {"id": 3, "project_slug": "лес",    "project": "Лес",
             "actual_price": 4_000_000, "max_floor": 25, "room": 1, "studio": False},
        ], "next": None},
    ]
    monkeypatch.setattr("pik.sources.a101.request_json",
                        lambda *a, **k: pages.pop(0))
    result = a101.collect()
    floors = {b.slug: b.meta.get("floors_max") for b in result.blocks}
    assert floors == {"родник": 17, "лес": 25}


def test_a101_studio_flag_overrides_room_count():
    norm = a101._to_norm({"id": 1, "project_slug": "p", "room": 1,
                          "studio": True, "actual_price": 5_000_000})
    assert norm.rooms == 0
    norm2 = a101._to_norm({"id": 2, "project_slug": "p", "room": 2,
                           "studio": False, "actual_price": 5_000_000})
    assert norm2.rooms == 2


def test_a101_collect_follows_next_pagination(monkeypatch):
    fl = json.load(open(FIXTURES / "a101_flats.json"))["results"][0]
    pages = [
        {"results": [fl], "next": "https://a101.ru/api/flats/?offset=1000"},
        {"results": [{**fl, "id": 99, "project_slug": "other", "project": "Другой"}],
         "next": None},
    ]
    flat_calls = []

    def fake_request_json(session, method, url, **kw):
        # collect теперь дополнительно тянет /api/projects/<slug>/ — пусть
        # вернётся пустой meta (без метро/координат), это не должно ломать тест.
        if "/projects/" in url:
            return {}
        flat_calls.append(url)
        return pages[len(flat_calls) - 1]

    monkeypatch.setattr("pik.sources.a101.request_json", fake_request_json)
    result = a101.collect()
    assert len(flat_calls) == 2  # прошли по `next`
    assert len(result.flats) == 2
    assert {b.slug for b in result.blocks} == {"rodniye-kvartaly", "other"}


# --- level --------------------------------------------------------------

from pik.sources import level  # noqa: E402


def test_level_settlement_formats_quarter_and_year():
    assert level._settlement({"completion_year": 2026, "completion_quarter": 1}) \
        == "1 кв. 2026"
    assert level._settlement({"completion_year": 2027}) == "2027"
    assert level._settlement({}) is None


def test_level_section_no_parses_numeric_title():
    assert level._section_no({"section_title": "2"}) == 2
    assert level._section_no({"section_title": "1-1"}) is None  # составной → не int
    assert level._section_no({"section_title": None}) is None
    assert level._section_no({"section_title": ""}) is None


def test_level_to_norm_sets_plan_url_prefers_plan_over_floor_plan():
    nf = level._to_norm({"pk": 1, "project_slug": "p", "price": 1,
                         "plan": "https://cdn.level.ru/a.png",
                         "floor_plan": "https://cdn.level.ru/b.png"})
    assert nf.plan_url == "https://cdn.level.ru/a.png"


def test_level_collect_aggregates_floors_section_total_per_block(monkeypatch):
    pages = [
        {"results": [
            {"pk": 1, "project_slug": "bauman", "project": "Bauman",
             "price": 1, "floors_section_total": 9},
            {"pk": 2, "project_slug": "bauman", "project": "Bauman",
             "price": 1, "floors_section_total": 13},
            {"pk": 3, "project_slug": "city",   "project": "City",
             "price": 1, "floors_section_total": 22},
        ], "next": None},
    ]
    monkeypatch.setattr("pik.sources.level.request_json",
                        lambda *a, **k: pages.pop(0))
    result = level.collect()
    floors = {b.slug: b.meta.get("floors_max") for b in result.blocks}
    assert floors == {"bauman": 13, "city": 22}


def test_level_to_norm_maps_real_fixture():
    fl = json.load(open(FIXTURES / "level_flats.json"))["results"][0]
    norm = level._to_norm(fl)
    assert norm.native_id == 58772
    assert norm.native_block_id == "bauman"
    assert norm.rooms == 5
    assert norm.price == 65_097_637
    assert norm.old_price == 81_372_046  # old_price float > price → int
    assert norm.meter_price == 339_580
    assert norm.bulk_name == "Корпус B"
    assert norm.status == "free"  # status 1
    assert norm.url == "https://level.ru/bauman/apartment/5room/2-N1/"
    assert norm.finish == "Без отделки"


# --- absolut ------------------------------------------------------------

from pik.sources import absolut  # noqa: E402


def test_absolut_round_price():
    assert absolut._round_price(8611351.88) == 8_611_352
    assert absolut._round_price(None) is None


def test_absolut_settlement():
    assert absolut._settlement({"completionYear": 2028, "completionQuarter": "I"}) \
        == "I кв. 2028"
    assert absolut._settlement({}) is None


def test_absolut_to_norm_maps_real_fixture():
    node = json.load(open(FIXTURES / "absolut_flats.json"))[
        "data"]["allFlats"]["edges"][0]["node"]
    norm = absolut._to_norm(node)
    assert norm.native_id == "4fe60a95-da39-11ef-9436-9c8e99fc8634"
    assert norm.native_block_id == "peredelkino-blizhnee"
    assert norm.rooms == 0
    assert norm.price == 8_611_352      # дробная цена округлена
    assert norm.old_price is None       # hasDiscount=false → без старой цены
    assert norm.bulk_name == "Корпус 2"
    assert norm.section_no == 4
    assert norm.floor == 1
    assert norm.settlement_date == "I кв. 2028"
    assert norm.finish == "Без отделки"  # facing=false


def test_absolut_old_price_only_when_has_discount():
    node = {"pk": "x", "project": {"slug": "p"}, "price": 8_000_000.0,
            "originPrice": 10_000_000.0, "hasDiscount": True,
            "building": {}, "section": {}, "floor": {}}
    norm = absolut._to_norm(node)
    assert norm.old_price == 10_000_000


def test_absolut_collect_cursor_pagination(monkeypatch):
    node = json.load(open(FIXTURES / "absolut_flats.json"))[
        "data"]["allFlats"]["edges"][0]["node"]
    pages = [
        {"data": {"allFlats": {"edges": [{"node": node}],
         "pageInfo": {"endCursor": "c1", "hasNextPage": True}}}},
        {"data": {"allFlats": {"edges": [{"node": {**node, "pk": "p2"}}],
         "pageInfo": {"endCursor": "c2", "hasNextPage": False}}}},
    ]
    seen_after = []

    def fake_request_json(session, method, url, *, json=None, **kw):
        seen_after.append(json["variables"]["after"])
        return pages[len(seen_after) - 1]

    monkeypatch.setattr("pik.sources.absolut.request_json", fake_request_json)
    result = absolut.collect()
    assert seen_after == [None, "c1"]  # курсор передаётся на 2-ю страницу
    assert len(result.flats) == 2


# --- mrgroup ------------------------------------------------------------

from pik.sources import mrgroup  # noqa: E402
from pik.sources import granel  # noqa: E402


def test_granel_coords_parses_string_pair():
    assert granel._coords("55.83,37.92") == (55.83, 37.92)
    assert granel._coords("  55.83 , 37.92  ") == (55.83, 37.92)
    assert granel._coords(None) is None
    assert granel._coords("bad") is None


def test_granel_to_norm_basic():
    fl = {"id": 4343, "project_slug": "new-ar", "project_full_name": "Новая АР",
          "number": "82", "price": 6225277.08, "current_price": "4637196.20",
          "show_price_discounted": True,
          "area": 23.38, "rooms": 0, "floor": 2, "floor_count": 17,
          "building": "3", "section": 2, "status": 1,
          "completion_year": "2025", "completion_quarter": "4",
          "finish_type": "without_finish",
          "plan": "https://cdn/.../plan.svg"}
    nf = granel._to_norm(fl)
    assert nf.native_id == 4343
    assert nf.native_block_id == "new-ar"
    assert nf.price == 4637196  # round(4637196.20) → 4637196
    assert nf.old_price == 6225277  # base > current → дисконт виден
    assert nf.area == 23.38
    assert nf.rooms == 0
    assert nf.floor == 2
    assert nf.bulk_name == "Корпус 3"
    assert nf.section_no == 2
    assert nf.settlement_date == "4 кв. 2025"
    assert nf.finish == "Без отделки"
    # URL обязательно с project_slug — /flats/<slug>/<id> (без trailing slash).
    # Без slug API даёт 404 для всех 2763 квартир (проверено live в ревью).
    assert nf.url == "https://granelle.ru/flats/new-ar/4343"
    assert nf.plan_url.endswith("plan.svg")


def test_granel_to_norm_no_discount_when_flag_false():
    fl = {"id": 1, "project_slug": "p", "price": 5_000_000,
          "current_price": "4_500_000".replace("_", ""),
          "show_price_discounted": False, "area": 30, "rooms": 1,
          "status": 1, "finish_type": "finish"}  # real API value
    nf = granel._to_norm(fl)
    assert nf.old_price is None  # флаг скидки отключён → не показываем
    assert nf.finish == "С отделкой"


def test_granel_finish_maps_actual_api_values():
    """Гранель API отдаёт без подчёркивания/префикса 'with_': finish, whitebox."""
    def f(t): return granel._to_norm({"id":1,"project_slug":"p","price":1,
                                       "area":1,"finish_type":t}).finish
    assert f("without_finish") == "Без отделки"
    assert f("finish") == "С отделкой"
    assert f("whitebox") == "WhiteBox"
    assert f("unknown_new_value") is None  # неизвестное → None, не сырой текст


def test_granel_to_norm_url_none_without_slug():
    nf = granel._to_norm({"id": 4343, "price": 1, "area": 1})
    assert nf.url is None  # нет project_slug → нет валидного URL


def test_granel_project_meta_detects_non_msk_by_coords(monkeypatch):
    """Уфимский ЖК не должен оказаться 'msk' с distance до Кремля."""
    monkeypatch.setattr("pik.sources.granel.request_json",
        lambda *a, **k: {"results": [
            {"slug": "ufa-zhk", "coords": "54.72,56.02"},     # Уфа
            {"slug": "msk-zhk", "coords": "55.75,37.61"},     # Москва
            {"slug": "remote",  "coords": "70.00,90.00"},     # >80 км от любого
        ]})
    meta = granel._fetch_project_meta(granel.make_session())
    assert meta["ufa-zhk"]["city"] == "ufa"
    assert meta["msk-zhk"]["city"] == "msk"
    assert "city" not in meta["remote"]  # дальше 80 км → не пытаемся угадать


def test_granel_collect_follows_next_pagination(monkeypatch):
    pages = [
        {"results": [
            {"id": 1, "project_slug": "alpha", "project": "Альфа",
             "project_full_name": "ЖК Альфа", "price": 5_000_000,
             "area": 30, "rooms": 1, "floor": 5, "floor_count": 9,
             "status": 1, "finish_type": "with_finish"},
        ], "next": "http://granelle.ru/api/flats/?offset=200"},
        {"results": [
            {"id": 2, "project_slug": "beta", "project": "Бета",
             "project_full_name": "ЖК Бета", "price": 8_000_000,
             "area": 45, "rooms": 2, "floor": 12, "floor_count": 17,
             "status": 1, "finish_type": "white_box"},
        ], "next": None},
    ]
    project_calls = []
    def fake_request_json(session, method, url, **kw):
        if "/projects/" in url:
            project_calls.append(url)
            return {"results": [
                {"slug": "alpha", "coords": "55.83,37.92",
                 "transport_access_point": {"time": 8, "transport_point": {"name": "Бабушкинская"}}},
                {"slug": "beta",  "coords": "55.74,37.61",
                 "transport_access_point": {"time": 15, "transport_point": {"name": "Динамо"}}},
            ]}
        return pages.pop(0)
    monkeypatch.setattr("pik.sources.granel.request_json", fake_request_json)

    result = granel.collect()
    assert len(result.flats) == 2
    assert {b.slug for b in result.blocks} == {"alpha", "beta"}
    alpha = next(b for b in result.blocks if b.slug == "alpha")
    assert alpha.meta["floors_max"] == 9
    assert alpha.meta["metro_name"] == "Бабушкинская"
    assert alpha.meta["metro_time_foot"] == 8
    assert alpha.meta["latitude"] == 55.83
    assert len(project_calls) == 1


def test_granel_collect_upgrades_next_http_to_https(monkeypatch):
    """API отдаёт next= http://… — нужно подменять на https, иначе MITM-risk."""
    calls = []
    def fake(session, method, url, **kw):
        calls.append(url)
        if "/projects/" in url: return {"results": []}
        if "offset=200" in url: return {"results": [], "next": None}
        return {"results": [], "next": "http://granelle.ru/api/flats/?offset=200"}
    monkeypatch.setattr("pik.sources.granel.request_json", fake)
    granel.collect()
    # Второй вызов flats должен идти ПО HTTPS
    flat_calls = [u for u in calls if "/flats/" in u]
    assert len(flat_calls) == 2
    assert flat_calls[1].startswith("https://")


from pik.sources import ingrad  # noqa: E402


def test_ingrad_coords_pair_rejects_none_string():
    assert ingrad._coords_pair("55.83,37.92") == (55.83, 37.92)
    assert ingrad._coords_pair("None") is None  # API даёт литерал «None»
    assert ingrad._coords_pair(None) is None
    assert ingrad._coords_pair("") is None
    assert ingrad._coords_pair("bad") is None


def test_ingrad_to_norm_filters_office_and_storeroom():
    assert ingrad._to_norm({"type": "office", "estateId": {"code": "x"},
                            "id": 1}) is None
    assert ingrad._to_norm({"type": "flat", "isStoreroom": 1,
                            "estateId": {"code": "x"}, "id": 1}) is None
    nf = ingrad._to_norm({"type": "flat", "id": 1, "price": 5_000_000,
                          "square": 30.0, "rooms": 1, "floorNum": 5,
                          "number": "12", "status": "free", "finish": "WhiteBox",
                          "link": "/projects/comfort/foo/flats/1/",
                          "estateId": {"code": "foo", "name": "ЖК Foo"},
                          "houseId": {"name": "К1", "floorsCount": 17,
                                       "settlement_year": "2026", "settlement_quarter": "2"}})
    assert nf is not None
    assert nf.native_id == 1
    assert nf.native_block_id == "foo"
    assert nf.price == 5_000_000
    assert nf.bulk_name == "К1"
    # www.ingrad.ru канонический + без trailing slash (иначе 308 redirect)
    assert nf.url == "https://www.ingrad.ru/projects/comfort/foo/flats/1"
    assert nf.settlement_date == "2 кв. 2026"
    assert nf.finish == "WhiteBox"


def test_ingrad_to_norm_old_price_only_with_discount():
    nf = ingrad._to_norm({"type": "flat", "id": 1,
                          "estateId": {"code": "x"}, "houseId": {},
                          "price": 8_000_000, "priceNoDiscount": 10_000_000,
                          "square": 40, "rooms": 1, "status": "free"})
    assert nf.old_price == 10_000_000
    nf2 = ingrad._to_norm({"type": "flat", "id": 2,
                           "estateId": {"code": "x"}, "houseId": {},
                           "price": 8_000_000, "priceNoDiscount": 8_000_000,
                           "square": 40, "rooms": 1, "status": "free"})
    assert nf2.old_price is None


def test_ingrad_second_house_fills_missing_coords(monkeypatch):
    """Если первый корпус ЖК не дал координат — второй должен их «долить»."""
    pages = [{"list": [
        {"type": "flat", "id": 1, "price": 1, "square": 1, "rooms": 1,
         "status": "free", "estateId": {"code": "z", "name": "Z"},
         "houseId": {"name": "К1", "coords": "None"}},  # нет координат
        {"type": "flat", "id": 2, "price": 1, "square": 1, "rooms": 1,
         "status": "free", "estateId": {"code": "z", "name": "Z"},
         "houseId": {"name": "К2", "coords": "55.7,37.5",
                      "address": "Москва, ул. Y", "floorsCount": 25}},
    ]}]
    monkeypatch.setattr("pik.sources.ingrad.request_json",
                        lambda *a, **k: pages.pop(0))
    result = ingrad.collect()
    b = result.blocks[0]
    assert b.meta["latitude"] == 55.7
    assert b.meta["longitude"] == 37.5
    assert b.meta["address"] == "Москва, ул. Y"
    assert b.meta["floors_max"] == 25


def test_ingrad_skips_latin_metro_for_post_hoc_assignment():
    """Latin-slug метро не пишем — пусть nearest-station post-hoc подберёт."""
    estate = {"code": "x", "name": "X", "metro": "medvedkovo",
              "timeToMetro": 10, "timeToMetroType": "foot"}
    house = {"coords": "55.85,37.65"}
    meta = ingrad._project_meta_from_estate_and_house(estate, house)
    assert "metro_name" not in meta  # latin slug отброшен
    # При кириллице — пишем
    estate_cyr = {**estate, "metro": "Медведково"}
    meta2 = ingrad._project_meta_from_estate_and_house(estate_cyr, house)
    assert meta2["metro_name"] == "Медведково"
    assert meta2["metro_time_foot"] == 10


def test_ingrad_collect_aggregates_block_meta(monkeypatch):
    pages = [{"list": [
        {"type": "flat", "id": 1, "price": 5_000_000, "square": 30, "rooms": 1,
         "status": "free", "number": "1", "link": "/p/foo/flats/1/",
         "estateId": {"code": "foo", "name": "Foo", "metro": "Динамо",
                       "timeToMetro": 7, "timeToMetroType": "foot"},
         "houseId": {"name": "К1", "coords": "55.78,37.55", "floorsCount": 17,
                      "address": "Москва, ул. X", "settlement_year": "2026",
                      "settlement_quarter": "3"}},
        {"type": "flat", "id": 2, "price": 6_000_000, "square": 35, "rooms": 2,
         "status": "free", "link": "/p/foo/flats/2/",
         "estateId": {"code": "foo", "name": "Foo"},
         "houseId": {"name": "К2", "floorsCount": 22}},   # этажность больше → перебить
        {"type": "office", "id": 99, "estateId": {"code": "foo"}, "houseId": {}},
    ]}]
    monkeypatch.setattr("pik.sources.ingrad.request_json",
                        lambda *a, **k: pages.pop(0))
    result = ingrad.collect()
    assert len(result.flats) == 2  # офис отброшен
    b = result.blocks[0]
    assert b.slug == "foo"
    assert b.meta["floors_max"] == 22  # max по корпусам
    assert b.meta["latitude"] == 55.78
    assert b.meta["address"] == "Москва, ул. X"
    assert b.meta["metro_name"] == "Динамо"
    assert b.meta["metro_time_foot"] == 7


def test_granel_collect_rejects_foreign_next_host(monkeypatch):
    """Если бэк вернул next с чужим доменом — НЕ должны за ним идти."""
    calls = []
    def fake(session, method, url, **kw):
        calls.append(url)
        if "/projects/" in url: return {"results": []}
        return {"results": [], "next": "http://evil.example.com/api/flats/"}
    monkeypatch.setattr("pik.sources.granel.request_json", fake)
    granel.collect()
    # Один flat-вызов (стартовый), второй должен быть отброшен с warning
    flat_calls = [u for u in calls if "/flats/" in u]
    assert len(flat_calls) == 1
    assert "evil.example.com" not in (flat_calls[0] if flat_calls else "")


def test_mrgroup_num_parses_spaced_decimal():
    assert mrgroup._num("53 945 211,17") == 53945211.17
    assert mrgroup._num("424\xa0632") == 424632.0
    assert mrgroup._num("junk") is None


def test_mrgroup_card_text_decodes_nbsp_entities():
    text = mrgroup._card_text("<div>53&nbsp;945&nbsp;211&nbsp;₽</div>")
    assert text == "53 945 211 ₽"


def test_mrgroup_parse_card_extracts_all_fields():
    text = ("Сити Бэй -10% MR Base 4-комнатная, 127,04 м² "
            "53 945 211,17 ₽ 424 632 ₽/м² 59 939 124 ₽ Норс 7 "
            "24/26 этаж I кв. 2027 Консультация")
    f = mrgroup._parse_card("/catalog/apartments/sb-3-7-k-1-24-6-72406/",
                            text, "citybay")
    assert f.native_id == "sb-3-7-k-1-24-6-72406"
    assert f.native_block_id == "citybay"
    assert f.rooms == 4
    assert f.area == 127.04
    assert f.floor == 24
    assert f.price == 53_945_211
    assert f.meter_price == 424_632
    assert f.old_price == 59_939_124
    assert f.bulk_name == "Норс 7"
    assert f.settlement_date == "I кв. 2027"
    assert f.url == "https://www.mr-group.ru/catalog/apartments/sb-3-7-k-1-24-6-72406/"


def test_mrgroup_building_name_strips_zhk_suffix():
    # к имени корпуса на сайте местами дописано « от <ЖК>» — должно отрезаться
    text = ("Сити Бэй -7% 1-комнатная, 38,85 м² 22 388 264 ₽ "
            "576 789 ₽/м² 24 073 403 ₽ Клиф 5 от Сити Бэй 12/20 этаж")
    f = mrgroup._parse_card("/catalog/apartments/x-1/", text, "citybay")
    assert f.bulk_name == "Клиф 5"


def test_mrgroup_parse_card_studio():
    f = mrgroup._parse_card("/catalog/apartments/x-1/",
                            "Студия, 25,0 м² 10 000 000 ₽", "mod")
    assert f.rooms == 0


def test_mrgroup_parse_card_returns_none_without_price():
    assert mrgroup._parse_card("/catalog/apartments/x-1/",
                               "2-комнатная, 50 м²", "mod") is None


def test_mrgroup_parse_flats_page_on_real_fixture():
    html = open(FIXTURES / "mrgroup_citybay.html", encoding="utf-8").read()
    flats = mrgroup.parse_flats_page(html, "citybay")
    assert len(flats) == 48
    # все карточки полные: цена, площадь, комнатность
    assert all(f.price and f.area and f.rooms is not None for f in flats)


def test_mrgroup_parse_flats_page_empty_on_antibot_stub():
    assert mrgroup.parse_flats_page("<html><body>challenge</body></html>", "x") == []
