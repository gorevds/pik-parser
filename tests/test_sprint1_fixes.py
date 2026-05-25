"""Тесты Sprint 1 — точечные регрессии под каждый фикс.

Один файл на спринт удобен: легко увидеть «что ловит спринт 1» одной командой,
легко удалить блок, если фикс откатили.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pik.blocks_meta import upsert_block_meta
from pik.mapping import _parse_rate
from pik.merge import merge_databases
from pik.sources.base import safe_next_url
from pik.sources.brusnika import _to_norm as _brusnika_to_norm
from pik.store import _assign_nearest_metro, apply_schema, upsert

# ============================================================== S1.6 SSRF =====


@pytest.mark.parametrize("url,expected", [
    ("https://a101.ru/api/x?o=1",          "https://a101.ru/api/x?o=1"),
    ("https://www.a101.ru/api/x",          "https://www.a101.ru/api/x"),
    ("http://a101.ru/api/x",               "https://a101.ru/api/x"),    # форсируем https
    ("https://evil.com/a101.ru/api/x",     None),
    ("https://a101.ru.evil.com/api/x",     None),
    ("https://attacker.org/?u=a101.ru",    None),
    ("javascript:alert(1)",                None),
    ("file:///etc/passwd",                 None),
    ("",                                   None),
    (None,                                 None),
])
def test_safe_next_url_blocks_foreign_hosts(url, expected):
    assert safe_next_url(url, "a101.ru") == expected


def test_safe_next_url_subdomain_match():
    """API может выдать next с поддоменом — это валидно."""
    assert (safe_next_url("https://api.granelle.ru/api/flats/?page=2", "granelle.ru")
            == "https://api.granelle.ru/api/flats/?page=2")


def test_safe_next_url_strips_trailing_dot_in_fqdn():
    """`a101.ru.` — валидная FQDN-форма, эквивалентна `a101.ru`."""
    got = safe_next_url("https://a101.ru./api/flats/?page=2", "a101.ru")
    assert got == "https://a101.ru./api/flats/?page=2"


def test_safe_next_url_userinfo_injection_blocked():
    """https://a101.ru@evil.com/x должен распарситься в host=evil.com."""
    assert safe_next_url("https://a101.ru@evil.com/x", "a101.ru") is None


# ============================================================== S1.10 rate ====


def test_parse_rate_accepts_plausible_mortgage_rate():
    assert _parse_rate("Семейная ипотека 6%") == 6.0
    assert _parse_rate("Стандартная 18,5%") == 18.5
    assert _parse_rate("IT-ипотека 5%") == 5.0


def test_parse_rate_rejects_implausible_promo_percent():
    # «50% скидка» — точно не ставка ипотеки
    assert _parse_rate("Скидка 50% при оплате наличными") is None
    # «0% комиссия» — не ставка
    assert _parse_rate("Комиссия 0% за месяц") is None
    # 100% — явный мусор
    assert _parse_rate("Гарантированный возврат 100%") is None


def test_parse_rate_accepts_high_but_plausible_rate():
    # «Стандартная ипотека 30%» (ЦБ 25%+ ставка) — это всё ещё реалистично
    assert _parse_rate("Стандартная ипотека 30%") == 30.0


def test_parse_rate_none_for_no_number():
    assert _parse_rate("Без процентов") is None
    assert _parse_rate("") is None
    assert _parse_rate(None) is None


# ============================================================== S1.3 COALESCE ===


def _schema():
    """In-memory БД с полной схемой — для тестов upsert_block_meta."""
    c = sqlite3.connect(":memory:")
    c.execute("PRAGMA foreign_keys = ON")
    apply_schema(c)
    return c


def test_upsert_block_meta_coalesce_preserves_existing_on_partial_meta():
    """Если второй scan отдал meta без metro — оно НЕ должно затереться NULL'ом."""
    c = _schema()
    # Первый скан: всё есть
    upsert_block_meta(
        c, block_id=999, name="ЖК Тест", developer="ПИК", slug="test",
        scan_ts="2026-05-24T06:00",
        meta={"metro_name": "Молодёжная", "metro_time_foot": 7,
              "latitude": 55.7, "longitude": 37.4, "city": "msk"},
    )
    # Второй скан: meta partial (источник /api/projects/ упал, metro нет)
    upsert_block_meta(
        c, block_id=999, name="ЖК Тест", developer="ПИК", slug="test",
        scan_ts="2026-05-25T06:00",
        meta={"city": "msk"},  # только city, metro/coords отсутствуют
    )
    metro, t, lat, lng = c.execute(
        "SELECT metro_name, metro_time_foot, latitude, longitude "
        "FROM blocks WHERE id=999"
    ).fetchone()
    # COALESCE: значения первого скана сохранились
    assert metro == "Молодёжная"
    assert t == 7
    assert lat == 55.7
    assert lng == 37.4


def test_upsert_block_meta_overwrites_when_new_value_present():
    """А вот ЕСЛИ в новой meta поле есть и непустое — затираем нормально."""
    c = _schema()
    upsert_block_meta(c, block_id=999, name="X", developer="ПИК", slug="x",
                      scan_ts="t1", meta={"metro_name": "Старая"})
    upsert_block_meta(c, block_id=999, name="X", developer="ПИК", slug="x",
                      scan_ts="t2", meta={"metro_name": "Новая"})
    assert c.execute("SELECT metro_name FROM blocks WHERE id=999").fetchone()[0] == "Новая"


# ====================================================== S1.4 nearest-metro guard ==


def test_assign_nearest_metro_does_not_overwrite_existing_value():
    """Если у блока УЖЕ есть metro_name, эвристика не должна его перетирать."""
    c = _schema()
    # Donor: блок с настоящим metro
    upsert_block_meta(c, block_id=1, name="Donor", developer="ПИК", slug="d", scan_ts="t",
                      meta={"metro_name": "Лубянка", "metro_time_foot": 3,
                            "latitude": 55.760, "longitude": 37.629})
    # Recipient: тоже с metro, но другим — нашим nearest-эвристикой мы НЕ
    # должны его перезаписать на «Лубянка».
    upsert_block_meta(c, block_id=2, name="Recipient", developer="ПИК", slug="r", scan_ts="t",
                      meta={"metro_name": "Театральная", "metro_time_foot": 5,
                            "latitude": 55.760, "longitude": 37.624})
    _assign_nearest_metro(c)
    metro = c.execute("SELECT metro_name FROM blocks WHERE id=2").fetchone()[0]
    assert metro == "Театральная"  # НЕ перезаписан


def test_assign_nearest_metro_fills_null_from_neighbor():
    """А ЕСЛИ metro_name действительно NULL — заполняем от ближайшего."""
    c = _schema()
    upsert_block_meta(c, block_id=1, name="Donor", developer="ПИК", slug="d", scan_ts="t",
                      meta={"metro_name": "Лубянка", "metro_time_foot": 3,
                            "latitude": 55.760, "longitude": 37.629})
    upsert_block_meta(c, block_id=2, name="Orphan", developer="ПИК", slug="o", scan_ts="t",
                      meta={"latitude": 55.760, "longitude": 37.629})
    _assign_nearest_metro(c)
    metro = c.execute("SELECT metro_name FROM blocks WHERE id=2").fetchone()[0]
    assert metro == "Лубянка"  # заполнено


# ============================================================== S1.12 Brusnika =====


def test_brusnika_to_norm_keeps_raw_native_id_for_region_prefixing():
    """_to_norm возвращает «голый» flat_id; префиксует уже _collect_region.

    Этот контракт важен: если кто-то прямо вызовет _to_norm, native_id всё ещё
    числовой/строковый из API. Префикс — задача обёртки, чтобы её можно было
    тестировать отдельно.
    """
    fl = {"flat_id": "1", "complex": "79", "price": "10000000",
          "square": "30", "rooms": "1", "is_booked": False, "tags": []}
    nf = _brusnika_to_norm(fl)
    assert nf.native_id == "1"
    assert nf.native_block_id == "79"


def test_brusnika_region_prefixes_collide_only_after_split():
    """Доказательство, что префикс реально разводит коллизию.

    Москва flat_id=100 и Тюмень flat_id=100 имеют одинаковую цифру 100;
    без префикса to_global_id("Брусника", 100) даст один и тот же gid.
    """
    from pik.sources.base import to_global_id
    gid_msk_raw = to_global_id("Брусника", 100)
    gid_tmn_raw = to_global_id("Брусника", 100)
    assert gid_msk_raw == gid_tmn_raw   # без префикса — коллизия
    # С префиксом — stable_int_id хеширует разные строки в разные числа
    gid_msk = to_global_id("Брусника", "moskva:100")
    gid_tmn = to_global_id("Брусника", "tyumen:100")
    assert gid_msk != gid_tmn           # коллизия разведена


# ============================================================== S1.14 upsert iter =


def test_upsert_handles_generator_input():
    """flats как генератор не должны теряться при материализации."""
    c = _schema()
    SAMPLE_F = {"id": 100, "guid": "g-100", "block_id": 1165,
                "bulk_id": 10397, "section_id": 23643, "layout_id": 60971,
                "bulk_name": "K1.3", "section_no": 3, "floor": 7,
                "rooms": "1", "rooms_fact": 1, "is_studio": 0, "area": 33.5,
                "area_kitchen": 8.0, "area_living": 16.2, "number": "1",
                "name": "kv1", "url": "u", "pdf_url": None,
                "plan_url": None, "ceiling_height": 2.75,
                "settlement_date": "2027-10-31", "first_seen": "2026-05-15"}
    SAMPLE_S = {"flat_id": 100, "scan_date": "2026-05-15",
                "scan_ts": "2026-05-15T06:00", "status": "free",
                "price": 12_000_000, "meter_price": 358_209,
                "base_meter_price": 358_209, "promo_price": 12_000_000,
                "discount_pct": 0.0, "has_promo": 0, "old_price": None,
                "discount": 0, "finish": "X", "mortgage_min_rate": 6.0,
                "mortgage_best_name": "Y", "updated_at": "t"}
    upsert(c, flats=(r for r in [SAMPLE_F]), snapshots=(r for r in [SAMPLE_S]))
    assert c.execute("SELECT COUNT(*) FROM flats").fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 1


# ============================================================== S1.11 merge.py ====


def _make_minimal_src_db(path: Path, *, with_apartment_col: bool):
    """Эмулируем БД со старой схемой (до миграции is_apartment)."""
    c = sqlite3.connect(path)
    if with_apartment_col:
        apply_schema(c)
    else:
        # Минимальная схема pre-2026-05-25: без is_apartment, без developer/city.
        c.executescript("""
            CREATE TABLE blocks (
                id INTEGER PRIMARY KEY, name TEXT NOT NULL, slug TEXT,
                updated_at TEXT
            );
            CREATE TABLE flats (
                id INTEGER PRIMARY KEY, guid TEXT NOT NULL, block_id INTEGER NOT NULL,
                bulk_id INTEGER, section_id INTEGER, layout_id INTEGER,
                bulk_name TEXT, section_no INTEGER, floor INTEGER, rooms TEXT,
                rooms_fact INTEGER, is_studio INTEGER, area REAL, area_kitchen REAL,
                area_living REAL, number TEXT, name TEXT, url TEXT, pdf_url TEXT,
                plan_url TEXT, ceiling_height REAL, settlement_date TEXT,
                first_seen TEXT NOT NULL
            );
            CREATE TABLE snapshots (
                flat_id INTEGER NOT NULL, scan_date TEXT NOT NULL,
                scan_ts TEXT NOT NULL, status TEXT, price INTEGER,
                meter_price INTEGER, base_meter_price INTEGER, promo_price INTEGER,
                discount_pct REAL, has_promo INTEGER,
                old_price INTEGER, discount INTEGER, finish TEXT,
                mortgage_min_rate REAL, mortgage_best_name TEXT, updated_at TEXT,
                PRIMARY KEY (flat_id, scan_date)
            );
        """)
    c.execute("INSERT INTO blocks (id, name, slug, updated_at) "
              "VALUES (?, ?, ?, ?)", (500, "Legacy ЖК", "legacy", "t"))
    c.execute("INSERT INTO flats (id, guid, block_id, first_seen) "
              "VALUES (?, ?, ?, ?)", (777, "g", 500, "2026-05-01"))
    c.execute("INSERT INTO snapshots (flat_id, scan_date, scan_ts, status, price) "
              "VALUES (?, ?, ?, ?, ?)", (777, "2026-05-01", "t", "free", 5_000_000))
    c.commit()
    c.close()


def test_merge_handles_legacy_db_without_is_apartment(tmp_path):
    """Старая БД без is_apartment не должна валить merge."""
    src = tmp_path / "legacy.db"
    main = tmp_path / "main.db"
    _make_minimal_src_db(src, with_apartment_col=False)
    summary = merge_databases(main_path=main, source_paths=[src])
    assert summary[str(src)]["flats_in_source"] == 1
    assert summary[str(src)]["blocks_in_source"] == 1
    # is_apartment получил DEFAULT 0 при миграции
    c = sqlite3.connect(main)
    apart = c.execute("SELECT is_apartment FROM flats WHERE id=777").fetchone()[0]
    assert apart == 0


def test_merge_carries_blocks_so_flats_are_not_orphan(tmp_path):
    """Merge переносит и blocks тоже — иначе сегодняшний today_all потерял бы ЖК."""
    src = tmp_path / "src.db"
    main = tmp_path / "main.db"
    _make_minimal_src_db(src, with_apartment_col=True)
    merge_databases(main_path=main, source_paths=[src])
    c = sqlite3.connect(main)
    name = c.execute("SELECT name FROM blocks WHERE id=500").fetchone()
    assert name == ("Legacy ЖК",)
