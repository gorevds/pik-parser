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


def _migrate_blocks(conn: sqlite3.Connection) -> None:
    """Доводит таблицу blocks до актуальной схемы (developer, city).

    Выполняется ДО executescript: view today_all ссылается на эти колонки,
    они должны существовать к моменту CREATE VIEW.

    `developer` — мульти-застройщик (2026-05-22). Существующие строки
    получают DEFAULT 'ПИК'.
    `city` — код города из адреса (2026-05-23). Без backfill старые блоки
    остались бы с NULL и для них view сваливался бы в 'msk' через COALESCE
    (т.е. Сахалин/Владивосток показались бы как Москва).
    """
    existing = {row[1] for row in conn.execute("PRAGMA table_info(blocks)")}
    if not existing:
        return  # таблицы ещё нет — schema.sql сейчас её создаст
    if "developer" not in existing:
        conn.execute(
            "ALTER TABLE blocks ADD COLUMN developer TEXT NOT NULL DEFAULT 'ПИК'"
        )
    if "city" not in existing:
        conn.execute("ALTER TABLE blocks ADD COLUMN city TEXT")
    # Backfill для строк без city — зеркало pik.geo.city_from_address.
    # `WHERE city IS NULL` делает повторный запуск дешёвым no-op'ом.
    # На минимальных легаси-схемах без `address` (фикстуры тестов) пропускаем:
    # извлекать город неоткуда, view упадёт в COALESCE → 'msk'.
    if "address" in existing:
        conn.execute(_CITY_BACKFILL_SQL)
        # вторая фаза: не-PIK 'other' (FSK без префикса города) → 'msk'
        if "developer" in existing or "developer" in {
            row[1] for row in conn.execute("PRAGMA table_info(blocks)")
        }:
            conn.execute(_CITY_NON_PIK_OTHER_FIX_SQL)


# Зеркало pik.geo._CITY_PATTERNS / city_from_address. Держим в SQL чтобы не
# вызывать Python из миграции на 20k строк; порядок WHEN тот же — регионы
# раньше «Московской области», иначе «Свердловская область» уйдёт в 'mo'.
_CITY_BACKFILL_SQL = """
UPDATE blocks SET city = CASE
    WHEN address LIKE '%Санкт-Петербург%' THEN 'spb'
    WHEN address LIKE '%Татарстан%' OR address LIKE '%Казан%' THEN 'kazan'
    WHEN address LIKE '%Свердловская%' OR address LIKE '%Екатеринбург%' THEN 'ekb'
    WHEN address LIKE '%Ярослав%' THEN 'yaroslavl'
    WHEN address LIKE '%Сахалин%' THEN 'yuzhno-sakhalinsk'
    WHEN address LIKE '%Приморский%' OR address LIKE '%Владивосток%' THEN 'vladivostok'
    WHEN address LIKE '%Хабаров%' THEN 'khabarovsk'
    WHEN address LIKE '%Новороссийск%' THEN 'novorossiisk'
    WHEN address LIKE '%Краснодар%' THEN 'krasnodar'
    WHEN address LIKE '%Тюмен%' THEN 'tyumen'
    WHEN address LIKE '%Обнинск%' THEN 'obninsk'
    WHEN address LIKE '%Калуг%' THEN 'kaluga'
    WHEN address LIKE '%Нижегород%' OR address LIKE '%Нижний Новгород%' THEN 'nn'
    WHEN address LIKE '%Башкортостан%' OR address LIKE '%г. Уфа%' OR address LIKE '%г.Уфа%' THEN 'ufa'
    WHEN address LIKE '%Челяб%' THEN 'chelyabinsk'
    WHEN address LIKE '%Улан-Удэ%' OR address LIKE '%Бурят%' THEN 'ulan-ude'
    WHEN address LIKE '%Благовещен%' OR address LIKE '%Амурская%' THEN 'blagoveshchensk'
    WHEN address LIKE '%Московская обл%' OR address LIKE '%Московская область%'
         OR address LIKE 'МО,%' OR address LIKE '%МО, %' THEN 'mo'
    WHEN address LIKE '%Москва%' THEN 'msk'
    WHEN address IS NULL THEN 'msk'
    ELSE 'other'
END
WHERE city IS NULL
"""


# Не-PIK источники сами фильтруют по Москве (FSK city_id=1, остальные —
# московские DRF/GraphQL), но FSK иногда отдаёт post_address без префикса
# города («ул. Шеногина, 2»). city_from_address для такого вернёт 'other'.
# Сворачиваем 'other' в 'msk' для не-PIK блоков — это корректно по контракту
# источника. Для PIK 'other' оставляем как есть (PIK всегда даёт регион в
# адресе, а 'other' там — реальный сигнал «надо расширить _CITY_PATTERNS»).
_CITY_NON_PIK_OTHER_FIX_SQL = (
    "UPDATE blocks SET city = 'msk' "
    "WHERE city = 'other' AND developer != 'ПИК'"
)


def apply_schema(conn: sqlite3.Connection) -> None:
    # WAL: писатель (скан) и читатели (Datasette) не блокируют друг друга.
    # Режим персистентен в файле БД. Для :memory: PRAGMA молча остаётся
    # 'memory' — исключения не будет.
    conn.execute("PRAGMA journal_mode=WAL")
    _migrate_snapshots(conn)
    _migrate_blocks(conn)
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
