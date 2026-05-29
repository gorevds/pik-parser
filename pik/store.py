import sqlite3
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from importlib.resources import files

_MSK = timezone(timedelta(hours=3))


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


def _migrate_flats(conn: sqlite3.Connection) -> None:
    """is_apartment добавлена в 2026-05-25 — апартаменты (нежилой фонд)
    обычно отдельный сегмент инвестиций."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(flats)")}
    if not existing:
        return
    if "is_apartment" not in existing:
        conn.execute(
            "ALTER TABLE flats ADD COLUMN is_apartment INTEGER NOT NULL DEFAULT 0"
        )


def _assign_nearest_metro(conn: sqlite3.Connection) -> None:
    """Для блоков с lat/lng без metro_name назначаем metro ближайшего блока,
    у которого metro_name известен. Идемпотентно (не перезаписываем), ограничено
    радиусом 5 км и итоговым временем 40 мин — иначе нет уверенности.

    Источник «эталонных» точек: те же blocks, где источник API отдал metro
    (PIK для всей Москвы, А101/Level/Абсолют для своих ЖК). FSK/Донстрой
    закрываются этим post-hoc-апдейтом — у них есть координаты, но API
    метро не отдают.
    """
    # Defensive — legacy-схемы (минимальные test-фикстуры) могут не иметь
    # latitude/longitude/metro_name. Без них считать нечего.
    cols = {row[1] for row in conn.execute("PRAGMA table_info(blocks)")}
    if not {"latitude", "longitude", "metro_name"} <= cols:
        return
    orphans = conn.execute(
        "SELECT id, latitude, longitude FROM blocks "
        "WHERE metro_name IS NULL "
        "  AND latitude IS NOT NULL AND longitude IS NOT NULL"
    ).fetchall()
    if not orphans:
        return
    refs = conn.execute(
        "SELECT metro_name, metro_line_name, metro_line_type, metro_time_foot, "
        "       latitude, longitude FROM blocks "
        "WHERE metro_name IS NOT NULL "
        "  AND latitude IS NOT NULL AND longitude IS NOT NULL"
    ).fetchall()
    if not refs:
        return
    from math import asin, cos, pi, sqrt
    def d(la1, lo1, la2, lo2):
        p = pi/180
        a = (0.5 - cos((la2-la1)*p)/2
             + cos(la1*p) * cos(la2*p) * (1 - cos((lo2-lo1)*p)) / 2)
        return 12742 * asin(sqrt(a))

    updates = []
    for oid, olat, olng in orphans:
        nearest = min(refs, key=lambda r: d(olat, olng, r[4], r[5]))
        dist_km = d(olat, olng, nearest[4], nearest[5])
        if dist_km > 5:
            continue   # слишком далеко — пусть остаётся NULL, чем гадать
        ref_time = nearest[3] or 5
        # время = собственное время блока-донора + пешком от orphan до него
        # (12 мин/км ≈ 5 км/ч). Это верхняя оценка, не точная.
        time_est = round(ref_time + dist_km * 12)
        if time_est > 40:
            continue
        updates.append((nearest[0], nearest[1], nearest[2], time_est, oid))
    if updates:
        # `AND metro_name IS NULL` — защита от гонки: между нашим SELECT
        # orphans и этим UPDATE мог отработать blocks_meta.upsert_block_meta
        # параллельного скана и положить metro_name от настоящего API.
        # Перетирать его нашей эвристикой «соседний ЖК» нельзя.
        conn.executemany(
            "UPDATE blocks SET metro_name=?, metro_line_name=?, "
            "metro_line_type=?, metro_time_foot=? "
            "WHERE id=? AND metro_name IS NULL",
            updates,
        )


# ============ Materialized views (today_all / today_one_room / sparkline) ===
#
# До 2026-05-25 эти три имени были VIEW'ами, вычислявшимися на каждый запрос.
# При >40k квартир CTE с GROUP BY и json_group_array выдавали 3-5с в
# Datasette (сам sqlite-CLI делает 0.3с — оверхед на JSON-сериализацию).
#
# refresh_materialized() заменяет VIEW → TABLE в конце каждого скана.
# Чтение становится 50мс вместо нескольких секунд. Atomic swap через
# transactional rename, чтобы Datasette-reader всегда видел консистентный
# срез (старый или новый, но не половину).
#
# Единый source-of-truth для SELECT-тел: эти строки используются И в
# apply_schema (создание view'ов на fresh DB) И в refresh_materialized
# (создание таблиц). Без этого пришлось бы держать SQL в двух местах и
# ловить дрейф.
_TODAY_ALL_SELECT = """
WITH block_latest AS (
    SELECT f.block_id AS block_id, MAX(s.scan_date) AS scan_date
    FROM snapshots s
    JOIN flats f ON f.id = s.flat_id
    GROUP BY f.block_id
)
SELECT
    f.id                      AS id,
    -- b.developer NOT NULL DEFAULT 'ПИК' в схеме; миграция _migrate_blocks
    -- бэкфилит существующие строки. COALESCE убран после R4 — иначе любая
    -- забытая запись блока тихо клеилась бы к ПИК.
    b.developer               AS застройщик,
    COALESCE(b.name, 'block ' || f.block_id) AS жк,
    COALESCE(b.city, 'msk')   AS город,
    b.metro_name              AS метро,
    CASE b.metro_line_type
        WHEN 1 THEN 'M'
        WHEN 2 THEN 'МЦК'
        WHEN 3 THEN 'МЦД'
        WHEN 4 THEN 'электр.'
        ELSE NULL
    END                       AS тип_транспорта,
    b.metro_time_foot         AS "мин_пешком",
    b.metro_line_name         AS линия,
    b.distance_km             AS "км_от_центра",
    CASE f.rooms
        WHEN 'studio' THEN 'студия'
        WHEN '-1'     THEN 'студия'
        ELSE f.rooms || 'к'
    END                       AS комнат,
    f.is_apartment            AS апартаменты,
    f.bulk_name               AS корпус,
    f.section_no              AS секция,
    f.floor                   AS этаж,
    f.area                    AS "площадь_м²",
    COALESCE(s.old_price, s.price) AS базовая_цена,
    s.promo_price             AS "цена_по_программе",
    s.base_meter_price        AS "база_за_м²",
    s.meter_price             AS "по_программе_за_м²",
    s.has_promo               AS "промо",
    s.discount_pct            AS "скидка_%",
    s.mortgage_best_name      AS программа,
    s.status                  AS статус,
    s.finish                  AS отделка,
    f.settlement_date         AS заселение,
    b.floors_max              AS "этажей_всего",
    b.address                 AS адрес,
    f.name                    AS артикул,
    f.url                     AS ссылка,
    f.plan_url                AS планировка,
    s.scan_date               AS дата_среза,
    f.block_id                AS block_id
FROM flats f
JOIN snapshots s ON s.flat_id = f.id
JOIN block_latest bl ON bl.block_id = f.block_id AND bl.scan_date = s.scan_date
LEFT JOIN blocks b ON b.id = f.block_id
"""

_TODAY_ONE_ROOM_SELECT = "SELECT * FROM today_all WHERE комнат = '1к'"


def _sparkline_select(cutoff_date: str) -> str:
    """Подставляет MSK-дату 30 дней назад. До этого было date('now','-30 days')
    в UTC — на границе МСК ночью окно сдвигалось на сутки, и из sparkline
    могла пропасть последняя точка.
    """
    return f"""
SELECT
    flat_id,
    json_group_array(price) AS prices
FROM (
    SELECT flat_id, scan_date, price
    FROM snapshots
    WHERE scan_date >= '{cutoff_date}' AND price IS NOT NULL
    ORDER BY flat_id, scan_date
)
GROUP BY flat_id
HAVING COUNT(*) >= 2
"""


def _ensure_view_or_drop_table(conn: sqlite3.Connection, name: str) -> None:
    """Подготовка к CREATE VIEW name: если в БД уже сидит TABLE с тем же
    именем (от прошлого refresh_materialized), сбрасываем её.

    SQLite строг: DROP TABLE на view возвращает «use DROP VIEW», и наоборот.
    Поэтому смотрим тип в sqlite_master и зовём правильный DROP.
    """
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name=?", (name,)
    ).fetchone()
    if row is None:
        return
    obj_type = row[0]
    if obj_type == "table":
        conn.execute(f"DROP TABLE {name}")
    elif obj_type == "view":
        conn.execute(f"DROP VIEW {name}")


def _create_views(conn: sqlite3.Connection) -> None:
    """Создаёт today_all / today_one_room / flat_sparkline_30d как VIEW.

    Вызывается из apply_schema на каждом скане. refresh_materialized потом
    переписывает их в TABLE (быстрее чтение), но во время скана и до его
    завершения это VIEW — иначе между scan-стартом и scan-концом /pik/today_all
    отдавал бы 404 или stale-table.
    """
    cutoff = (datetime.now(_MSK) - timedelta(days=30)).strftime("%Y-%m-%d")
    for name, sql in [
        ("today_all", _TODAY_ALL_SELECT),
        ("today_one_room", _TODAY_ONE_ROOM_SELECT),
        ("flat_sparkline_30d", _sparkline_select(cutoff)),
    ]:
        _ensure_view_or_drop_table(conn, name)
        conn.execute(f"CREATE VIEW {name} AS {sql}")


def refresh_materialized(conn: sqlite3.Connection) -> None:
    """Заменяет today_all / today_one_room / flat_sparkline_30d из VIEW в TABLE.

    Вызывать в КОНЦЕ скана, после `upsert()` (иначе материализуем устаревший
    снапшот). Транзакция атомарна (BEGIN IMMEDIATE): WAL-readers видят либо
    предыдущую версию целиком, либо новую — никогда промежуточное состояние.

    Идемпотентна: можно вызвать второй раз подряд (первый раз view→table,
    второй — table→table, обновлённая из base flats/snapshots).
    """
    cutoff = (datetime.now(_MSK) - timedelta(days=30)).strftime("%Y-%m-%d")
    sources = [
        # ВАЖЕН ПОРЯДОК: today_one_room SELECT'ит из today_all,
        # поэтому today_all должен сущ. как table к моменту его билда.
        ("today_all", _TODAY_ALL_SELECT,
         ["CREATE INDEX idx_today_all_dev ON today_all(застройщик)",
          "CREATE INDEX idx_today_all_block ON today_all(block_id)"]),
        ("today_one_room", _TODAY_ONE_ROOM_SELECT, []),
        ("flat_sparkline_30d", _sparkline_select(cutoff),
         ["CREATE INDEX idx_spark_flat ON flat_sparkline_30d(flat_id)"]),
    ]
    cur = conn.cursor()
    cur.execute("BEGIN IMMEDIATE")
    try:
        # Фаза 1: сносим ВСЕ существующие объекты с этими именами заранее.
        # Иначе при ALTER TABLE RENAME (SQLite >= 3.25) для today_all SQLite
        # верифицирует ссылки в today_one_room view и падает на "no such
        # table". Используем _ensure_view_or_drop_table (он сам различает
        # view/table по sqlite_master — DROP VIEW на table даёт OperationalError
        # и наоборот, поэтому простой DROP VIEW IF EXISTS тут не годится).
        for name, _, _ in sources:
            _ensure_view_or_drop_table(conn, name)
        # Фаза 2: для каждого — build _new, drop остатки (`_new` от прерванной
        # сессии), rename.
        for name, sel, idxs in sources:
            cur.execute(f"DROP TABLE IF EXISTS {name}_new")
            cur.execute(f"CREATE TABLE {name}_new AS {sel}")
            cur.execute(f"ALTER TABLE {name}_new RENAME TO {name}")
            # Индексы — после rename, имя ссылается на финальное.
            # SQLite < 3.35 не переименовывает индексы автоматически.
            for idx_sql in idxs:
                cur.execute(idx_sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def apply_schema(conn: sqlite3.Connection) -> None:
    # WAL: писатель (скан) и читатели (Datasette) не блокируют друг друга.
    # Режим персистентен в файле БД. Для :memory: PRAGMA молча остаётся
    # 'memory' — исключения не будет.
    conn.execute("PRAGMA journal_mode=WAL")
    _migrate_snapshots(conn)
    _migrate_blocks(conn)
    _migrate_flats(conn)
    sql = files("pik").joinpath("schema.sql").read_text(encoding="utf-8")
    conn.executescript(sql)
    # Post-hoc: для FSK/Донстрой (есть lat/lng, нет metro в API) — назначаем
    # nearest. Выполняется после executescript, чтобы view today_all уже
    # подхватила свежие metro_name через b.metro_name.
    _assign_nearest_metro(conn)
    # views — единый source-of-truth в Python (см. _TODAY_ALL_SELECT etc.).
    # На fresh DB создаёт VIEW; через refresh_materialized → TABLE в конце скана.
    _create_views(conn)
    conn.commit()


_FLAT_COLS = (
    "id", "guid", "block_id", "bulk_id", "section_id", "layout_id",
    "bulk_name", "section_no", "floor", "rooms", "rooms_fact", "is_studio",
    "area", "area_kitchen", "area_living", "number", "name", "url",
    "pdf_url", "plan_url", "ceiling_height", "settlement_date", "first_seen",
    "is_apartment",
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


# Колонки flats/snapshots, добавленные миграциями — у внешних вызывающих
# (старые скан-модули, merge с легаси-БД, тесты) их в dict может не быть.
# Дополняем дефолтом чтобы named-параметры sqlite3 не падали с ProgrammingError
# или NOT NULL IntegrityError.
_FLAT_DEFAULTS = {"is_apartment": 0}
# snapshots.has_promo: NOT NULL DEFAULT 0 в миграции. Если row пришёл
# из БД, где колонка ещё не существовала, явный 0 защищает от Integrity.
_SNAP_DEFAULTS = {"has_promo": 0}


def record_scan_run(
    conn: sqlite3.Connection, *,
    developer: str, scan_date: str, scan_ts: str,
    n_blocks: int = 0, n_flats: int = 0,
    duration_s: float | None = None,
    status: str = "ok", error_msg: str | None = None,
) -> None:
    """Записать результат скана в scan_runs (UPSERT по scan_date+developer).

    Повторный вызов в тот же день затирает прошлый — это нормально
    (manual rerun должен перезатирать запись). Поле scan_ts всегда — момент
    ПОСЛЕДНЕГО завершения, который и интересует мониторинг.
    """
    conn.execute(
        "INSERT INTO scan_runs (scan_date, scan_ts, developer, n_blocks, "
        "n_flats, duration_s, status, error_msg) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(scan_date, developer) DO UPDATE SET "
        "scan_ts=excluded.scan_ts, n_blocks=excluded.n_blocks, "
        "n_flats=excluded.n_flats, duration_s=excluded.duration_s, "
        "status=excluded.status, error_msg=excluded.error_msg",
        (scan_date, scan_ts, developer, n_blocks, n_flats,
         duration_s, status, error_msg),
    )
    conn.commit()


def upsert(
    conn: sqlite3.Connection,
    *,
    flats: Iterable[dict],
    snapshots: Iterable[dict],
    manage_transaction: bool = True,
) -> None:
    # Материализуем сразу: API типизирует параметры как Iterable, но раньше
    # код «потреблял» flats дважды — сначала проходом для setdefault, потом
    # в executemany. Если вызывающий передал генератор, второй проход видел
    # пустую последовательность и квартиры тихо пропадали. Сейчас контракт
    # явно требует list — но удержим устойчивость.
    #
    # manage_transaction=False: вызывающий уже открыл BEGIN и сам сделает
    # commit/rollback — нужно, чтобы блок-мета и flats/snapshots застройщика
    # легли ОДНОЙ транзакцией (см. scan_dev.run_developer, R5/R16). Тогда
    # эта функция только добавляет executemany в уже открытую транзакцию.
    flat_rows = list(flats)
    snap_rows = list(snapshots)
    for row in flat_rows:
        for k, v in _FLAT_DEFAULTS.items():
            row.setdefault(k, v)
    cur = conn.cursor()
    if manage_transaction:
        cur.execute("BEGIN")
    try:
        cur.executemany(_FLATS_INSERT, flat_rows)
        cur.executemany(_SNAP_INSERT, snap_rows)
        if manage_transaction:
            conn.commit()
    except Exception:
        if manage_transaction:
            conn.rollback()
        raise
