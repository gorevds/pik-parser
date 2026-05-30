-- Справочник ЖК: id (block_id) → имя/url + гео-метаданные.
-- Эти поля стабильны на уровне проекта и не дублируются в snapshots/flats.
-- `developer` — застройщик ('ПИК', 'Самолёт', …); см. pik/developers.py.
-- id для не-PIK застройщиков пространственно разнесён (offset*1e12 + native_id),
-- чтобы block_id/flat_id разных застройщиков не сталкивались в общих таблицах.
CREATE TABLE IF NOT EXISTS blocks (
    id                 INTEGER PRIMARY KEY,
    name               TEXT NOT NULL,
    developer          TEXT NOT NULL DEFAULT 'ПИК',
    slug               TEXT,
    updated_at         TEXT,
    metro_name         TEXT,        -- ближайшая станция (по timeOnFoot)
    metro_line_name    TEXT,        -- название линии (Замоскворецкая / МЦК / МЦД-2)
    metro_line_type    INTEGER,     -- 1=метро 2=МЦК 3=МЦД 4=электричка
    metro_time_foot    INTEGER,     -- минут пешком до станции
    metro_time_transport INTEGER,   -- минут на транспорте (если пешком далеко)
    latitude           REAL,        -- координаты ЖК
    longitude          REAL,
    address            TEXT,
    city               TEXT,        -- код города: 'msk' | 'mo' | 'spb' | ... (см. pik.geo)
    distance_km        REAL,        -- расстояние от центра ПРАВИЛЬНОГО города (по city)
    floors_max         INTEGER      -- этажность здания
);

CREATE TABLE IF NOT EXISTS flats (
    id              INTEGER PRIMARY KEY,
    guid            TEXT NOT NULL,
    block_id        INTEGER NOT NULL,
    bulk_id         INTEGER,
    section_id      INTEGER,
    layout_id       INTEGER,
    bulk_name       TEXT,
    section_no      INTEGER,
    floor           INTEGER,
    rooms           TEXT,
    rooms_fact      INTEGER,
    is_studio       INTEGER,
    is_apartment    INTEGER NOT NULL DEFAULT 0,  -- апартаменты (нежилой фонд)
    area            REAL,
    area_kitchen    REAL,
    area_living     REAL,
    number          TEXT,
    name            TEXT,
    url             TEXT,
    pdf_url         TEXT,
    plan_url        TEXT,
    ceiling_height  REAL,
    settlement_date TEXT,
    first_seen      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    flat_id          INTEGER NOT NULL,
    scan_date        TEXT NOT NULL,
    scan_ts          TEXT NOT NULL,
    status           TEXT,
    price            INTEGER,         -- база (полная оплата)
    meter_price      INTEGER,         -- цена за м² с ипотечной программой
    base_meter_price INTEGER,         -- цена за м² при оплате налом = round(price/area)
    promo_price      INTEGER,         -- итоговая цена с программой = round(meter_price*area)
    discount_pct     REAL,            -- размер скидки от базы, 0..100
    has_promo        INTEGER NOT NULL DEFAULT 0,  -- 1 если discount_pct >= 0.5
    old_price        INTEGER,
    discount         INTEGER,
    finish           TEXT,
    mortgage_min_rate REAL,
    mortgage_best_name TEXT,
    updated_at       TEXT,
    PRIMARY KEY (flat_id, scan_date),
    FOREIGN KEY (flat_id) REFERENCES flats(id)
);

-- safe migration для существующих БД: добавляем колонки если их ещё нет
-- (apply_schema идемпотентен)

CREATE INDEX IF NOT EXISTS idx_snap_date  ON snapshots(scan_date);
CREATE INDEX IF NOT EXISTS idx_flat_rooms ON flats(rooms);
CREATE INDEX IF NOT EXISTS idx_flat_block ON flats(block_id);
CREATE INDEX IF NOT EXISTS idx_block_developer ON blocks(developer);

-- Агрегированная история из не-PIK источников: Cian, mskguru, новости и т.п.
-- Для каждой строки — на какую дату относится цена и какой ЖК.
CREATE TABLE IF NOT EXISTS history_aggregated (
    block_id        INTEGER NOT NULL,
    date            TEXT NOT NULL,
    source          TEXT NOT NULL,
    source_url      TEXT,
    rooms           TEXT NOT NULL DEFAULT 'all',
    price_min       INTEGER,
    price_max       INTEGER,
    price_avg       INTEGER,
    meter_price_min INTEGER,
    meter_price_max INTEGER,
    meter_price_avg INTEGER,
    notes           TEXT,
    PRIMARY KEY (block_id, date, source, rooms),
    FOREIGN KEY (block_id) REFERENCES blocks(id)
);

CREATE INDEX IF NOT EXISTS idx_hist_block_date ON history_aggregated(block_id, date);

-- Operational telemetry: одна строка на (scan_date, developer). Лог
-- завершения скана: сколько ЖК/квартир записано, сколько секунд заняло,
-- статус (ok|error|partial) и текст ошибки если упало.
-- Прежнее status quo: разбираться приходилось по journalctl на сервере;
-- этот реестр позволяет лэндингу/Datasette показать "последний скан был N
-- часов назад" и алерту видеть stale-data за минуты, а не за день.
CREATE TABLE IF NOT EXISTS scan_runs (
    scan_date  TEXT NOT NULL,         -- yyyy-mm-dd МСК (та же, что у snapshots)
    scan_ts    TEXT NOT NULL,         -- ISO8601 МСК — момент завершения
    developer  TEXT NOT NULL,         -- 'ПИК' | 'А101' | … | '_all_' для bin/scan
    n_blocks   INTEGER NOT NULL DEFAULT 0,
    n_flats    INTEGER NOT NULL DEFAULT 0,
    n_rejected INTEGER NOT NULL DEFAULT 0,  -- квартир отброшено data-quality gate
    duration_s REAL,
    status     TEXT NOT NULL,         -- 'ok' | 'error' | 'partial'
    error_msg  TEXT,
    PRIMARY KEY (scan_date, developer)
);
CREATE INDEX IF NOT EXISTS idx_scan_runs_ts ON scan_runs(scan_ts);

-- today_all / today_one_room / flat_sparkline_30d:
-- объекты создаются ИЗ Python (pik/store.py: _create_views / refresh_materialized)
-- — единый source-of-truth для SELECT-тел и MSK-окна 30 дней. Тут раньше были
-- VIEW-определения, но их пришлось вынести, чтобы те же SQL-тела можно было
-- использовать для CREATE TABLE в refresh_materialized (материализация в конце
-- скана даёт чтение 50мс вместо 4с view-evaluation в Datasette).
