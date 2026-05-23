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

-- Свежий срез: ВСЕ квартиры со всеми ценами (база + с программой) по всем ЖК.
-- Для каждого ЖК берётся ЕГО последний скан (а не глобальный максимум по всей
-- таблице) — иначе ЖК, отсканированный раньше других, целиком исчезает из
-- витрины. Колонка `жк` = название проекта из блока (если зарегистрирован).
DROP VIEW IF EXISTS today_all;
CREATE VIEW today_all AS
WITH block_latest AS (
    SELECT f.block_id AS block_id, MAX(s.scan_date) AS scan_date
    FROM snapshots s
    JOIN flats f ON f.id = s.flat_id
    GROUP BY f.block_id
)
SELECT
    f.id                      AS id,
    COALESCE(b.developer, 'ПИК') AS застройщик,
    COALESCE(b.name, 'block ' || f.block_id) AS жк,
    -- город — из b.city (заполняется при сканировании из адреса; см.
    -- pik.geo.city_from_address). Для строк до миграции, где city не
    -- проставлен, считаем 'msk' (так было до мульти-города).
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
    f.bulk_name               AS корпус,
    f.section_no              AS секция,
    f.floor                   AS этаж,
    f.area                    AS "площадь_м²",
    s.price                   AS базовая_цена,
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
LEFT JOIN blocks b ON b.id = f.block_id;

-- Обратная совместимость: тот же набор колонок, но только 1-к
DROP VIEW IF EXISTS today_one_room;
CREATE VIEW today_one_room AS
SELECT * FROM today_all WHERE комнат = '1к';
