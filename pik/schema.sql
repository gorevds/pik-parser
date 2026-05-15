-- Справочник ЖК: id (PIK block_id) → имя/url, для UI и мульти-проектной витрины
CREATE TABLE IF NOT EXISTS blocks (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL,
    slug  TEXT,
    updated_at TEXT
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

-- Сегодняшний срез: ВСЕ квартиры со всеми ценами (база + с программой) +
-- по всем ЖК. Колонка `жк` = название проекта из блока (если зарегистрирован).
DROP VIEW IF EXISTS today_all;
CREATE VIEW today_all AS
SELECT
    f.id                      AS id,
    COALESCE(b.name, 'block ' || f.block_id) AS жк,
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
    f.name                    AS артикул,
    f.url                     AS ссылка,
    f.plan_url                AS планировка,
    s.scan_date               AS дата_среза,
    f.block_id                AS block_id
FROM flats f
JOIN snapshots s ON s.flat_id = f.id
LEFT JOIN blocks b ON b.id = f.block_id
WHERE s.scan_date = (
    SELECT MAX(s2.scan_date)
    FROM snapshots s2
    JOIN flats f2 ON f2.id = s2.flat_id
    WHERE f2.block_id = f.block_id
);

-- Обратная совместимость: тот же набор колонок, но только 1-к
DROP VIEW IF EXISTS today_one_room;
CREATE VIEW today_one_room AS
SELECT * FROM today_all WHERE комнат = '1к';
