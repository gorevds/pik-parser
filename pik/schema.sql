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
    price            INTEGER,
    meter_price      INTEGER,
    old_price        INTEGER,
    discount         INTEGER,
    finish           TEXT,
    mortgage_min_rate REAL,
    mortgage_best_name TEXT,
    updated_at       TEXT,
    PRIMARY KEY (flat_id, scan_date),
    FOREIGN KEY (flat_id) REFERENCES flats(id)
);

CREATE INDEX IF NOT EXISTS idx_snap_date  ON snapshots(scan_date);
CREATE INDEX IF NOT EXISTS idx_flat_rooms ON flats(rooms);

DROP VIEW IF EXISTS today_one_room;
CREATE VIEW today_one_room AS
SELECT
    f.id                      AS id,
    f.bulk_name               AS корпус,
    f.section_no              AS секция,
    f.floor                   AS этаж,
    f.area                    AS "площадь_м²",
    s.price                   AS цена,
    s.meter_price             AS "цена_за_м²",
    s.old_price               AS старая_цена,
    s.discount                AS скидка,
    s.status                  AS статус,
    s.finish                  AS отделка,
    s.mortgage_min_rate       AS "мин_ставка_%",
    s.mortgage_best_name      AS программа,
    f.settlement_date         AS заселение,
    f.name                    AS артикул,
    f.url                     AS ссылка,
    f.plan_url                AS планировка,
    s.scan_date               AS дата_среза
FROM flats f
JOIN snapshots s ON s.flat_id = f.id
WHERE s.scan_date = (
    SELECT MAX(scan_date) FROM snapshots
)
AND f.rooms = '1';
