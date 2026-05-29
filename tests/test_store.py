from pik.store import apply_schema, refresh_materialized, upsert

SAMPLE_FLAT = {
    "id": 100, "guid": "g-100", "block_id": 1165, "bulk_id": 10397,
    "section_id": 23643, "layout_id": 60971, "bulk_name": "Корпус 1.3",
    "section_no": 3, "floor": 7, "rooms": "1", "rooms_fact": 1, "is_studio": 0,
    "area": 33.5, "area_kitchen": 8.0, "area_living": 16.2, "number": "1",
    "name": "Нарвин-1.3(кв)", "url": "https://www.pik.ru/flat/100",
    "pdf_url": None, "plan_url": None, "ceiling_height": 2.75,
    "settlement_date": "2027-10-31T00:00:00+00:00", "first_seen": "2026-05-15",
}
SAMPLE_SNAPSHOT = {
    "flat_id": 100, "scan_date": "2026-05-15", "scan_ts": "2026-05-15T06:00+03:00",
    "status": "free", "price": 12_000_000, "meter_price": 358_209,
    "base_meter_price": 358_209, "promo_price": 12_000_000,
    "discount_pct": 0.0, "has_promo": 0,
    "old_price": None, "discount": 0, "finish": "С отделкой",
    "mortgage_min_rate": 6.0, "mortgage_best_name": "Семейная",
    "updated_at": "2026-05-14T10:00:00+00:00",
}


def test_apply_schema_creates_tables_and_view(conn):
    apply_schema(conn)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
    )}
    assert {"flats", "snapshots", "today_one_room"}.issubset(tables)


def test_apply_schema_is_idempotent(conn):
    apply_schema(conn)
    apply_schema(conn)


def test_apply_schema_sets_busy_timeout(conn):
    # R7: устойчивость к concurrent Datasette-читателю не зависит от вызывающего
    apply_schema(conn)
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30000


def test_refresh_materialized_sets_busy_timeout(conn):
    apply_schema(conn)
    conn.execute("PRAGMA busy_timeout=1000")  # сбили — refresh обязан вернуть 30000
    refresh_materialized(conn)
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30000


def test_upsert_inserts_new_rows(conn):
    apply_schema(conn)
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[SAMPLE_SNAPSHOT])
    assert conn.execute("SELECT COUNT(*) FROM flats").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 1


def test_upsert_is_idempotent_within_day(conn):
    apply_schema(conn)
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[SAMPLE_SNAPSHOT])
    snap2 = dict(SAMPLE_SNAPSHOT, price=11_500_000)
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[snap2])
    assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 1
    price = conn.execute("SELECT price FROM snapshots").fetchone()[0]
    assert price == 11_500_000


def test_upsert_preserves_first_seen_on_reinsert(conn):
    apply_schema(conn)
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[SAMPLE_SNAPSHOT])
    later = dict(SAMPLE_FLAT, first_seen="2026-05-20")
    upsert(conn, flats=[later], snapshots=[])
    assert conn.execute("SELECT first_seen FROM flats").fetchone()[0] == "2026-05-15"


def test_upsert_keeps_history_across_days(conn):
    apply_schema(conn)
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[SAMPLE_SNAPSHOT])
    snap_tomorrow = dict(SAMPLE_SNAPSHOT, scan_date="2026-05-16", price=11_900_000)
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[snap_tomorrow])
    assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 2


def test_today_view_returns_latest_only(conn):
    apply_schema(conn)
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[SAMPLE_SNAPSHOT])
    snap_tomorrow = dict(SAMPLE_SNAPSHOT, scan_date="2026-05-16", price=11_900_000)
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[snap_tomorrow])
    rows = list(conn.execute("SELECT базовая_цена FROM today_one_room"))
    assert rows == [(11_900_000,)]


def test_today_view_shows_each_block_at_its_own_latest(conn):
    """Каждый ЖК показывается на свой последний скан, не на глобальный max.

    ЖК, отсканированный раньше другого, не должен исчезать из витрины.
    """
    apply_schema(conn)
    # block 1165: отсканирован 2026-05-15 и 2026-05-20
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[SAMPLE_SNAPSHOT])
    upsert(
        conn, flats=[SAMPLE_FLAT],
        snapshots=[dict(SAMPLE_SNAPSHOT, scan_date="2026-05-20", price=13_000_000)],
    )
    # block 999: отсканирован только 2026-05-15 (раньше глобального max)
    flat_b = dict(SAMPLE_FLAT, id=200, guid="g-200", block_id=999)
    snap_b = dict(SAMPLE_SNAPSHOT, flat_id=200, price=9_000_000)
    upsert(conn, flats=[flat_b], snapshots=[snap_b])

    rows = {
        bid: (date, price)
        for bid, date, price in conn.execute(
            "SELECT block_id, дата_среза, базовая_цена FROM today_all"
        )
    }
    assert rows[1165] == ("2026-05-20", 13_000_000)
    assert rows[999] == ("2026-05-15", 9_000_000)  # не исчез, хоть скан и старше


def test_apply_schema_migrates_old_db_adding_promo_columns(conn):
    """БД из 0.1.0 (без promo-колонок) не должна крашить apply_schema."""
    conn.executescript(
        """
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
            flat_id INTEGER NOT NULL, scan_date TEXT NOT NULL, scan_ts TEXT NOT NULL,
            status TEXT, price INTEGER, meter_price INTEGER, old_price INTEGER,
            discount INTEGER, finish TEXT, mortgage_min_rate REAL,
            mortgage_best_name TEXT, updated_at TEXT,
            PRIMARY KEY (flat_id, scan_date)
        );
        """
    )
    apply_schema(conn)  # must add missing columns without error
    cols = {row[1] for row in conn.execute("PRAGMA table_info(snapshots)")}
    assert {"base_meter_price", "promo_price", "discount_pct", "has_promo"}.issubset(cols)
