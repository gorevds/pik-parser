"""Тесты velocity: жизненный цикл лотов и скорость продаж по ЖК."""
import sqlite3

from pik.store import apply_schema
from pik.velocity import (
    _full_scan_dates,
    build_flat_lifecycle,
    build_velocity_tables,
)


def _conn():
    c = sqlite3.connect(":memory:")
    apply_schema(c)
    return c


def _block(c, bid, name="ЖК", dev="ПИК", city="msk"):
    c.execute(
        "INSERT INTO blocks (id, name, developer, city) VALUES (?,?,?,?)",
        (bid, name, dev, city),
    )


def _flat(c, fid, bid, rooms="1", bulk="К1"):
    c.execute(
        "INSERT INTO flats (id, guid, block_id, bulk_name, rooms, first_seen) "
        "VALUES (?,?,?,?,?,?)",
        (fid, str(fid), bid, bulk, rooms, "2026-06-01"),
    )


def _snap(c, fid, day, status="free", price=10_000_000):
    d = f"2026-06-{day:02d}"
    c.execute(
        "INSERT INTO snapshots (flat_id, scan_date, scan_ts, status, price, has_promo) "
        "VALUES (?,?,?,?,?,0)",
        (fid, d, d + "T06:00:00+03:00", status, price),
    )


def _present(c, fid, days, **kw):
    for d in days:
        _snap(c, fid, d, **kw)


def _lc(c, fid):
    row = c.execute(
        "SELECT gone, gone_date, still_listed, dom_days, ever_reserved "
        "FROM flat_lifecycle WHERE flat_id=?",
        (fid,),
    ).fetchone()
    return dict(zip(("gone", "gone_date", "still", "dom", "ever_res"), row, strict=True))


def test_gone_detected_with_dom_after_two_full_scans():
    c = _conn()
    _block(c, 1)
    # «фон» — много лотов, чтобы дни 1..5 были полными сканами
    for fid in range(100, 130):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 3, 4, 5])
    _flat(c, 10, 1)
    _present(c, 10, [1, 2, 3])  # исчез после дня 3; дни 4,5 — 2 полных скана
    build_flat_lifecycle(c)
    lc = _lc(c, 10)
    assert lc["gone"] == 1
    assert lc["gone_date"] == "2026-06-04"
    assert lc["still"] == 0
    assert lc["dom"] == 3  # 04-06 минус 01-06 = 3 дня


def test_still_listed_when_present_in_latest_scan():
    c = _conn()
    _block(c, 1)
    for fid in range(100, 130):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 3, 4, 5])
    build_flat_lifecycle(c)
    lc = _lc(c, 100)
    assert lc["still"] == 1
    assert lc["gone"] == 0
    assert lc["gone_date"] is None


def test_pending_one_scan_after_is_not_yet_gone():
    c = _conn()
    _block(c, 1)
    for fid in range(100, 130):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 3, 4, 5])
    _flat(c, 10, 1)
    _present(c, 10, [1, 2, 3, 4])  # отсутствует только день 5 → 1 полный скан после
    build_flat_lifecycle(c)
    lc = _lc(c, 10)
    assert lc["gone"] == 0   # ещё не подтверждён уход
    assert lc["still"] == 0  # но и не в последнем скане


def test_flapping_reappearance_not_counted_as_gone():
    c = _conn()
    _block(c, 1)
    for fid in range(100, 130):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 3, 4, 5])
    _flat(c, 10, 1)
    _present(c, 10, [1, 2, 4, 5])  # пропал в день 3, вернулся в 4 → last_seen=5
    build_flat_lifecycle(c)
    lc = _lc(c, 10)
    assert lc["gone"] == 0
    assert lc["still"] == 1


def test_partial_scan_day_excluded_from_full_dates():
    c = _conn()
    _block(c, 1)
    # дни 1,2,4,5 полные (30 лотов), день 3 частичный (только 2 лота)
    for fid in range(100, 130):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 4, 5])
    _flat(c, 200, 1)
    _flat(c, 201, 1)
    _present(c, 200, [1, 2, 3, 4, 5])
    _present(c, 201, [1, 2, 3, 4, 5])
    full = _full_scan_dates(c)
    assert "2026-06-03" not in full["ПИК"]  # частичный день отсеян
    assert full["ПИК"] == ["2026-06-01", "2026-06-02", "2026-06-04", "2026-06-05"]


def test_gone_date_skips_partial_day():
    c = _conn()
    _block(c, 1)
    # фон присутствует в полные дни 1,2,4,5; день 3 частичный
    for fid in range(100, 130):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 4, 5])
    # пара лотов держит день 3 «живым» (частичным), но не полным
    for fid in (200, 201):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 3, 4, 5])
    _flat(c, 10, 1)
    _present(c, 10, [1, 2])  # ушёл после дня 2; полные после: 4,5 (день 3 отсеян)
    build_flat_lifecycle(c)
    lc = _lc(c, 10)
    assert lc["gone"] == 1
    assert lc["gone_date"] == "2026-06-04"  # не 06-03 (частичный)


def test_ever_reserved_flag():
    c = _conn()
    _block(c, 1)
    for fid in range(100, 130):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 3, 4, 5])
    _flat(c, 10, 1)
    _snap(c, 10, 1, status="free")
    _snap(c, 10, 2, status="reserve")
    _snap(c, 10, 3, status="reserve")
    build_flat_lifecycle(c)
    assert _lc(c, 10)["ever_res"] == 1
    assert _lc(c, 100)["ever_res"] == 0


def test_block_velocity_aggregates_absorbed_active_new():
    c = _conn()
    _block(c, 1, name="ЖК Тест")
    # 20 лотов держатся всё время (active), 5 ушли после дня 3 (absorbed)
    for fid in range(100, 120):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 3, 4, 5])
    for fid in range(200, 205):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 3])
    build_velocity_tables(c)
    row = c.execute(
        "SELECT active_now, absorbed_30d, absorbed_7d, median_dom_days, "
        "absorption_pct_30d, total_tracked FROM block_velocity WHERE block_id=1"
    ).fetchone()
    active, abs30, abs7, mdom, pct, total = row
    assert active == 20
    assert abs30 == 5
    assert abs7 == 5
    assert mdom == 3
    assert total == 25
    assert pct == round(100 * 5 / 25, 1)  # 20.0
    status = c.execute(
        "SELECT block_status FROM block_velocity WHERE block_id=1"
    ).fetchone()[0]
    assert status == "active"  # есть активные лоты


def test_block_status_off_market_when_no_active_lots():
    c = _conn()
    _block(c, 1)
    # фон в другом ЖК держит полные сканы дней 1..5
    _block(c, 2)
    for fid in range(300, 330):
        _flat(c, fid, 2)
        _present(c, fid, [1, 2, 3, 4, 5])
    # ЖК 1: все лоты исчезли после дня 2 (ушёл с витрины целиком)
    for fid in range(100, 110):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2])
    build_velocity_tables(c)
    row = c.execute(
        "SELECT active_now, block_status FROM block_velocity WHERE block_id=1"
    ).fetchone()
    assert row[0] == 0
    assert row[1] == "off_market"


def test_inventory_daily_curve_counts_listed_per_day():
    c = _conn()
    _block(c, 1)
    for fid in range(100, 110):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 3])
    # день 1: 10 лотов, день 2: 10, день 3: 10, потом часть уходит
    for fid in range(100, 105):
        _snap(c, fid, 4)  # только 5 лотов в день 4
    build_velocity_tables(c)
    rows = dict(
        c.execute(
            "SELECT scan_date, listed FROM block_inventory_daily WHERE block_id=1"
        ).fetchall()
    )
    assert rows["2026-06-01"] == 10
    assert rows["2026-06-04"] == 5


def test_inventory_curve_excludes_partial_scan_days():
    c = _conn()
    _block(c, 1)
    # дни 1,2,4,5 полные (30 лотов), день 3 — частичный (2 лота)
    for fid in range(100, 130):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 4, 5])
    for fid in (200, 201):
        _flat(c, fid, 1)
        _present(c, fid, [1, 2, 3, 4, 5])
    build_velocity_tables(c)
    days = [r[0] for r in c.execute(
        "SELECT scan_date FROM block_inventory_daily WHERE block_id=1 ORDER BY scan_date"
    ).fetchall()]
    assert "2026-06-03" not in days  # частичный день исключён из кривой
    assert days == ["2026-06-01", "2026-06-02", "2026-06-04", "2026-06-05"]
