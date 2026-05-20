"""Тесты для bin/scan.py: парсинг block_id, чтение из БД, параллельный обход."""
import sqlite3

from bin.scan import (
    BlockData,
    _block_ids_from_db,
    _parse_block_ids,
    main,
    run_sweep,
)
from pik.blocks_meta import upsert_block_meta
from pik.client import PikApiError
from pik.store import apply_schema


def _flat(flat_id: int, block_id: int) -> dict:
    return {
        "id": flat_id, "guid": f"g{flat_id}", "block_id": block_id,
        "bulk_id": None, "section_id": None, "layout_id": None,
        "bulk_name": None, "section_no": None, "floor": 5,
        "rooms": "1", "rooms_fact": 1, "is_studio": 0,
        "area": 33.0, "area_kitchen": 8.0, "area_living": 16.0,
        "number": "1", "name": "n", "url": "u", "pdf_url": None,
        "plan_url": None, "ceiling_height": 2.7, "settlement_date": None,
        "first_seen": "2026-05-20",
    }


def _snap(flat_id: int, scan_date: str) -> dict:
    return {
        "flat_id": flat_id, "scan_date": scan_date, "scan_ts": "ts",
        "status": "free", "price": 1, "meter_price": 1,
        "base_meter_price": 1, "promo_price": 1, "discount_pct": 0.0,
        "has_promo": 0, "old_price": None, "discount": 0, "finish": None,
        "mortgage_min_rate": None, "mortgage_best_name": None, "updated_at": None,
    }


# --- _parse_block_ids ---------------------------------------------------

def test_parse_block_ids_single():
    assert _parse_block_ids("1165") == [1165]


def test_parse_block_ids_multiple():
    assert _parse_block_ids("1,2,3") == [1, 2, 3]


def test_parse_block_ids_strips_whitespace_and_blanks():
    assert _parse_block_ids(" 1 , 2 ,") == [1, 2]
    assert _parse_block_ids("1,,2") == [1, 2]


def test_parse_block_ids_empty():
    assert _parse_block_ids("") == []
    assert _parse_block_ids("   ") == []


# --- _block_ids_from_db -------------------------------------------------

def test_block_ids_from_db_missing_file(tmp_path):
    assert _block_ids_from_db(tmp_path / "nope.db") == []


def test_block_ids_from_db_empty(tmp_path):
    db = tmp_path / "pik.db"
    conn = sqlite3.connect(db)
    apply_schema(conn)
    conn.close()
    assert _block_ids_from_db(db) == []


def test_block_ids_from_db_returns_sorted_ids(tmp_path):
    db = tmp_path / "pik.db"
    conn = sqlite3.connect(db)
    apply_schema(conn)
    for bid in (1165, 47, 999):
        upsert_block_meta(
            conn, block_id=bid, name=f"b{bid}", slug=None, meta={}, scan_ts="t"
        )
    conn.close()
    assert _block_ids_from_db(db) == [47, 999, 1165]


# --- run_sweep ----------------------------------------------------------

def test_run_sweep_writes_every_block(tmp_path, monkeypatch):
    db = tmp_path / "pik.db"

    def fake_fetch(block_id, *, scan_date, scan_ts):
        fid = block_id * 10
        return BlockData(
            block_id, 1, [_flat(fid, block_id)], [_snap(fid, scan_date)],
            f"ЖК{block_id}", None, {},
        )

    monkeypatch.setattr("bin.scan.fetch_block", fake_fetch)
    failed = run_sweep(db, [101, 202, 303], workers=3)

    assert failed == 0
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM flats").fetchone()[0] == 3
    blocks = {r[0] for r in conn.execute("SELECT block_id FROM flats")}
    conn.close()
    assert blocks == {101, 202, 303}


def test_run_sweep_counts_failed_blocks(tmp_path, monkeypatch):
    db = tmp_path / "pik.db"

    def fake_fetch(block_id, *, scan_date, scan_ts):
        if block_id == 202:
            raise PikApiError("boom")
        fid = block_id * 10
        return BlockData(
            block_id, 1, [_flat(fid, block_id)], [_snap(fid, scan_date)],
            f"ЖК{block_id}", None, {},
        )

    monkeypatch.setattr("bin.scan.fetch_block", fake_fetch)
    failed = run_sweep(db, [101, 202, 303], workers=3)

    assert failed == 1
    conn = sqlite3.connect(db)
    # упавший ЖК не записан, два других — записаны
    assert conn.execute("SELECT COUNT(*) FROM flats").fetchone()[0] == 2
    conn.close()


# --- main exit code -----------------------------------------------------

def test_main_tolerates_minority_failures(tmp_path, monkeypatch):
    monkeypatch.setattr("bin.scan.run_sweep", lambda *a, **k: 1)
    rc = main(["--db", str(tmp_path / "x.db"), "--block-id", "1,2,3,4,5"])
    assert rc == 0  # 1 из 5 — не повод валить юнит


def test_main_fails_on_majority_failures(tmp_path, monkeypatch):
    monkeypatch.setattr("bin.scan.run_sweep", lambda *a, **k: 3)
    rc = main(["--db", str(tmp_path / "x.db"), "--block-id", "1,2,3,4,5"])
    assert rc == 1  # 3 из 5 — систематический сбой
