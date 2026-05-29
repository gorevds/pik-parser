"""PR scan-write-model: fail-loud (R1), partial status (R2), atomic
per-developer write (R5/R16), single-developer exit code (R3)."""
import sqlite3

import pytest

from bin import scan_dev
from pik.client import PikApiError
from pik.sources import pik as pik_source
from pik.sources.base import CollectResult, NormBlock, NormFlat, SourceError


def _pik_item(bid: int, fid: int) -> dict:
    return {
        "id": fid, "block_id": bid, "rooms": 1, "area": 40.0, "floor": 3,
        "price": 10_000_000, "meterPrice": 250_000,
        "block": {"name": f"ЖК {bid}", "url": f"/zhk-{bid}/"},
    }


# ── R1: тотальный отказ ПИК — это ошибка, а не «пустой успешный скан» ──────

def test_pik_collect_raises_when_every_block_fails(monkeypatch):
    def boom(block_id, types):
        raise PikApiError(f"HTTP 502 for {block_id}")

    monkeypatch.setattr(pik_source, "_fetch_one", boom)
    with pytest.raises(SourceError):
        pik_source.collect(block_ids=[101, 102, 103])


def test_pik_collect_empty_block_list_is_not_an_error(monkeypatch):
    # «в БД нет блоков ПИК» — легитимный пустой результат, НЕ SourceError
    result = pik_source.collect(block_ids=[])
    assert result.blocks == [] and result.flats == [] and result.skipped == 0


# ── R2: частичный отказ виден (skipped>0), но не валит сбор ────────────────

def test_pik_collect_partial_failure_reports_skipped(monkeypatch):
    def half(block_id, types):
        if block_id == 102:
            raise PikApiError("HTTP 502")
        return [_pik_item(block_id, block_id * 10)]

    monkeypatch.setattr(pik_source, "_fetch_one", half)
    result = pik_source.collect(block_ids=[101, 102])
    assert result.skipped == 1
    assert len(result.flats) == 1
    assert len(result.blocks) == 1


def test_run_developer_records_partial_status(tmp_path, monkeypatch):
    db = tmp_path / "p.db"
    scan_dev._ensure_schema(db)

    def partial_source():
        return CollectResult(
            blocks=[NormBlock(native_id="x-zhk", name="ЖК X", slug="x")],
            flats=[NormFlat(native_id="x-1", native_block_id="x-zhk",
                            rooms=1, area=40.0, floor=3, price=10_000_000)],
            skipped=2,
        )

    monkeypatch.setattr(scan_dev, "SOURCES", {"ГК ФСК": partial_source})
    scan_dev.run_developer(db, "ГК ФСК", scan_date="2026-05-29", scan_ts="t")
    conn = sqlite3.connect(db)
    status, err = conn.execute(
        "SELECT status, error_msg FROM scan_runs WHERE developer='ГК ФСК'"
    ).fetchone()
    conn.close()
    assert status == "partial"
    assert err and "2" in err


# ── R5/R16: блоки и квартиры застройщика пишутся одной транзакцией ─────────

def test_run_developer_write_is_atomic_on_failure(tmp_path, monkeypatch):
    """Если запись flats/snapshots падает, block-meta тоже откатывается —
    не остаётся блоков-сирот без квартир."""
    db = tmp_path / "a.db"
    scan_dev._ensure_schema(db)

    def good_source():
        return CollectResult(
            blocks=[NormBlock(native_id="y-zhk", name="ЖК Y", slug="y")],
            flats=[NormFlat(native_id="y-1", native_block_id="y-zhk",
                            rooms=1, area=40.0, floor=3, price=10_000_000)],
        )

    def boom_upsert(*a, **k):
        raise RuntimeError("disk full mid-write")

    monkeypatch.setattr(scan_dev, "SOURCES", {"ГК ФСК": good_source})
    monkeypatch.setattr(scan_dev, "upsert", boom_upsert)
    with pytest.raises(RuntimeError):
        scan_dev.run_developer(db, "ГК ФСК", scan_date="2026-05-29", scan_ts="t")
    conn = sqlite3.connect(db)
    n_blocks = conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
    conn.close()
    assert n_blocks == 0, "block rows must roll back with the failed flats write"


# ── R3: одиночный --developer возвращает ненулевой код при сбое ────────────

def test_main_single_developer_fails_loud(tmp_path, monkeypatch):
    monkeypatch.setattr(scan_dev, "run_sweep", lambda *a, **k: 1)
    monkeypatch.setattr(scan_dev, "SOURCES", {"ГК ФСК": lambda: None})
    rc = scan_dev.main(["--db", str(tmp_path / "x.db"), "--developer", "ГК ФСК"])
    assert rc == 1


def test_main_single_developer_ok_returns_zero(tmp_path, monkeypatch):
    monkeypatch.setattr(scan_dev, "run_sweep", lambda *a, **k: 0)
    monkeypatch.setattr(scan_dev, "SOURCES", {"ГК ФСК": lambda: None})
    rc = scan_dev.main(["--db", str(tmp_path / "x.db"), "--developer", "ГК ФСК"])
    assert rc == 0
