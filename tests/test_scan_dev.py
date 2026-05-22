"""Тесты bin/scan_dev.py: параллельный обход застройщиков."""
import sqlite3

import pytest

from bin import scan_dev
from pik.sources.base import CollectResult, NormBlock, NormFlat, SourceError


def _fake_result(tag: str) -> CollectResult:
    return CollectResult(
        blocks=[NormBlock(native_id=f"{tag}-zhk", name=f"ЖК {tag}", slug=tag)],
        flats=[NormFlat(native_id=f"{tag}-1", native_block_id=f"{tag}-zhk",
                        rooms=1, area=40.0, floor=3, price=10_000_000)],
    )


def test_run_sweep_writes_every_developer(tmp_path, monkeypatch):
    db = tmp_path / "multi.db"
    monkeypatch.setattr(scan_dev, "SOURCES", {
        "ГК ФСК": lambda: _fake_result("fsk"),
        "Донстрой": lambda: _fake_result("don"),
        "А101": lambda: _fake_result("a101"),
    })
    failed = scan_dev.run_sweep(
        db, ["ГК ФСК", "Донстрой", "А101"],
        scan_date="2026-05-22", scan_ts="t", workers=3,
    )
    assert failed == 0
    conn = sqlite3.connect(db)
    devs = {r[0] for r in conn.execute("SELECT developer FROM blocks")}
    assert devs == {"ГК ФСК", "Донстрой", "А101"}
    assert conn.execute("SELECT COUNT(*) FROM flats").fetchone()[0] == 3
    assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 3
    conn.close()


def test_run_sweep_counts_failed_sources(tmp_path, monkeypatch):
    def boom():
        raise SourceError("источник недоступен")

    monkeypatch.setattr(scan_dev, "SOURCES", {
        "ГК ФСК": lambda: _fake_result("fsk"),
        "Самолёт": boom,
    })
    failed = scan_dev.run_sweep(
        tmp_path / "m.db", ["ГК ФСК", "Самолёт"],
        scan_date="d", scan_ts="t", workers=2,
    )
    assert failed == 1
    conn = sqlite3.connect(tmp_path / "m.db")
    # успешный застройщик записан, упавший — нет
    assert conn.execute("SELECT COUNT(*) FROM flats").fetchone()[0] == 1
    conn.close()


def test_main_rejects_unknown_developer(tmp_path):
    with pytest.raises(SystemExit):
        scan_dev.main(["--db", str(tmp_path / "x.db"), "--developer", "Неведомый"])


def test_main_succeeds_when_all_developers_ok(tmp_path, monkeypatch):
    monkeypatch.setattr(scan_dev, "run_sweep", lambda *a, **k: 0)
    monkeypatch.setattr(scan_dev, "SOURCES", dict.fromkeys("abcde"))
    rc = scan_dev.main(["--db", str(tmp_path / "x.db"), "--all"])
    assert rc == 0


def test_main_fails_on_any_developer_failure(tmp_path, monkeypatch):
    # источников мало — сбой даже одного это потеря данных по застройщику
    # за сутки, юнит обязан стать failed
    monkeypatch.setattr(scan_dev, "run_sweep", lambda *a, **k: 1)
    monkeypatch.setattr(scan_dev, "SOURCES", dict.fromkeys("abcde"))
    rc = scan_dev.main(["--db", str(tmp_path / "x.db"), "--all"])
    assert rc == 1


def test_sources_registry_within_developer_registry():
    """Каждый источник обязан быть зарегистрирован в pik.developers."""
    from pik.developers import DEVELOPERS

    assert set(scan_dev.SOURCES) <= set(DEVELOPERS)
