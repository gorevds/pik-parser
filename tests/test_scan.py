"""Тесты deprecation-шима bin/scan.py.

До R2 (2026-05-25) этот файл содержал тесты внутренностей PIK-сканера
(_parse_block_ids, _block_ids_from_db, run_sweep, BlockData NamedTuple).
После рефактора PIK живёт в pik/sources/pik.py, тестируется в
test_sprint2_fixes.py. Здесь — тонкий проверочный слой что shim
правильно переводит legacy-CLI в новую команду.
"""
from __future__ import annotations

import sqlite3

from bin import scan
from pik.blocks_meta import upsert_block_meta
from pik.store import apply_schema


def test_shim_delegates_to_scan_dev_with_pik_developer(monkeypatch, tmp_path):
    """`bin.scan` любой вариант → `bin.scan_dev --developer ПИК`."""
    captured: dict[str, list[str]] = {}

    def fake_main(argv):
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr("bin.scan.scan_dev.main", fake_main)
    rc = scan.main(["--db", str(tmp_path / "x.db"), "--all-blocks", "--workers", "4"])
    assert rc == 0
    argv = captured["argv"]
    assert "--developer" in argv
    assert argv[argv.index("--developer") + 1] == "ПИК"
    assert "--workers" in argv
    assert argv[argv.index("--workers") + 1] == "4"


def test_shim_with_explicit_block_id_still_delegates(monkeypatch, tmp_path):
    """Старый --block-id игнорируется в shim-режиме (адаптер сам берёт из БД)."""
    captured: dict[str, list[str]] = {}
    monkeypatch.setattr("bin.scan.scan_dev.main",
                        lambda argv: captured.setdefault("argv", list(argv)) or 0)
    scan.main(["--db", str(tmp_path / "x.db"), "--block-id", "1165"])
    assert "--developer" in captured["argv"]


def test_pik_block_ids_from_db_helper(tmp_path):
    """PIK блок-id'ы из БД для scan_dev._pik_collect_for_known_blocks.

    PIK имеет offset 0 в id-namespace (id < ID_NAMESPACE); не-PIK
    застройщики выше. Сканер должен брать только PIK-id чтобы не
    скормить чужой namespaced id в api.pik.ru.
    """
    from bin.scan_dev import _pik_collect_for_known_blocks
    from pik.developers import namespaced_id

    db = tmp_path / "pik.db"
    conn = sqlite3.connect(db)
    apply_schema(conn)
    # PIK блок (id < ID_NAMESPACE)
    upsert_block_meta(conn, block_id=1165, name="Нарвин", slug=None,
                      meta={}, scan_ts="t", developer="ПИК")
    # FSK блок (id выше — не должен попасть в PIK выборку)
    fsk_id = namespaced_id("ГК ФСК", 138935)
    upsert_block_meta(conn, block_id=fsk_id, name="ФСК-ЖК", slug=None,
                      meta={}, scan_ts="t", developer="ГК ФСК")
    conn.close()

    # Замокаем pik_source.collect чтобы не делать HTTP — нам важен лишь
    # что коллектор был вызван ровно с PIK-id'ами.
    import pik.sources.pik as pik_source
    called_with: dict[str, list[int]] = {}

    def fake_collect(*, block_ids, **kw):
        called_with["block_ids"] = list(block_ids)
        from pik.sources.base import CollectResult
        return CollectResult(blocks=[], flats=[])

    original = pik_source.collect
    pik_source.collect = fake_collect
    try:
        _pik_collect_for_known_blocks(db)
    finally:
        pik_source.collect = original

    assert called_with["block_ids"] == [1165]  # ТОЛЬКО PIK
    assert fsk_id not in called_with["block_ids"]


def test_pik_collect_helper_empty_db_returns_empty(tmp_path):
    """БД без PIK-блоков → пустой CollectResult, без HTTP-обращений."""
    from bin.scan_dev import _pik_collect_for_known_blocks
    db = tmp_path / "pik.db"
    conn = sqlite3.connect(db)
    apply_schema(conn)
    conn.close()
    result = _pik_collect_for_known_blocks(db)
    assert result.blocks == []
    assert result.flats == []


def test_pik_collect_helper_no_db_file_returns_empty(tmp_path):
    """Несуществующий файл БД → пустой CollectResult, без падения."""
    from bin.scan_dev import _pik_collect_for_known_blocks
    result = _pik_collect_for_known_blocks(tmp_path / "missing.db")
    assert result.blocks == []
    assert result.flats == []
