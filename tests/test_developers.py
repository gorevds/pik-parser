"""Тесты реестра застройщиков и неймспейсинга id."""
import sqlite3

import pytest

from pik.developers import (
    DEVELOPERS,
    ID_NAMESPACE,
    namespaced_id,
    split_id,
    stable_int_id,
)
from pik.store import apply_schema


def test_pik_offset_is_zero_native_ids_unchanged():
    # ПИК = offset 0 → id остаётся нативным, миграция старой БД не нужна.
    assert namespaced_id("ПИК", 980273) == 980273


def test_namespaced_id_disjoint_ranges():
    ids = [namespaced_id(dev, 5) for dev in DEVELOPERS]
    assert len(set(ids)) == len(ids)  # ни одного пересечения


def test_namespaced_id_round_trips():
    for dev in DEVELOPERS:
        gid = namespaced_id(dev, 12345)
        assert split_id(gid) == (dev, 12345)


def test_namespaced_id_rejects_unknown_developer():
    with pytest.raises(ValueError):
        namespaced_id("Неведомый", 1)


def test_namespaced_id_rejects_out_of_range():
    with pytest.raises(ValueError):
        namespaced_id("Самолёт", ID_NAMESPACE)
    with pytest.raises(ValueError):
        namespaced_id("Самолёт", -1)


def test_stable_int_id_deterministic_and_in_range():
    a = stable_int_id("samolet/nekrasovka-korpus-3")
    b = stable_int_id("samolet/nekrasovka-korpus-3")
    assert a == b
    assert 0 <= a < ID_NAMESPACE


def test_stable_int_id_distinct_keys_differ():
    assert stable_int_id("flat-a") != stable_int_id("flat-b")


def test_migration_adds_developer_to_legacy_blocks(tmp_path):
    # БД до мультизастройщика: blocks без колонки developer.
    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE blocks (id INTEGER PRIMARY KEY, name TEXT NOT NULL, slug TEXT)"
    )
    conn.execute("INSERT INTO blocks (id, name) VALUES (1165, 'Нарвин')")
    conn.commit()

    apply_schema(conn)  # должна добавить developer и не упасть

    cols = {r[1] for r in conn.execute("PRAGMA table_info(blocks)")}
    assert "developer" in cols
    # существующая строка получила DEFAULT 'ПИК'
    dev = conn.execute("SELECT developer FROM blocks WHERE id=1165").fetchone()[0]
    assert dev == "ПИК"
    conn.close()
