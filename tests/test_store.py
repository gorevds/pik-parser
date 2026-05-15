from pik.store import apply_schema


def test_apply_schema_creates_tables_and_view(conn):
    apply_schema(conn)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
    )}
    assert {"flats", "snapshots", "today_one_room"}.issubset(tables)


def test_apply_schema_is_idempotent(conn):
    apply_schema(conn)
    apply_schema(conn)
