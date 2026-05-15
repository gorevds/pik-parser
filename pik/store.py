from importlib.resources import files
import sqlite3


def apply_schema(conn: sqlite3.Connection) -> None:
    sql = files("pik").joinpath("schema.sql").read_text(encoding="utf-8")
    conn.executescript(sql)
    conn.commit()
