# PIK Нарвин: парсер цен — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Поднять ежедневный парсер 1-комн. квартир ЖК Нарвин с публикацией snapshot-таблицы на `https://pik.gorev.space`.

**Architecture:** Python-скрипт `scan.py` тянет `https://api.pik.ru/v2/flat?block_id=1165&types=1&page=N`, пишет в SQLite (`flats` + `snapshots` + view `today_one_room`); Datasette отдаёт БД по HTTP; Nginx с TLS проксирует субдомен. Cron 06:00 МСК, ретраи на 502, идемпотентно в рамках дня.

**Tech Stack:** Python 3.12 (stdlib `sqlite3` + `urllib`/`requests`), SQLite, Datasette 0.65, Nginx, systemd, certbot, pytest.

Спецификация: `docs/superpowers/specs/2026-05-15-pik-narvin-parser-design.md`.

---

## Раскладка файлов

| Путь | Назначение |
|------|------------|
| `pyproject.toml` | Зависимости + setuptools config |
| `.gitignore` | venv, __pycache__, *.db, data/ |
| `README.md` | Краткое описание + deploy-инструкция |
| `pik/__init__.py` | Пакет, экспортирует версию |
| `pik/schema.sql` | DDL: `flats`, `snapshots`, view `today_one_room`, индексы |
| `pik/client.py` | `fetch_block_flats(block_id, types, ua, retries) -> list[dict]` |
| `pik/mapping.py` | `to_flat_row(item)`, `to_snapshot_row(item, scan_ts)` |
| `pik/store.py` | `apply_schema(conn)`, `upsert(conn, flats, snapshots)` |
| `bin/scan.py` | CLI: parse args → client → mapping → store + log |
| `metadata.yml` | Datasette title/description/sort/SQL views |
| `tests/conftest.py` | fixtures: тестовая БД in-memory, sample item JSON |
| `tests/test_client.py` | Пагинация, ретраи через mocked transport |
| `tests/test_mapping.py` | Сырое JSON → ожидаемые dict-строки |
| `tests/test_store.py` | Применение схемы, upsert идемпотентен в рамках дня |
| `tests/fixtures/sample_flat.json` | Один реальный item из API (anonymized id) |
| `deploy/pik.service` | systemd unit для Datasette |
| `deploy/pik-scan.service` + `.timer` | systemd timer вместо cron (надёжнее) |
| `deploy/nginx-pik.gorev.space.conf` | Reverse proxy + TLS |
| `deploy/install.sh` | Документированный скрипт для развёртывания на сервере |

---

### Task 1: Инициализация проекта

**Files:**
- Create: `/home/sber/gorev/pik-parser/.gitignore`
- Create: `/home/sber/gorev/pik-parser/pyproject.toml`
- Create: `/home/sber/gorev/pik-parser/pik/__init__.py`
- Create: `/home/sber/gorev/pik-parser/README.md`

- [ ] **Step 1: Создать `.gitignore`**

```
__pycache__/
*.py[cod]
.venv/
venv/
data/
*.db
*.db-journal
*.db-wal
*.db-shm
.pytest_cache/
.coverage
```

- [ ] **Step 2: Создать `pyproject.toml`**

```toml
[project]
name = "pik-narvin-parser"
version = "0.1.0"
description = "Daily price snapshots for PIK Narvin 1-bedroom flats"
requires-python = ">=3.10"
dependencies = [
    "requests>=2.31,<3",
]

[project.optional-dependencies]
serve = ["datasette>=0.65,<1.0"]
test = ["pytest>=8.0", "responses>=0.25"]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools]
packages = ["pik"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-q"
```

- [ ] **Step 3: Создать `pik/__init__.py`**

```python
__version__ = "0.1.0"
```

- [ ] **Step 4: Создать минимальный `README.md`**

```markdown
# pik-narvin-parser

Daily snapshot of 1-bedroom flat prices in PIK Narvin residential complex.
Public dashboard: https://pik.gorev.space

## Quick start

    python3.12 -m venv venv
    . venv/bin/activate
    pip install -e .[serve,test]
    pytest
    python -m bin.scan --once
    datasette serve data/pik.db -m metadata.yml --port 5051
```

- [ ] **Step 5: Инициализировать git, первый коммит**

```bash
cd /home/sber/gorev/pik-parser
git init -b main
git add .gitignore pyproject.toml pik/__init__.py README.md docs/
git commit -m "chore: scaffold project and ship design+plan"
```

---

### Task 2: Схема БД и `apply_schema`

**Files:**
- Create: `/home/sber/gorev/pik-parser/pik/schema.sql`
- Create: `/home/sber/gorev/pik-parser/pik/store.py`
- Create: `/home/sber/gorev/pik-parser/tests/__init__.py` (пустой)
- Create: `/home/sber/gorev/pik-parser/tests/conftest.py`
- Create: `/home/sber/gorev/pik-parser/tests/test_store.py`

- [ ] **Step 1: Написать `tests/conftest.py`**

```python
import sqlite3
import pytest

@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.execute("PRAGMA foreign_keys = ON")
    yield c
    c.close()
```

- [ ] **Step 2: Написать падающий тест `tests/test_store.py`**

```python
from pik.store import apply_schema


def test_apply_schema_creates_tables_and_view(conn):
    apply_schema(conn)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
    )}
    assert {"flats", "snapshots", "today_one_room"}.issubset(tables)


def test_apply_schema_is_idempotent(conn):
    apply_schema(conn)
    apply_schema(conn)  # must not raise
```

- [ ] **Step 3: Прогнать — увидеть ImportError**

```bash
. venv/bin/activate && pytest tests/test_store.py -v
```
Expected: ModuleNotFoundError: pik.store.

- [ ] **Step 4: Написать `pik/schema.sql`**

```sql
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
```

- [ ] **Step 5: Написать `pik/store.py` с `apply_schema`**

```python
from importlib.resources import files
import sqlite3


def apply_schema(conn: sqlite3.Connection) -> None:
    sql = files("pik").joinpath("schema.sql").read_text(encoding="utf-8")
    conn.executescript(sql)
    conn.commit()
```

- [ ] **Step 6: Прогнать тесты, увидеть зелёное**

```bash
pytest tests/test_store.py -v
```
Expected: 2 passed.

- [ ] **Step 7: Коммит**

```bash
git add pik/schema.sql pik/store.py tests/__init__.py tests/conftest.py tests/test_store.py
git commit -m "feat(store): SQLite schema with flats, snapshots, today_one_room view"
```

---

### Task 3: Маппинг JSON → строки БД

**Files:**
- Create: `/home/sber/gorev/pik-parser/tests/fixtures/sample_flat.json`
- Create: `/home/sber/gorev/pik-parser/pik/mapping.py`
- Create: `/home/sber/gorev/pik-parser/tests/test_mapping.py`

- [ ] **Step 1: Сохранить sample item из реального API в фикстуру**

Запустить однократно:

```bash
mkdir -p tests/fixtures
curl -s -A "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Safari/605.1.15" \
  "https://api.pik.ru/v2/flat?block_id=1165&types=1&page=1" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(json.dumps(d['flats'][0], ensure_ascii=False, indent=2))" \
  > tests/fixtures/sample_flat.json
```

- [ ] **Step 2: Падающий тест `tests/test_mapping.py`**

```python
import json
from pathlib import Path
from pik.mapping import to_flat_row, to_snapshot_row

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "sample_flat.json").read_text("utf-8")
)


def test_to_flat_row_extracts_stable_fields():
    row = to_flat_row(FIXTURE, first_seen="2026-05-15")
    assert row["id"] == FIXTURE["id"]
    assert row["block_id"] == 1165
    assert row["bulk_id"] == FIXTURE["bulk_id"]
    assert row["floor"] == FIXTURE["floor"]
    assert row["area"] == FIXTURE["area"]
    assert row["rooms"] == str(FIXTURE["rooms"])  # always TEXT
    assert row["url"] == FIXTURE["url"]
    assert row["first_seen"] == "2026-05-15"


def test_to_snapshot_row_extracts_volatile_fields():
    row = to_snapshot_row(FIXTURE, scan_date="2026-05-15", scan_ts="2026-05-15T06:00:00+03:00")
    assert row["flat_id"] == FIXTURE["id"]
    assert row["scan_date"] == "2026-05-15"
    assert row["price"] == FIXTURE["price"]
    assert row["meter_price"] == FIXTURE["meterPrice"]
    assert row["status"] == FIXTURE["status"]


def test_to_snapshot_row_extracts_min_mortgage_rate_when_available():
    item = dict(FIXTURE)
    item["benefits"] = {
        "mortgage": [
            {"name": "Семейная 6%", "rate": 6.0, "isMain": True},
            {"name": "IT 5%",       "rate": 5.0, "isMain": False},
        ]
    }
    row = to_snapshot_row(item, scan_date="2026-05-15", scan_ts="x")
    assert row["mortgage_min_rate"] == 5.0
    assert row["mortgage_best_name"] == "IT 5%"


def test_to_snapshot_row_handles_missing_benefits():
    item = dict(FIXTURE)
    item["benefits"] = None
    row = to_snapshot_row(item, scan_date="2026-05-15", scan_ts="x")
    assert row["mortgage_min_rate"] is None
    assert row["mortgage_best_name"] is None
```

- [ ] **Step 3: Прогнать — упасть**

```bash
pytest tests/test_mapping.py -v
```
Expected: ModuleNotFoundError.

- [ ] **Step 4: Написать `pik/mapping.py`**

```python
"""JSON → SQLite row dicts."""
from typing import Any, Optional


def to_flat_row(item: dict, *, first_seen: str) -> dict:
    bulk = item.get("bulk") or {}
    section = item.get("section") or {}
    layout = item.get("layout") or {}
    return {
        "id":              item["id"],
        "guid":            item["guid"],
        "block_id":        item["block_id"],
        "bulk_id":         item.get("bulk_id"),
        "section_id":      item.get("section_id"),
        "layout_id":       item.get("layout_id"),
        "bulk_name":       bulk.get("name") if isinstance(bulk, dict) else None,
        "section_no":      section.get("number") if isinstance(section, dict) else None,
        "floor":           item.get("floor"),
        "rooms":           str(item["rooms"]) if item.get("rooms") is not None else None,
        "rooms_fact":      item.get("rooms_fact"),
        "is_studio":       item.get("is_studio"),
        "area":            item.get("area"),
        "area_kitchen":    item.get("areaKitchen"),
        "area_living":     item.get("areaLiving"),
        "number":          item.get("number"),
        "name":            item.get("name"),
        "url":             item.get("url"),
        "pdf_url":         item.get("pdf"),
        "plan_url":        (layout.get("flatLayout") if isinstance(layout, dict) else None)
                           or item.get("floorPlan"),
        "ceiling_height":  item.get("ceilingHeight"),
        "settlement_date": item.get("settlementDate") or item.get("rveDateFact") or None,
        "first_seen":      first_seen,
    }


def _best_mortgage(item: dict) -> tuple[Optional[float], Optional[str]]:
    benefits = item.get("benefits") or {}
    if not isinstance(benefits, dict):
        return None, None
    mortgages = benefits.get("mortgage") or []
    rated = [(m.get("rate"), m.get("name")) for m in mortgages
             if isinstance(m, dict) and isinstance(m.get("rate"), (int, float)) and m.get("rate") > 0]
    if not rated:
        return None, None
    rate, name = min(rated, key=lambda x: x[0])
    return float(rate), name


def to_snapshot_row(item: dict, *, scan_date: str, scan_ts: str) -> dict:
    rate, name = _best_mortgage(item)
    return {
        "flat_id":           item["id"],
        "scan_date":         scan_date,
        "scan_ts":           scan_ts,
        "status":            item.get("status"),
        "price":             item.get("price"),
        "meter_price":       item.get("meterPrice"),
        "old_price":         item.get("oldPrice"),
        "discount":          item.get("discount"),
        "finish":            item.get("finish"),
        "mortgage_min_rate": rate,
        "mortgage_best_name": name,
        "updated_at":        item.get("updatedAt"),
    }
```

- [ ] **Step 5: Прогнать тесты, увидеть зелёное**

```bash
pytest tests/test_mapping.py -v
```
Expected: 4 passed.

- [ ] **Step 6: Коммит**

```bash
git add pik/mapping.py tests/fixtures/sample_flat.json tests/test_mapping.py
git commit -m "feat(mapping): convert PIK flat JSON to flat+snapshot row dicts"
```

---

### Task 4: Upsert в БД

**Files:**
- Modify: `/home/sber/gorev/pik-parser/pik/store.py`
- Modify: `/home/sber/gorev/pik-parser/tests/test_store.py`

- [ ] **Step 1: Дописать падающие тесты в `tests/test_store.py`**

Дописать в конец файла:

```python
from pik.store import apply_schema, upsert

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
    "old_price": None, "discount": 0, "finish": "Отделка",
    "mortgage_min_rate": 6.0, "mortgage_best_name": "Семейная",
    "updated_at": "2026-05-14T10:00:00+00:00",
}


def test_upsert_inserts_new_rows(conn):
    apply_schema(conn)
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[SAMPLE_SNAPSHOT])
    assert conn.execute("SELECT COUNT(*) FROM flats").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 1


def test_upsert_is_idempotent_within_day(conn):
    apply_schema(conn)
    upsert(conn, flats=[SAMPLE_FLAT], snapshots=[SAMPLE_SNAPSHOT])
    # second run same day with new price: should overwrite snapshot, NOT duplicate
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
    rows = list(conn.execute("SELECT цена FROM today_one_room"))
    assert rows == [(11_900_000,)]
```

- [ ] **Step 2: Прогнать — увидеть провалы**

```bash
pytest tests/test_store.py -v
```
Expected: 5 ошибок про `upsert` не определён.

- [ ] **Step 3: Дополнить `pik/store.py`**

Дописать в конец файла:

```python
from typing import Iterable


_FLAT_COLS = (
    "id", "guid", "block_id", "bulk_id", "section_id", "layout_id",
    "bulk_name", "section_no", "floor", "rooms", "rooms_fact", "is_studio",
    "area", "area_kitchen", "area_living", "number", "name", "url",
    "pdf_url", "plan_url", "ceiling_height", "settlement_date", "first_seen",
)
_SNAP_COLS = (
    "flat_id", "scan_date", "scan_ts", "status", "price", "meter_price",
    "old_price", "discount", "finish", "mortgage_min_rate",
    "mortgage_best_name", "updated_at",
)


def _insert_sql(table: str, cols: tuple[str, ...], on_conflict_do: str) -> str:
    placeholders = ", ".join(f":{c}" for c in cols)
    return (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT DO UPDATE SET {on_conflict_do}"
    )


_FLATS_INSERT = _insert_sql(
    "flats",
    _FLAT_COLS,
    # обновляем всё КРОМЕ first_seen — первое появление зафиксировано навсегда
    ", ".join(f"{c}=excluded.{c}" for c in _FLAT_COLS if c not in ("id", "first_seen")),
)

_SNAP_INSERT = _insert_sql(
    "snapshots",
    _SNAP_COLS,
    ", ".join(f"{c}=excluded.{c}" for c in _SNAP_COLS if c not in ("flat_id", "scan_date")),
)


def upsert(
    conn: sqlite3.Connection,
    *,
    flats: Iterable[dict],
    snapshots: Iterable[dict],
) -> None:
    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        cur.executemany(_FLATS_INSERT, list(flats))
        cur.executemany(_SNAP_INSERT, list(snapshots))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
```

- [ ] **Step 4: Прогнать тесты, увидеть зелёное**

```bash
pytest tests/test_store.py -v
```
Expected: 7 passed.

- [ ] **Step 5: Коммит**

```bash
git add pik/store.py tests/test_store.py
git commit -m "feat(store): upsert flats and daily snapshots in one transaction"
```

---

### Task 5: HTTP-клиент PIK API

**Files:**
- Create: `/home/sber/gorev/pik-parser/pik/client.py`
- Create: `/home/sber/gorev/pik-parser/tests/test_client.py`

- [ ] **Step 1: Падающие тесты с мокированной сетью**

```python
import json
import responses

from pik.client import PikClient, PikApiError


BLOCK_URL = "https://api.pik.ru/v2/flat"


def _page(page: int, count: int, n: int):
    return {
        "block": {"id": 1165, "name": "Нарвин"},
        "count": count,
        "flats": [{"id": page * 100 + i, "guid": f"g-{page}-{i}",
                   "block_id": 1165, "bulk_id": 10395, "rooms": 1,
                   "floor": 1, "area": 30.0, "price": 1000,
                   "meterPrice": 100, "status": "free",
                   "name": "x", "url": "x", "updatedAt": "x"}
                  for i in range(n)],
    }


@responses.activate
def test_fetch_paginates_until_empty():
    responses.add(responses.GET, BLOCK_URL, json=_page(1, 75, 50), status=200)
    responses.add(responses.GET, BLOCK_URL, json=_page(2, 75, 25), status=200)
    responses.add(responses.GET, BLOCK_URL, json=_page(3, 75, 0),  status=200)

    client = PikClient(retries=0, backoff=lambda _i: 0)
    items = client.fetch_block_flats(block_id=1165, types=(1,))

    assert len(items) == 75
    assert items[0]["id"] == 100
    assert items[-1]["id"] == 224


@responses.activate
def test_fetch_retries_on_502():
    responses.add(responses.GET, BLOCK_URL, status=502)
    responses.add(responses.GET, BLOCK_URL, status=502)
    responses.add(responses.GET, BLOCK_URL, json=_page(1, 1, 1), status=200)
    responses.add(responses.GET, BLOCK_URL, json=_page(2, 1, 0), status=200)

    client = PikClient(retries=2, backoff=lambda _i: 0)
    items = client.fetch_block_flats(block_id=1165, types=(1,))

    assert len(items) == 1


@responses.activate
def test_fetch_gives_up_after_retries():
    for _ in range(4):
        responses.add(responses.GET, BLOCK_URL, status=502)

    client = PikClient(retries=2, backoff=lambda _i: 0)
    try:
        client.fetch_block_flats(block_id=1165, types=(1,))
    except PikApiError:
        return
    raise AssertionError("PikApiError not raised")


@responses.activate
def test_fetch_sends_browser_user_agent():
    responses.add(responses.GET, BLOCK_URL, json=_page(1, 0, 0), status=200)
    client = PikClient(retries=0, backoff=lambda _i: 0)
    client.fetch_block_flats(block_id=1165, types=(1,))
    ua = responses.calls[0].request.headers["User-Agent"]
    assert "Mozilla" in ua and "AppleWebKit" in ua
```

- [ ] **Step 2: Прогнать — увидеть ImportError**

```bash
pytest tests/test_client.py -v
```

- [ ] **Step 3: Написать `pik/client.py`**

```python
"""Тонкий клиент к публичному PIK JSON API."""
from __future__ import annotations
import time
from typing import Callable, Iterable

import requests


DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)
BASE_URL = "https://api.pik.ru/v2/flat"


class PikApiError(RuntimeError):
    pass


def _default_backoff(attempt: int) -> float:
    # 1s, 5s, 15s, 45s, ...
    return [1, 5, 15, 45][min(attempt, 3)]


class PikClient:
    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_UA,
        retries: int = 3,
        backoff: Callable[[int], float] = _default_backoff,
        timeout: float = 20.0,
        session: requests.Session | None = None,
    ):
        self.retries = retries
        self.backoff = backoff
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Referer": "https://www.pik.ru/",
        })

    def fetch_block_flats(
        self, *, block_id: int, types: Iterable[int] = (1,)
    ) -> list[dict]:
        items: list[dict] = []
        page = 1
        while True:
            payload = self._get_page(block_id=block_id, types=types, page=page)
            flats = payload.get("flats") or []
            if not flats:
                break
            items.extend(flats)
            page += 1
            if page > 200:
                raise PikApiError(f"pagination runaway at page {page}")
        return items

    def _get_page(self, *, block_id: int, types: Iterable[int], page: int) -> dict:
        params = {
            "block_id": block_id,
            "types": ",".join(str(t) for t in types),
            "page": page,
        }
        last_exc: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                resp = self.session.get(BASE_URL, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                last_exc = exc
            else:
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except ValueError as exc:
                        last_exc = PikApiError(f"non-JSON body: {exc}")
                elif resp.status_code in (502, 503, 504, 429):
                    last_exc = PikApiError(f"HTTP {resp.status_code}")
                else:
                    raise PikApiError(
                        f"HTTP {resp.status_code} for page={page}: "
                        f"{resp.text[:200]}"
                    )
            if attempt < self.retries:
                time.sleep(self.backoff(attempt))
        raise PikApiError(
            f"page {page} failed after {self.retries + 1} attempts: {last_exc}"
        )
```

- [ ] **Step 4: Прогнать — зелёное**

```bash
pytest tests/test_client.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Коммит**

```bash
git add pik/client.py tests/test_client.py
git commit -m "feat(client): paginated PIK API client with retry on 502/503/504"
```

---

### Task 6: CLI `bin/scan.py`

**Files:**
- Create: `/home/sber/gorev/pik-parser/bin/__init__.py` (пустой)
- Create: `/home/sber/gorev/pik-parser/bin/scan.py`

- [ ] **Step 1: Создать `bin/__init__.py` (пустой) и `bin/scan.py`**

```python
"""Однопроходный сканер: API → SQLite."""
from __future__ import annotations
import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from pik.client import PikClient, PikApiError
from pik.mapping import to_flat_row, to_snapshot_row
from pik.store import apply_schema, upsert

MSK = timezone(timedelta(hours=3))
NARVIN_BLOCK_ID = 1165


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
    )


def run_once(db_path: Path, block_id: int = NARVIN_BLOCK_ID) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("pik.scan")
    client = PikClient()

    now = datetime.now(MSK)
    scan_date = now.strftime("%Y-%m-%d")
    scan_ts = now.isoformat(timespec="seconds")

    log.info("scanning block_id=%s scan_date=%s", block_id, scan_date)
    items = client.fetch_block_flats(block_id=block_id, types=(1,))
    log.info("api returned %d items", len(items))

    flats = [to_flat_row(it, first_seen=scan_date) for it in items]
    snaps = [to_snapshot_row(it, scan_date=scan_date, scan_ts=scan_ts) for it in items]

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        apply_schema(conn)
        upsert(conn, flats=flats, snapshots=snaps)
        one_room = conn.execute(
            "SELECT COUNT(*) FROM flats f JOIN snapshots s ON s.flat_id=f.id "
            "WHERE s.scan_date=? AND f.rooms='1'",
            (scan_date,),
        ).fetchone()[0]

    log.info("stored %d flats; 1-room on витрине: %d", len(items), one_room)
    return one_room


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default="data/pik.db",
        type=Path,
        help="Path to SQLite DB (default: data/pik.db)",
    )
    parser.add_argument(
        "--block-id",
        default=NARVIN_BLOCK_ID,
        type=int,
        help="PIK block id (default: 1165 for Narvin)",
    )
    args = parser.parse_args(argv)
    try:
        run_once(args.db, args.block_id)
    except PikApiError as exc:
        logging.error("PIK API error: %s", exc)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Запустить локально по-настоящему — обратиться к live API**

```bash
. venv/bin/activate
python -m bin.scan --db data/pik.db
```
Expected: лог `api returned ~281 items` и `1-room на витрине: ~50–80`.

- [ ] **Step 3: Проверить витрину через sqlite3 shell**

```bash
sqlite3 data/pik.db "SELECT корпус, этаж, \"площадь_м²\", цена, заселение FROM today_one_room ORDER BY цена LIMIT 5;"
```
Expected: 5 строк с разумными значениями.

- [ ] **Step 4: Коммит**

```bash
git add bin/__init__.py bin/scan.py
git commit -m "feat(scan): CLI entrypoint runs one daily scan into SQLite"
```

---

### Task 7: Datasette metadata + локальный smoke

**Files:**
- Create: `/home/sber/gorev/pik-parser/metadata.yml`

- [ ] **Step 1: Написать `metadata.yml`**

```yaml
title: ЖК Нарвин — цены на 1-комн. квартиры
description_html: |
  <p>Ежедневный срез цен PIK по проекту <a href="https://www.pik.ru/narvin">Нарвин</a>.
  Сегодняшняя витрина — <a href="/pik/today_one_room">today_one_room</a>.
  Полная история — таблицы <code>flats</code> и <code>snapshots</code>.</p>
  <p>Источник: <code>api.pik.ru/v2/flat?block_id=1165&types=1</code>.
  Скан раз в сутки в 06:00 МСК. Цены могут отличаться от сайта PIK на пару часов.</p>
license: ODbL
license_url: https://opendatacommons.org/licenses/odbl/
source: api.pik.ru
source_url: https://www.pik.ru/narvin
databases:
  pik:
    tables:
      flats:
        description: Стабильные характеристики квартир (этаж, площадь, корпус)
      snapshots:
        description: Ежедневный срез — цены, статус, отделка, лучшая ипотека
        sort_desc: scan_date
    queries:
      price_history:
        title: История цены конкретной квартиры
        sql: |-
          SELECT scan_date, price, meter_price, status
          FROM snapshots
          WHERE flat_id = :flat_id
          ORDER BY scan_date
```

- [ ] **Step 2: Поставить datasette в venv**

```bash
pip install -e .[serve]
```

- [ ] **Step 3: Запустить локально**

```bash
datasette serve data/pik.db -m metadata.yml --port 5051 --host 127.0.0.1 &
sleep 2
curl -s "http://127.0.0.1:5051/pik/today_one_room.json?_size=3&_shape=array" | head -c 600
kill %1
```
Expected: JSON с тремя записями, содержащими `корпус`, `этаж`, `цена`.

- [ ] **Step 4: Коммит**

```bash
git add metadata.yml
git commit -m "feat(datasette): metadata.yml with one-click витрина URL"
```

---

### Task 8: Deploy-артефакты для сервера

**Files:**
- Create: `/home/sber/gorev/pik-parser/deploy/pik.service`
- Create: `/home/sber/gorev/pik-parser/deploy/pik-scan.service`
- Create: `/home/sber/gorev/pik-parser/deploy/pik-scan.timer`
- Create: `/home/sber/gorev/pik-parser/deploy/nginx-pik.gorev.space.conf`
- Create: `/home/sber/gorev/pik-parser/deploy/install.sh`

- [ ] **Step 1: `deploy/pik.service` — Datasette**

```ini
[Unit]
Description=Datasette serving PIK Narvin snapshots
After=network.target

[Service]
Type=simple
User=pik
Group=pik
WorkingDirectory=/opt/pik
Environment=TZ=Europe/Moscow
ExecStart=/opt/pik/venv/bin/datasette serve /opt/pik/data/pik.db \
  --metadata /opt/pik/metadata.yml \
  --host 127.0.0.1 --port 5051 \
  --setting truncate_cells_html 0 \
  --setting max_returned_rows 5000
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: `deploy/pik-scan.service` — однократный сканер**

```ini
[Unit]
Description=PIK Narvin daily price scan
After=network.target

[Service]
Type=oneshot
User=pik
Group=pik
WorkingDirectory=/opt/pik
Environment=TZ=Europe/Moscow
ExecStart=/opt/pik/venv/bin/python -m bin.scan --db /opt/pik/data/pik.db
StandardOutput=journal
StandardError=journal
```

- [ ] **Step 3: `deploy/pik-scan.timer` — ежедневно 06:00 МСК**

```ini
[Unit]
Description=Run pik-scan daily at 06:00 MSK
Requires=pik-scan.service

[Timer]
OnCalendar=*-*-* 06:00:00 Europe/Moscow
Persistent=true
RandomizedDelaySec=300

[Install]
WantedBy=timers.target
```

- [ ] **Step 4: `deploy/nginx-pik.gorev.space.conf`**

```nginx
server {
    listen 80;
    listen [::]:80;
    server_name pik.gorev.space;

    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
    }
    location / {
        return 301 https://$host$request_uri;
    }
}

server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name pik.gorev.space;

    ssl_certificate     /etc/letsencrypt/live/pik.gorev.space/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/pik.gorev.space/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

    add_header X-Frame-Options DENY always;
    add_header X-Content-Type-Options nosniff always;
    add_header Referrer-Policy strict-origin-when-cross-origin always;

    # Корень → витрина
    location = / {
        return 302 /pik/today_one_room;
    }

    location / {
        proxy_pass http://127.0.0.1:5051;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-Host $host;
        proxy_read_timeout 60s;
    }
}
```

- [ ] **Step 5: `deploy/install.sh` — документированный bootstrap**

```bash
#!/usr/bin/env bash
set -euo pipefail

# Запускать на сервере под root. Idempotent (можно перезапускать).
# Предусловия:
#   - DNS A-record pik.gorev.space -> server IP
#   - Установлены nginx, certbot, python3.12, sqlite3
#   - В рабочей директории есть актуальный клон pik-parser

REPO_DIR="${REPO_DIR:-$PWD}"
APP_DIR="/opt/pik"
SVC_USER="pik"

# 1. Системный пользователь
id -u "$SVC_USER" &>/dev/null || useradd --system --home "$APP_DIR" --shell /usr/sbin/nologin "$SVC_USER"

# 2. Раскладка
install -d -o "$SVC_USER" -g "$SVC_USER" "$APP_DIR" "$APP_DIR/data"
rsync -a --delete --exclude='data/' --exclude='.git/' --exclude='venv/' "$REPO_DIR/" "$APP_DIR/"
chown -R "$SVC_USER":"$SVC_USER" "$APP_DIR"

# 3. venv с datasette
sudo -u "$SVC_USER" python3.12 -m venv "$APP_DIR/venv"
sudo -u "$SVC_USER" "$APP_DIR/venv/bin/pip" install --upgrade pip
sudo -u "$SVC_USER" "$APP_DIR/venv/bin/pip" install -e "$APP_DIR[serve]"

# 4. systemd
install -m 644 "$APP_DIR/deploy/pik.service"      /etc/systemd/system/pik.service
install -m 644 "$APP_DIR/deploy/pik-scan.service" /etc/systemd/system/pik-scan.service
install -m 644 "$APP_DIR/deploy/pik-scan.timer"   /etc/systemd/system/pik-scan.timer
systemctl daemon-reload

# 5. Первый прогон скана (наполнить БД)
systemctl start pik-scan.service
journalctl -u pik-scan.service --no-pager | tail -20

# 6. Поднять datasette
systemctl enable --now pik.service
systemctl enable --now pik-scan.timer
systemctl status pik.service --no-pager | head -10

# 7. Nginx + certbot
install -m 644 "$APP_DIR/deploy/nginx-pik.gorev.space.conf" /etc/nginx/sites-available/pik.gorev.space
ln -sf /etc/nginx/sites-available/pik.gorev.space /etc/nginx/sites-enabled/pik.gorev.space

# Сначала включаем только HTTP (порт 80) для acme-challenge, потом certbot допишет HTTPS.
if [ ! -e /etc/letsencrypt/live/pik.gorev.space/fullchain.pem ]; then
  echo ">>> Запустите вручную: certbot --nginx -d pik.gorev.space"
  echo ">>> Затем: nginx -t && systemctl reload nginx"
  exit 0
fi

nginx -t
systemctl reload nginx
echo ">>> Готово. Откройте https://pik.gorev.space"
```

- [ ] **Step 6: Сделать install.sh исполняемым и закоммитить**

```bash
chmod +x deploy/install.sh
git add deploy/
git commit -m "deploy: systemd units, nginx config, и install.sh для pik.gorev.space"
```

---

### Task 9: README с инструкцией по deploy

**Files:**
- Modify: `/home/sber/gorev/pik-parser/README.md`

- [ ] **Step 1: Заменить README**

```markdown
# pik-narvin-parser

Daily snapshots of 1-bedroom flat prices in PIK «Нарвин» residential complex.

Public dashboard: **https://pik.gorev.space**

## Что внутри

- `pik/` — Python-пакет: HTTP-клиент к `api.pik.ru`, маппинг JSON → SQLite, upsert
- `bin/scan.py` — однопроходный сканер (одна транзакция, идемпотентен в рамках дня)
- `metadata.yml` — конфиг Datasette
- `deploy/` — systemd units + nginx config + install.sh

## Локально

    python3.12 -m venv venv
    . venv/bin/activate
    pip install -e .[serve,test]
    pytest
    python -m bin.scan --db data/pik.db
    datasette serve data/pik.db -m metadata.yml --port 5051

## На сервере

DNS-запись `pik.gorev.space → <server IP>` уже должна быть. Затем:

    sudo REPO_DIR=$PWD bash deploy/install.sh
    sudo certbot --nginx -d pik.gorev.space
    sudo nginx -t && sudo systemctl reload nginx

Скан ежедневно в 06:00 МСК через `pik-scan.timer`.
Состояние: `systemctl status pik.service pik-scan.timer`.

## Источник данных

`GET https://api.pik.ru/v2/flat?block_id=1165&types=1&page=N` (Нарвин, 281 квартира,
~6 страниц). Требует браузерный User-Agent, который выставляет клиент.

## Спецификация и план

- `docs/superpowers/specs/2026-05-15-pik-narvin-parser-design.md`
- `docs/superpowers/plans/2026-05-15-pik-narvin-parser-plan.md`
```

- [ ] **Step 2: Коммит**

```bash
git add README.md
git commit -m "docs: README with local dev и server deploy инструкции"
```

---

### Task 10: Реальный smoke на сервере

Выполняется руками пользователем или мной по запросу — **не до этого момента** без подтверждения, поскольку трогает прод-сервер.

- [ ] Скопировать репо на сервер (`rsync` или `git clone` из private remote)
- [ ] Прописать DNS `pik.gorev.space` (если ещё нет)
- [ ] Запустить `deploy/install.sh`
- [ ] `certbot --nginx -d pik.gorev.space`
- [ ] `curl -I https://pik.gorev.space/` → 302 на `/pik/today_one_room`
- [ ] Открыть в браузере, убедиться, что таблица есть и фильтры работают

---

## Self-review

**Spec coverage:**
- Источник `api.pik.ru/v2/flat?block_id=1165&types=1` → Task 5 ✓
- SQLite со схемой `flats` + `snapshots` + view → Task 2 ✓
- Маппинг → Task 3 ✓
- Upsert идемпотентно → Task 4 ✓
- CLI скан → Task 6 ✓
- Datasette + metadata → Task 7 ✓
- systemd для Datasette + cron-эквивалент → Task 8 ✓
- Nginx с TLS на pik.gorev.space → Task 8 ✓
- YAGNI (нет TG, графиков, студий на витрине) — соблюдено ✓
- Ретраи 1s/5s/15s → Task 5 (через `_default_backoff`) ✓
- Часовой пояс МСК → Task 6 (datetime aware) + Task 8 (TZ=Europe/Moscow в unit-файлах) ✓

**Placeholder scan:** Нет TBD/TODO/«implement later». Каждый шаг даёт код.

**Type consistency:** `PikClient.fetch_block_flats(*, block_id, types=(1,))` — типы и имена согласованы между Task 5 и Task 6. `apply_schema(conn)` / `upsert(conn, *, flats, snapshots)` — то же. `to_flat_row(item, *, first_seen)` и `to_snapshot_row(item, *, scan_date, scan_ts)` — соответствуют использованию в scan.py.

**Замечание:** заменил cron на systemd timer (Task 8) — он надёжнее, не нужен отдельный пользовательский crontab, journald логирует автоматически, есть `Persistent=true` для пропущенных запусков.
