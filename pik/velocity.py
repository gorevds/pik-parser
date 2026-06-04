"""Velocity / absorption: жизненный цикл лотов и скорость продаж по ЖК.

Из append-only журнала `snapshots` восстанавливаем, что случилось с каждой
квартирой: когда впервые появилась, когда исчезла (= продана/снята), сколько
«висела» (days-on-market). Из этого считаем по ЖК скорость вымывания и темп
новых поступлений — сигнал, который инкумбенты публично НЕ отдают.

ГЛАВНАЯ ПРОБЛЕМА — целостность. Скан негарантированно ежедневный, отдельные
застройщики падают (Инград), часть дней — частичные. Поэтому «исчез» нельзя
выводить наивно «нет между двумя сканами»:

  * full-scan: день засчитывается как полный скан застройщика, только если в
    нём ≥ FULL_SCAN_FRACTION от медианного дневного числа лотов застройщика.
    Частичные/упавшие дни (Инград, OOM-обрезка) отсекаются — по ним нельзя
    судить об исчезновении.
  * gone: лот считается ушедшим, только если ПОСЛЕ его последнего появления
    застройщик отсканился ПОЛНО ≥ GONE_PERSIST раз, а лота нет. Это убивает
    одиночные пропуски и хвостовые сбои скрапа.
  * мерцание (47% лотов появляются/исчезают/появляются) безопасно само:
    last_seen_date = MAX(дата), поэтому «вернувшийся» лот не считается ушедшим
    в середине — его last_seen автоматически позже.

Все таблицы материализуются в конце скана (refresh_materialized) — данных мало
по меркам батча, читаются Datasette/SPA как /pik/<table>.json.
"""
from __future__ import annotations

import bisect
import sqlite3
import statistics
from collections import defaultdict
from datetime import date, timedelta

# Дневной объём застройщика ниже этой доли от медианы → частичный скан, не в счёт.
FULL_SCAN_FRACTION = 0.5
# Сколько полных сканов застройщика после last_seen нужно, чтобы признать «ушёл».
GONE_PERSIST = 2
# Статусы «забронировано» — лидирующий индикатор продажи (free → reserve → gone).
RESERVED_STATUSES = ("reserve", "booked", "2")


def _to_date(s: str | None) -> date | None:
    """ISO-дата или None. В проде scan_date всегда ISO; синтетические тестовые
    значения ('d', 't') не должны ронять материализацию."""
    try:
        return date.fromisoformat(s) if s else None
    except (ValueError, TypeError):
        return None


def _full_scan_dates(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """developer → отсортированный список дат, где был ПОЛНЫЙ скан застройщика.

    Полный = дневное число лотов ≥ FULL_SCAN_FRACTION × медиана по застройщику.
    Так частичные/упавшие дни не дают ложных «исчезновений».
    """
    rows = conn.execute(
        """
        SELECT b.developer AS dev, s.scan_date AS d, COUNT(*) AS c
        FROM snapshots s
        JOIN flats f ON f.id = s.flat_id
        JOIN blocks b ON b.id = f.block_id
        GROUP BY b.developer, s.scan_date
        """
    ).fetchall()
    by_dev: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for dev, d, c in rows:
        by_dev[dev].append((d, c))
    full: dict[str, list[str]] = {}
    for dev, lst in by_dev.items():
        counts = [c for _, c in lst]
        med = statistics.median(counts) if counts else 0
        thr = FULL_SCAN_FRACTION * med
        full[dev] = sorted(d for d, c in lst if c >= thr)
    return full


def build_flat_lifecycle(conn: sqlite3.Connection) -> None:
    """Строит таблицу flat_lifecycle: по строке на квартиру.

    Поля: first_seen_date / last_seen_date / ever_reserved / gone / gone_date /
    still_listed / dom_days (дни в продаже, для ушедших).
    """
    full = _full_scan_dates(conn)
    reserved_set = ",".join(f"'{s}'" for s in RESERVED_STATUSES)
    rows = conn.execute(
        f"""
        SELECT f.id, f.block_id, b.developer, f.bulk_name, f.rooms,
               MIN(s.scan_date), MAX(s.scan_date),
               MAX(CASE WHEN s.status IN ({reserved_set}) THEN 1 ELSE 0 END),
               COUNT(*)
        FROM snapshots s
        JOIN flats f ON f.id = s.flat_id
        JOIN blocks b ON b.id = f.block_id
        GROUP BY f.id
        """
    ).fetchall()

    out = []
    for (fid, bid, dev, bulk, rooms, first, last, ever_res, n_snaps) in rows:
        dates = full.get(dev, [])
        # полные сканы застройщика СТРОГО после последнего появления лота
        after = dates[bisect.bisect_right(dates, last):]
        scans_after = len(after)
        gone = 1 if scans_after >= GONE_PERSIST else 0
        gone_date = after[0] if gone else None
        # still_listed — присутствовал в последнем полном скане застройщика
        still = 1 if scans_after == 0 else 0
        gd, fd = _to_date(gone_date), _to_date(first)
        dom = (gd - fd).days if (gone and gd and fd) else None
        out.append((fid, bid, dev, bulk, rooms, first, last,
                    ever_res, gone, gone_date, still, dom, n_snaps))

    conn.execute("DROP TABLE IF EXISTS flat_lifecycle")
    conn.execute(
        """
        CREATE TABLE flat_lifecycle (
            flat_id         INTEGER PRIMARY KEY,
            block_id        INTEGER,
            developer       TEXT,
            bulk_name       TEXT,
            rooms           TEXT,
            first_seen_date TEXT,
            last_seen_date  TEXT,
            ever_reserved   INTEGER,
            gone            INTEGER,
            gone_date       TEXT,
            still_listed    INTEGER,
            dom_days        INTEGER,
            n_snaps         INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO flat_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", out
    )
    conn.execute("CREATE INDEX idx_lc_block ON flat_lifecycle(block_id)")


def _coverage_30d(full: dict[str, list[str]], today: str) -> dict[str, int]:
    """developer → % дней с полным сканом за последние 30 кал. дней (достоверность)."""
    td = _to_date(today)
    if td is None:
        return {}
    d30 = (td - timedelta(days=30)).isoformat()
    cov = {}
    for dev, dates in full.items():
        n = sum(1 for d in dates if d > d30)
        cov[dev] = round(100 * n / 30)
    return cov


def build_block_velocity(conn: sqlite3.Connection, today: str | None = None) -> None:
    """Агрегат скорости продаж по ЖК поверх flat_lifecycle.

    active_now, new_7d/30d, absorbed_7d/30d, median_dom_days,
    absorption_pct_30d (доля распроданного), coverage_30d (достоверность).
    """
    if today is None:
        today = conn.execute("SELECT MAX(scan_date) FROM snapshots").fetchone()[0]
    td = _to_date(today)
    if today is None:
        # пустая БД — создаём пустую таблицу и выходим
        _create_empty_block_velocity(conn)
        return
    # td=None — синтетические не-ISO даты (тесты): окна вырождаем в "всё", чтобы
    # материализация не падала; в проде даты всегда ISO.
    d7 = (td - timedelta(days=7)).isoformat() if td else ""
    d30 = (td - timedelta(days=30)).isoformat() if td else ""
    cov = _coverage_30d(_full_scan_dates(conn), today)

    rows = conn.execute(
        """
        SELECT lc.block_id, b.name, b.developer, b.city, b.metro_name,
               lc.first_seen_date, lc.gone, lc.gone_date, lc.still_listed, lc.dom_days
        FROM flat_lifecycle lc
        LEFT JOIN blocks b ON b.id = lc.block_id
        """
    ).fetchall()

    agg: dict[int, dict] = {}
    for (bid, name, dev, city, metro, first, gone, gdate, still, dom) in rows:
        a = agg.get(bid)
        if a is None:
            a = agg[bid] = {
                "name": name, "developer": dev, "city": city, "metro": metro,
                "total": 0, "active": 0, "new7": 0, "new30": 0,
                "abs7": 0, "abs30": 0, "doms": [],
            }
        a["total"] += 1
        a["active"] += still or 0
        if first and first > d30:
            a["new30"] += 1
            if first > d7:
                a["new7"] += 1
        if gone and gdate:
            if gdate > d30:
                a["abs30"] += 1
                if gdate > d7:
                    a["abs7"] += 1
            if dom is not None:
                a["doms"].append(dom)

    _create_empty_block_velocity(conn)
    out = []
    for bid, a in agg.items():
        denom = a["active"] + a["abs30"]
        absorption = round(100 * a["abs30"] / denom, 1) if denom else 0.0
        median_dom = round(statistics.median(a["doms"])) if a["doms"] else None
        # active_now=0 при наличии лотов = ЖК ушёл с витрины целиком (снят /
        # завершён / распродан) — это НЕ «темп продаж», поэтому отдельный статус,
        # чтобы UI не выдавал «100% за месяц» за горячие продажи.
        status = "off_market" if (a["active"] == 0 and a["total"] > 0) else "active"
        out.append((
            bid, a["name"], a["developer"], a["city"], a["metro"],
            a["total"], a["active"], a["new7"], a["new30"],
            a["abs7"], a["abs30"], median_dom, absorption,
            cov.get(a["developer"], 0), status,
        ))
    conn.executemany(
        "INSERT INTO block_velocity VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", out
    )
    conn.execute("CREATE INDEX idx_bv_block ON block_velocity(block_id)")


def _create_empty_block_velocity(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS block_velocity")
    conn.execute(
        """
        CREATE TABLE block_velocity (
            block_id           INTEGER PRIMARY KEY,
            name               TEXT,
            developer          TEXT,
            city               TEXT,
            metro_name         TEXT,
            total_tracked      INTEGER,
            active_now         INTEGER,
            new_7d             INTEGER,
            new_30d            INTEGER,
            absorbed_7d        INTEGER,
            absorbed_30d       INTEGER,
            median_dom_days    INTEGER,
            absorption_pct_30d REAL,
            coverage_30d       INTEGER,
            block_status       TEXT
        )
        """
    )


def build_block_inventory_daily(conn: sqlite3.Connection) -> None:
    """Кривая остатка: по (block_id, scan_date) число выставленных лотов.

    Точечный счёт — устойчив к мерцанию (не выводит исчезновения), работает на
    любой глубине истории. Падающая кривая = ЖК распродаётся.
    """
    reserved_set = ",".join(f"'{s}'" for s in RESERVED_STATUSES)
    conn.execute("DROP TABLE IF EXISTS block_inventory_daily")
    conn.execute(
        f"""
        CREATE TABLE block_inventory_daily AS
        SELECT f.block_id AS block_id,
               s.scan_date AS scan_date,
               COUNT(DISTINCT s.flat_id) AS listed,
               SUM(CASE WHEN s.status IN ({reserved_set}) THEN 1 ELSE 0 END) AS reserved
        FROM snapshots s
        JOIN flats f ON f.id = s.flat_id
        GROUP BY f.block_id, s.scan_date
        """
    )
    conn.execute(
        "CREATE INDEX idx_inv_block ON block_inventory_daily(block_id, scan_date)"
    )


def build_velocity_tables(conn: sqlite3.Connection) -> None:
    """Полная пересборка velocity-витрин. Вызывается в конце refresh_materialized."""
    build_flat_lifecycle(conn)
    build_block_velocity(conn)
    build_block_inventory_daily(conn)
