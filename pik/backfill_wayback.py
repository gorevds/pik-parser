"""Подтягивает исторические снимки страниц pik.ru/narvin* из Wayback Machine.

Каждая снапшот-страница содержит `__NEXT_DATA__` с `searchService.filteredFlats.data.flats` —
первые 20 квартир (page=1). Разные снапшоты + разные слаги дают разнообразный
исторический срез цен, который не у меня нет в `api.pik.ru/v2/flat`.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Iterable

import requests

from .client import DEFAULT_UA
from .mapping import to_flat_row, to_snapshot_row
from .store import apply_schema, upsert


log = logging.getLogger("pik.backfill")

CDX_API = "https://web.archive.org/cdx/search/cdx"
NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL
)

# Сюда грузим. Чем больше — тем больше уникальных квартир (каждая URL даёт свою «страницу 1»).
NARVIN_URLS = (
    "https://www.pik.ru/narvin",
    "https://www.pik.ru/search/narvin",
    "https://www.pik.ru/search/narvin/one-room",
    "https://www.pik.ru/search/narvin/two-room",
    "https://www.pik.ru/search/narvin/three-room",
    "https://www.pik.ru/search/narvin/studio",
    "https://www.pik.ru/search/narvin/chessplan",
    "https://www.pik.ru/search/narvin/one-room/finish",
)


def _make_session(user_agent: str = DEFAULT_UA) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
    })
    return s


def list_snapshots(
    url: str,
    *,
    from_yyyymmdd: str = "20250601",
    to_yyyymmdd: str = "20260601",
    session: requests.Session | None = None,
) -> list[dict]:
    """Список Wayback-снимков (timestamp, original_url) для одного URL."""
    s = session or _make_session()
    params = {
        "url": url,
        "matchType": "exact",
        "output": "json",
        "filter": "statuscode:200",
        "from": from_yyyymmdd,
        "to": to_yyyymmdd,
        "collapse": "timestamp:8",
    }
    r = s.get(CDX_API, params=params, timeout=30)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return []
    header, *rest = rows
    return [{"timestamp": row[1], "url": row[2]} for row in rest]


def fetch_replay(
    timestamp: str,
    original_url: str,
    *,
    session: requests.Session | None = None,
) -> str:
    """Скачивает raw HTML из Wayback (id_ — без переписывания тела)."""
    s = session or _make_session()
    wb_url = f"https://web.archive.org/web/{timestamp}id_/{original_url}"
    r = s.get(wb_url, timeout=60)
    r.raise_for_status()
    return r.text


def extract_flats_from_html(html: str) -> list[dict]:
    """Достаёт массив 'flats' из __NEXT_DATA__."""
    m = NEXT_DATA_RE.search(html)
    if not m:
        return []
    try:
        data = json.loads(m.group(1))
    except ValueError:
        return []
    try:
        return data["props"]["pageProps"]["initialState"]["searchService"][
            "filteredFlats"
        ]["data"]["flats"]
    except (KeyError, TypeError):
        return []


def _to_api_v2_shape(wb_flat: dict) -> dict:
    """Конвертирует formato `filteredFlats.flats` в форму, ожидаемую mapping.to_*."""
    href = wb_flat.get("href")
    return {
        "id": wb_flat["id"],
        "guid": wb_flat["guid"],
        "block_id": 1165,  # снапшоты только narvin-страниц
        "bulk_id": None,
        "section_id": None,
        "layout_id": None,
        "floor": wb_flat.get("floor"),
        "rooms": wb_flat.get("rooms"),
        "rooms_fact": (
            1 if wb_flat.get("rooms") == 1 else None
        ),
        "is_studio": 1 if wb_flat.get("rooms") in ("studio", -1) else 0,
        "area": wb_flat.get("area"),
        "areaKitchen": None,
        "areaLiving": None,
        "number": None,
        "name": None,
        "url": ("https://www.pik.ru" + href) if href else None,
        "pdf": None,
        "ceilingHeight": None,
        "price": wb_flat.get("price"),
        "meterPrice": wb_flat.get("meterPrice"),
        "oldPrice": wb_flat.get("oldPrice"),
        "discount": None,
        "status": wb_flat.get("status"),
        "finish": None,  # отсутствует в листинге
        "benefits": None,
        "updatedAt": None,
        "settlementDate": wb_flat.get("settlementDate"),
        "bulk": {"name": wb_flat.get("bulkName")},
        "section": {"number": wb_flat.get("sectionNumber")},
        "layout": {},
    }


def _wayback_date(timestamp: str) -> str:
    """20250629055318 -> 2025-06-29."""
    return f"{timestamp[0:4]}-{timestamp[4:6]}-{timestamp[6:8]}"


def _wayback_iso(timestamp: str) -> str:
    """20250629055318 -> 2025-06-29T05:53:18+00:00 (Wayback хранит в UTC)."""
    return (
        f"{timestamp[0:4]}-{timestamp[4:6]}-{timestamp[6:8]}T"
        f"{timestamp[8:10]}:{timestamp[10:12]}:{timestamp[12:14]}+00:00"
    )


def backfill(
    db_path: Path,
    *,
    urls: Iterable[str] = NARVIN_URLS,
    from_yyyymmdd: str = "20250601",
    to_yyyymmdd: str = "20260601",
    sleep_sec: float = 1.5,
    session: requests.Session | None = None,
) -> dict[str, int]:
    """Полный проход: собрать CDX, скачать каждый снимок, заинсертить.

    Возвращает счётчики: snapshots / flats_seen / unique_keys.
    """
    s = session or _make_session()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    all_snaps: list[dict] = []
    for url in urls:
        try:
            snaps = list_snapshots(
                url, from_yyyymmdd=from_yyyymmdd, to_yyyymmdd=to_yyyymmdd, session=s
            )
        except requests.HTTPError as exc:
            log.warning("CDX for %s failed: %s", url, exc)
            continue
        log.info("CDX %s -> %d snapshots", url, len(snaps))
        all_snaps.extend(snaps)

    # Дедуп по (date, original_url) — в один день несколько снимков одной страницы
    # дают одинаковые данные.
    seen = set()
    deduped = []
    for snap in sorted(all_snaps, key=lambda x: x["timestamp"]):
        key = (_wayback_date(snap["timestamp"]), snap["url"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(snap)

    log.info("processing %d unique snapshots", len(deduped))

    flats_by_pk: dict[tuple, dict] = {}
    snaps_by_pk: dict[tuple, dict] = {}
    unique_flat_ids: set[int] = set()
    date_counts: dict[str, int] = defaultdict(int)
    snapshot_errors = 0

    for snap in deduped:
        try:
            html = fetch_replay(snap["timestamp"], snap["url"], session=s)
        except requests.RequestException as exc:
            log.warning("replay %s@%s failed: %s", snap["url"], snap["timestamp"], exc)
            snapshot_errors += 1
            time.sleep(sleep_sec)
            continue

        wb_flats = extract_flats_from_html(html)
        scan_date = _wayback_date(snap["timestamp"])
        scan_ts = _wayback_iso(snap["timestamp"])
        date_counts[scan_date] += len(wb_flats)

        for wb_flat in wb_flats:
            if wb_flat.get("blockSlug") and wb_flat["blockSlug"] != "narvin":
                continue  # на /narvin страницах могут попадаться рекомендации других ЖК
            if not wb_flat.get("id"):
                continue
            api_shape = _to_api_v2_shape(wb_flat)
            flat_row = to_flat_row(api_shape, first_seen=scan_date)
            snap_row = to_snapshot_row(
                api_shape, scan_date=scan_date, scan_ts=scan_ts
            )
            # последняя запись по (id) выигрывает в flats; последняя по (id,date) — в snapshots
            flats_by_pk[(flat_row["id"],)] = flat_row
            snaps_by_pk[(snap_row["flat_id"], snap_row["scan_date"])] = snap_row
            unique_flat_ids.add(flat_row["id"])

        time.sleep(sleep_sec)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        apply_schema(conn)
        upsert(
            conn,
            flats=list(flats_by_pk.values()),
            snapshots=list(snaps_by_pk.values()),
        )

    log.info(
        "stored: snapshots=%d unique_flats=%d dates=%d errors=%d",
        len(snaps_by_pk),
        len(unique_flat_ids),
        len(date_counts),
        snapshot_errors,
    )
    for d in sorted(date_counts):
        log.info("  %s: %d raw flats", d, date_counts[d])

    return {
        "snapshots": len(snaps_by_pk),
        "unique_flats": len(unique_flat_ids),
        "dates": len(date_counts),
        "errors": snapshot_errors,
    }
