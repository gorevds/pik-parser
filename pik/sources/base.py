"""Нормализованная модель данных застройщика и сборка строк БД.

ПИК отдаёт данные через свой JSON API; остальные застройщики — каждый
по-своему (REST, GraphQL, HTML). Чтобы не дублировать логику записи,
каждый источник приводит свои данные к `NormBlock`/`NormFlat`, а
`build_rows` единообразно превращает их в строки blocks/flats/snapshots
с глобально-уникальными id (см. pik.developers).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

import requests

from pik.developers import ID_NAMESPACE, namespaced_id, stable_int_id


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_RETRYABLE_STATUS = (429, 500, 502, 503, 504)


class SourceError(RuntimeError):
    """Сбой получения данных застройщика."""


@dataclass(frozen=True)
class NormBlock:
    """ЖК застройщика в нормализованном виде."""
    native_id: int | str
    name: str
    slug: str | None = None
    meta: dict = field(default_factory=dict)  # ключи как в blocks_meta._BLOCK_META_COLS


@dataclass(frozen=True)
class NormFlat:
    """Квартира застройщика в нормализованном виде.

    `rooms`: 0 — студия, иначе число комнат.
    `meter_price`: цена за м²; если None — считается как price/area.
    """
    native_id: int | str
    native_block_id: int | str
    rooms: int | None = None
    area: float | None = None
    floor: int | None = None
    price: int | None = None
    meter_price: int | None = None
    old_price: int | None = None
    status: str | None = None
    bulk_name: str | None = None
    section_no: int | None = None
    settlement_date: str | None = None
    url: str | None = None
    finish: str | None = None
    number: str | None = None


@dataclass
class CollectResult:
    """Результат обхода застройщика — готов к build_rows."""
    blocks: list[NormBlock] = field(default_factory=list)
    flats: list[NormFlat] = field(default_factory=list)


def to_global_id(developer: str, native: int | str) -> int:
    """native id застройщика → глобально уникальный id для таблиц БД.

    Числовой id (или строка из цифр) в допустимом диапазоне используется
    напрямую; всё прочее (UUID, составные коды) хешируется детерминированно.
    """
    n: int
    if isinstance(native, int):
        n = native
    else:
        s = str(native).strip()
        if s.lstrip("-").isdigit():
            n = int(s)
        else:
            n = stable_int_id(s)
    if not 0 <= n < ID_NAMESPACE:
        n = stable_int_id(str(native))
    return namespaced_id(developer, n)


def _detect_discount(
    price: int | None, old_price: int | None
) -> tuple[int, float | None, int]:
    """(discount_abs, discount_pct, has_promo) из текущей и старой цены."""
    if not price or not old_price or old_price <= price:
        return 0, 0.0, 0
    abs_disc = old_price - price
    pct = round(abs_disc / old_price * 100, 2)
    return abs_disc, pct, (1 if pct >= 0.5 else 0)


def build_rows(
    developer: str,
    result: CollectResult,
    *,
    scan_date: str,
    scan_ts: str,
) -> tuple[list[dict], list[dict], list[dict]]:
    """NormBlock/NormFlat → (block_payloads, flat_rows, snap_rows).

    block_payloads — аргументы для blocks_meta.upsert_block_meta;
    flat_rows / snap_rows — строки для store.upsert.
    """
    block_payloads = [
        {
            "block_id": to_global_id(developer, b.native_id),
            "name": b.name,
            "slug": b.slug,
            "meta": b.meta,
            "developer": developer,
        }
        for b in result.blocks
    ]

    flat_rows: list[dict] = []
    snap_rows: list[dict] = []
    for f in result.flats:
        gid = to_global_id(developer, f.native_id)
        block_gid = to_global_id(developer, f.native_block_id)
        area = f.area
        price = f.price
        meter_price = f.meter_price
        if meter_price is None and price and area and area > 0:
            meter_price = round(price / area)
        base_meter = round(price / area) if price and area and area > 0 else None
        disc_abs, disc_pct, has_promo = _detect_discount(price, f.old_price)
        rooms = f.rooms

        flat_rows.append({
            "id": gid,
            "guid": str(f.native_id),
            "block_id": block_gid,
            "bulk_id": None,
            "section_id": None,
            "layout_id": None,
            "bulk_name": f.bulk_name,
            "section_no": f.section_no,
            "floor": f.floor,
            "rooms": ("studio" if rooms == 0 else str(rooms))
                     if rooms is not None else None,
            "rooms_fact": rooms,
            "is_studio": 1 if rooms == 0 else 0,
            "area": area,
            "area_kitchen": None,
            "area_living": None,
            "number": f.number,
            "name": f.number,
            "url": f.url,
            "pdf_url": None,
            "plan_url": None,
            "ceiling_height": None,
            "settlement_date": f.settlement_date,
            "first_seen": scan_date,
        })
        snap_rows.append({
            "flat_id": gid,
            "scan_date": scan_date,
            "scan_ts": scan_ts,
            "status": f.status,
            "price": price,
            "meter_price": meter_price,
            "base_meter_price": base_meter,
            "promo_price": price,
            "discount_pct": disc_pct,
            "has_promo": has_promo,
            "old_price": f.old_price,
            "discount": disc_abs,
            "finish": f.finish,
            "mortgage_min_rate": None,
            "mortgage_best_name": None,
            "updated_at": None,
        })
    return block_payloads, flat_rows, snap_rows


def make_session(user_agent: str = DEFAULT_UA) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    })
    return s


def _backoff(attempt: int) -> float:
    return [1, 5, 15, 45][min(attempt, 3)]


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    retries: int = 3,
    timeout: float = 40.0,
    backoff: Callable[[int], float] = _backoff,
    **kwargs: Any,
) -> Any:
    """HTTP-запрос с ретраями на сетевых сбоях и 5xx/429. Возвращает JSON."""
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException as exc:
            last_exc = exc
        else:
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError as exc:
                    last_exc = SourceError(f"non-JSON body: {exc}")
            elif resp.status_code in _RETRYABLE_STATUS:
                last_exc = SourceError(f"HTTP {resp.status_code}")
            else:
                raise SourceError(
                    f"HTTP {resp.status_code} for {url}: {resp.text[:200]}"
                )
        if attempt < retries:
            time.sleep(backoff(attempt))
    raise SourceError(f"{method} {url} failed after {retries + 1} attempts: {last_exc}")
