"""Нормализованная модель данных застройщика и сборка строк БД.

ПИК отдаёт данные через свой JSON API; остальные застройщики — каждый
по-своему (REST, GraphQL, HTML). Чтобы не дублировать логику записи,
каждый источник приводит свои данные к `NormBlock`/`NormFlat`, а
`build_rows` единообразно превращает их в строки blocks/flats/snapshots
с глобально-уникальными id (см. pik.developers).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import requests

from pik.developers import ID_NAMESPACE, namespaced_id, stable_int_id
from pik.geo import CITY_CENTERS, city_from_address, haversine_km


log = logging.getLogger("pik.sources")


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
    plan_url: str | None = None


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
    if native is None:
        # иначе все None схлопнулись бы в один и тот же hash("None")
        raise ValueError("native id is None")
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
        # числовой id вне диапазона [0, ID_NAMESPACE) — обычно симптом ошибки
        # парсинга выше по стеку (отрицательный sentinel, лишний разряд).
        # Квартиру не теряем — хешируем, но логируем: split_id() для такого
        # id уже необратим.
        log.warning(
            "%s: числовой native id %r вне [0, %d) — заменён хешем",
            developer, native, ID_NAMESPACE,
        )
        n = stable_int_id(str(native))
    return namespaced_id(developer, n)


def _detect_discount(
    price: int | None, old_price: int | None
) -> tuple[int, float, int]:
    """(discount_abs, discount_pct, has_promo) из текущей и старой цены.

    Скидка ниже 0.5% (в т.ч. округляющаяся до 0.0%) считается отсутствующей —
    все три значения тогда нулевые, без рассинхрона между ними.
    """
    if not price or not old_price or old_price <= price:
        return 0, 0.0, 0
    abs_disc = old_price - price
    pct = round(abs_disc / old_price * 100, 2)
    if pct < 0.5:
        return 0, 0.0, 0
    return abs_disc, pct, 1


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
    block_payloads = []
    for b in result.blocks:
        # копируем — NormBlock frozen, и мы вписываем 'city'. Город из адреса,
        # если есть; без адреса (Donstroy/A101/Level/Absolut/MR Group все
        # московские по выбору источника) — 'msk'. FSK даёт post_address →
        # city_from_address вернёт 'msk' / 'mo' / 'other' (последнее — если в
        # адресе нет ни региона, ни «Москва»: бывает у FSK с голым «ул. ..., д.
        # ...»). Не-PIK источники сами фильтруют по Москве, поэтому 'other' у
        # них принудительно сворачивается в 'msk'.
        meta = dict(b.meta)
        city = meta.get("city") or city_from_address(meta.get("address"))
        if city == "other":
            city = "msk"
        meta["city"] = city
        # distance_km: если источник дал lat/lng, считаем сами — иначе колонка
        # «км от центра» в today_all остаётся NULL у не-PIK.
        if (meta.get("distance_km") is None
                and meta.get("latitude") is not None
                and meta.get("longitude") is not None
                and city in CITY_CENTERS):
            c_lat, c_lon = CITY_CENTERS[city]
            meta["distance_km"] = round(
                haversine_km(float(meta["latitude"]), float(meta["longitude"]),
                             c_lat, c_lon), 1
            )
        block_payloads.append({
            "block_id": to_global_id(developer, b.native_id),
            "name": b.name,
            "slug": b.slug,
            "meta": meta,
            "developer": developer,
        })

    known_block_ids = {bp["block_id"] for bp in block_payloads}

    flat_rows: list[dict] = []
    snap_rows: list[dict] = []
    seen_ids: set[int] = set()
    dup_natives: list[int | str] = []
    skipped_noid = skipped_orphan = skipped_dup = 0
    for f in result.flats:
        if f.native_id is None or f.native_block_id is None:
            skipped_noid += 1
            continue
        gid = to_global_id(developer, f.native_id)
        block_gid = to_global_id(developer, f.native_block_id)
        # квартира без зарегистрированного ЖК осиротеет: в today_all её
        # COALESCE(developer,'ПИК') ошибочно приписал бы к ПИК
        if block_gid not in known_block_ids:
            skipped_orphan += 1
            continue
        # коллизия id (теоретически — хеш строковых id) затёрла бы соседа
        if gid in seen_ids:
            skipped_dup += 1
            dup_natives.append(f.native_id)
            continue
        seen_ids.add(gid)
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
            "plan_url": f.plan_url,
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

    if skipped_noid or skipped_orphan or skipped_dup:
        log.warning(
            "%s: пропущено квартир — без id: %d, без ЖК: %d, дубль id: %d",
            developer, skipped_noid, skipped_orphan, skipped_dup,
        )
        if dup_natives:
            # дубль — либо источник вернул один id дважды (безвредно), либо
            # хеш двух РАЗНЫХ строковых ключей совпал (тогда это потеря
            # квартиры). native id в логе позволяет различить эти случаи.
            log.warning("%s: native id с конфликтом global id: %s",
                        developer, dup_natives)
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


def request_text(
    session: requests.Session,
    method: str,
    url: str,
    *,
    retries: int = 3,
    timeout: float = 40.0,
    retry_status: tuple[int, ...] = _RETRYABLE_STATUS + (403,),
    backoff: Callable[[int], float] = _backoff,
    **kwargs: Any,
) -> str:
    """HTTP-запрос с ретраями, возвращает тело ответа как текст.

    Для HTML-источников. В отличие от request_json к ретраябельным статусам
    добавлен 403: анти-боты (ServicePipe и пр.) отдают его спорадически, и
    повтор с паузой часто проходит.
    """
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException as exc:
            last_exc = exc
        else:
            if resp.status_code == 200:
                return resp.text
            last_exc = SourceError(f"HTTP {resp.status_code} for {url}")
            if resp.status_code not in retry_status:
                raise last_exc
        if attempt < retries:
            time.sleep(backoff(attempt))
    raise SourceError(
        f"{method} {url} failed after {retries + 1} attempts: {last_exc}"
    )
