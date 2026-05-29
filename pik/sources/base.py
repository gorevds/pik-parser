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
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import requests

from pik import PikParserError
from pik.developers import ID_NAMESPACE, namespaced_id, stable_int_id
from pik.geo import CITY_CENTERS, city_from_address, haversine_km

log = logging.getLogger("pik.sources")


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_RETRYABLE_STATUS = (429, 500, 502, 503, 504)


class SourceError(PikParserError):
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

    Дополнительные опциональные поля (по мере появления у источников):
    `ceiling_height`, `area_kitchen`, `area_living` — фактически только
    PIK API их даёт. У других — None.
    `mortgage_min_rate`/`mortgage_best_name` — best-rate программа из
    benefits.mortgage (PIK; у остальных нет данных в листинге).
    `pdf_url` — ссылка на PDF-планировку (PIK).
    `bulk_id`/`section_id`/`layout_id` — внутренние id корпуса/секции/
    планировки (PIK); другим источникам не нужны (они в bulk_name/section_no).
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
    is_apartment: bool = False
    # PIK-specific (другие источники их не заполняют):
    ceiling_height: float | None = None
    area_kitchen: float | None = None
    area_living: float | None = None
    mortgage_min_rate: float | None = None
    mortgage_best_name: str | None = None
    pdf_url: str | None = None
    bulk_id: int | None = None
    section_id: int | None = None
    layout_id: int | None = None
    # Скан-time поля (PIK даёт явный updated_at; используется для аналитики
    # «когда у этой квартиры реально менялась цена»):
    updated_at: str | None = None
    # Промо-семантика для PIK: API не отдаёт old_price (sticker), но даёт
    # meter_price (с программой) и price (нал). У ПИК-скана исторически
    # promo_price = round(meter_price * area). Чтобы build_rows не дрейфовал
    # от двух семантик, источник может сам передать готовый promo_price и
    # discount_pct; для non-PIK адаптеров эти поля None и работает обычная
    # old_price-based детекция.
    promo_price: int | None = None
    discount_pct: float | None = None


@dataclass
class CollectResult:
    """Результат обхода застройщика — готов к build_rows.

    `skipped` — сколько единиц (блоков/проектов) источник не смог достать,
    но обход продолжился. >0 означает частичную деградацию: данные неполные,
    хотя сбор формально не упал. run_developer переводит это в
    status='partial' в scan_runs (см. R2), чтобы алерт отличал
    «60→3 блока, антибот» от честного успеха.
    """
    blocks: list[NormBlock] = field(default_factory=list)
    flats: list[NormFlat] = field(default_factory=list)
    skipped: int = 0


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
    # Эвристика «апартаменты» на уровне БЛОКА: имя ЖК содержит «апарт».
    # Универсально работает для всех застройщиков (не зависит от
    # специфических полей API), плюс источники могут пометить is_apartment
    # на уровне квартиры из своих полей — берём OR.
    block_apart_hint = {
        to_global_id(developer, b.native_id): ("апарт" in (b.name or "").lower())
        for b in result.blocks
    }

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

        is_apart = bool(f.is_apartment) or block_apart_hint.get(block_gid, False)
        flat_rows.append({
            "id": gid,
            "guid": str(f.native_id),
            "block_id": block_gid,
            "bulk_id": f.bulk_id,
            "section_id": f.section_id,
            "layout_id": f.layout_id,
            "bulk_name": f.bulk_name,
            "section_no": f.section_no,
            "floor": f.floor,
            "rooms": ("studio" if rooms == 0 else str(rooms))
                     if rooms is not None else None,
            "rooms_fact": rooms,
            "is_studio": 1 if rooms == 0 else 0,
            "is_apartment": 1 if is_apart else 0,
            "area": area,
            "area_kitchen": f.area_kitchen,
            "area_living": f.area_living,
            "number": f.number,
            "name": f.number,
            "url": f.url,
            "pdf_url": f.pdf_url,
            "plan_url": f.plan_url,
            "ceiling_height": f.ceiling_height,
            "settlement_date": f.settlement_date,
            "first_seen": scan_date,
        })
        # Промо-логика — две семантики:
        # 1) Источник дал явный promo_price (PIK, считает adapter): берём
        #    его + discount_pct из adapter'а если есть, иначе вычисляем.
        # 2) Источник дал old_price > price (FSK, MR Group, Гранель, …):
        #    promo_price = price (нет отдельной «программы»), discount =
        #    old_price → price.
        # 3) Ни того, ни другого: promo_price = price, has_promo = 0.
        if f.promo_price is not None:
            promo_price = f.promo_price
            if f.discount_pct is not None:
                disc_pct = f.discount_pct
                has_promo = 1 if disc_pct >= 0.5 else 0
            elif price and promo_price < price:
                disc_pct = round((price - promo_price) / price * 100, 2)
                has_promo = 1 if disc_pct >= 0.5 else 0
            # R9: абсолютную скидку считаем от ТОЙ ЖЕ (программной) базы, что
            # и disc_pct: price → promo_price. Иначе discount хранил бы
            # old_price−price (стикерную), рассинхронясь с discount_pct.
            if price is not None and has_promo and promo_price < price:
                disc_abs = price - promo_price
            else:
                disc_abs = 0
        else:
            promo_price = price  # default = «без программы»
        snap_rows.append({
            "flat_id": gid,
            "scan_date": scan_date,
            "scan_ts": scan_ts,
            "status": f.status,
            "price": price,
            "meter_price": meter_price,
            "base_meter_price": base_meter,
            "promo_price": promo_price,
            "discount_pct": disc_pct,
            "has_promo": has_promo,
            "old_price": f.old_price,
            "discount": disc_abs,
            "finish": f.finish,
            "mortgage_min_rate": f.mortgage_min_rate,
            "mortgage_best_name": f.mortgage_best_name,
            "updated_at": f.updated_at,
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


def safe_next_url(url: str | None, allowed_host: str) -> str | None:
    """Возвращает URL, если хост == allowed_host или его поддомен; иначе None.

    Защита от SSRF: API застройщиков отдают `next`-страницу как полный URL.
    Без валидации хоста ответ {"next": "http://attacker.com/x", ...} увёл бы
    наш сканер на чужой домен; payload оттуда мы бы интерпретировали как
    «следующая страница каталога застройщика» и сложили в БД.

    Принудительно нормализуем схему в https — некоторые API отдают next
    как http://, что роняет nginx gzip (proxy_http_version 1.1) и
    отдаёт лишние редиректы.
    """
    if not url:
        return None
    try:
        parsed = urlparse(url)
    except (ValueError, AttributeError):
        return None
    # Trailing dot — валидная FQDN-форма (a101.ru. == a101.ru). Снимаем
    # перед сравнением, иначе любой бэк, отдающий `next: 'https://a101.ru./?p=2'`
    # уронит сканер в пол-обхода.
    host = (parsed.hostname or "").rstrip(".")
    if not (host == allowed_host or host.endswith("." + allowed_host)):
        return None
    if parsed.scheme not in ("http", "https"):
        return None
    return parsed._replace(scheme="https").geturl()


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
    # R15/SSRF: по умолчанию НЕ идём по redirect. safe_next_url валидирует
    # хост API-данного URL, но 302 с валидного хоста на внутренний адрес
    # (169.254.169.254, localhost) обошёл бы allowlist. Все наши catalog-URL
    # — стабильные https, редиректов в норме нет; если эндпоинт всё же
    # редиректит (напр. добавляет trailing slash), это всплывёт громкой
    # SourceError ниже, а вызывающий при необходимости передаст
    # allow_redirects=True явно.
    kwargs.setdefault("allow_redirects", False)
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
    kwargs.setdefault("allow_redirects", False)  # R15/SSRF, см. request_json
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
