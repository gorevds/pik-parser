"""Источник данных MR Group (mr-group.ru).

У MR Group нет JSON API: данные отрисованы в SSR-HTML каталога `/flats/`.
Сайт за анти-ботом ServicePipe — обходится User-Agent'ом Googlebot.
CSS-классы захешированы пер-сборка, поэтому парсер опирается НЕ на них,
а на устойчивые признаки: ссылку `/catalog/apartments/...` как границу
карточки и текстовые маркеры (₽, м², «этаж», «-комнатная»).

Ограничение: постраничная навигация (`page-N/`) у сайта отдаёт пустое
тело, поэтому собирается только первая страница каждого ЖК (до 48 квартир).
"""
from __future__ import annotations

import logging
import re
import time
from html import unescape

import requests

from pik.sources.base import CollectResult, NormBlock, NormFlat, SourceError


DEVELOPER = "MR Group"
_GOOGLEBOT_UA = (
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
)
_SITE = "https://www.mr-group.ru"

# ЖК MR Group в активной продаже (slug на сайте → отображаемое имя).
MR_BLOCKS: dict[str, str] = {
    "citybay": "Сити Бэй",
    "mira": "МИRА",
    "jois": "JOIS",
    "mod": "MOD",
    "seliger-siti": "Селигер Сити",
    "set": "SET",
    "slava": "Слава",
    "veer": "Veer",
}

log = logging.getLogger("pik.sources.mrgroup")

_FLAT_ANCHOR = re.compile(r'<a\b[^>]*href="(/catalog/apartments/[a-z0-9-]+)/?"')
_TAG = re.compile(r"<[^>]+>")
_ROOMS = re.compile(r"(\d+)-комнатн")
_AREA = re.compile(r"(\d[\d ]*,\d+)\s*м²")          # «127,04 м²» — с запятой
_PPM = re.compile(r"(\d[\d ]*)\s*₽/м²")              # «424 632 ₽/м²»
_PRICE = re.compile(r"(\d[\d ]*(?:,\d+)?)\s*₽(?!/)")  # «53 945 211,17 ₽», не ₽/м²
_FLOOR = re.compile(r"(\d+)/\d+\s*этаж")
_SETTLEMENT = re.compile(r"([IVX]+\s*кв\.?\s*\d{4})")
# Корпус — словом (может содержать цифру: «Норс 7») между суммой и «X/Y этаж»
_BUILDING = re.compile(
    r"(?:₽|²)\s+([А-ЯЁA-Z][\w .-]*?)\s+\d+/\d+\s*этаж"
)


def _num(raw: str) -> float | None:
    """«53 945 211,17» → 53945211.17."""
    cleaned = raw.replace(" ", "").replace("\xa0", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _card_text(card_html: str) -> str:
    # HTML хранит разделители разрядов как сущность &nbsp; — раскодируем
    # её (и прочие сущности), затем сводим всё к обычным пробелам.
    text = unescape(_TAG.sub(" ", card_html)).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _parse_card(href: str, text: str, block_slug: str) -> NormFlat | None:
    prices = [p for p in (_num(m) for m in _PRICE.findall(text)) if p]
    if not prices:
        return None  # без цены карточка бесполезна
    price = round(prices[0])
    old_price = round(prices[1]) if len(prices) > 1 and prices[1] > prices[0] else None

    rooms_m = _ROOMS.search(text)
    rooms = int(rooms_m.group(1)) if rooms_m else (0 if "Студи" in text else None)
    area_m = _AREA.search(text)
    ppm_m = _PPM.search(text)
    floor_m = _FLOOR.search(text)
    settle_m = _SETTLEMENT.search(text)
    bld_m = _BUILDING.search(text)

    href = href.rstrip("/")
    code = href.rsplit("/", 1)[-1]
    return NormFlat(
        native_id=code,
        native_block_id=block_slug,
        rooms=rooms,
        area=_num(area_m.group(1)) if area_m else None,
        floor=int(floor_m.group(1)) if floor_m else None,
        price=price,
        meter_price=round(_num(ppm_m.group(1))) if ppm_m else None,
        old_price=old_price,
        status="free",
        bulk_name=bld_m.group(1).strip() if bld_m else None,
        settlement_date=settle_m.group(1).strip() if settle_m else None,
        url=_SITE + href + "/",
        number=code.rsplit("-", 1)[-1],
    )


def parse_flats_page(html: str, block_slug: str) -> list[NormFlat]:
    """Достаёт все карточки квартир из HTML каталога одного ЖК."""
    anchors = list(_FLAT_ANCHOR.finditer(html))
    flats: list[NormFlat] = []
    for i, m in enumerate(anchors):
        end = anchors[i + 1].start() if i + 1 < len(anchors) else len(html)
        card = _parse_card(m.group(1), _card_text(html[m.start():end]), block_slug)
        if card is not None:
            flats.append(card)
    return flats


def _fetch_page(session: requests.Session, slug: str) -> str:
    url = f"{_SITE}/flats/zhk-{slug}/"
    resp = session.get(url, timeout=40)
    if resp.status_code != 200:
        raise SourceError(f"HTTP {resp.status_code} for {url}")
    return resp.text


def collect(
    *, session: requests.Session | None = None, sleep_sec: float = 4.0
) -> CollectResult:
    """Обходит каталоги всех ЖК MR Group (первая страница каждого)."""
    s = session or requests.Session()
    s.headers.update({"User-Agent": _GOOGLEBOT_UA,
                      "Accept-Language": "ru-RU,ru;q=0.9"})

    blocks: list[NormBlock] = []
    flats: list[NormFlat] = []
    for slug, name in MR_BLOCKS.items():
        try:
            html = _fetch_page(s, slug)
        except (SourceError, requests.RequestException) as exc:
            log.warning("MR Group: %s — пропущен: %s", slug, exc)
            continue
        page_flats = parse_flats_page(html, slug)
        if not page_flats:
            log.warning("MR Group: %s — 0 квартир (анти-бот или нет в продаже)", slug)
            continue
        blocks.append(NormBlock(native_id=slug, name=name, slug=slug))
        flats.extend(page_flats)
        log.info("MR Group: %s — %d квартир", slug, len(page_flats))
        time.sleep(sleep_sec)

    log.info("MR Group: %d ЖК, %d квартир", len(blocks), len(flats))
    return CollectResult(blocks=blocks, flats=flats)
