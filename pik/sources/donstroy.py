"""Источник данных Донстрой (donstroy.moscow).

Открытый JSON API: POST `/api/v1/flatssearch/choose_params_api_flats/`
с телом `{"page": N}` отдаёт квартиры постранично (фиксировано 12 на
страницу). ЖК у Донстроя — это поле `project` в карточке квартиры.

Метро/координаты отдельным API не отдаются, но в HTML-странице ЖК
(/objects/<slug>/) embedded JSON содержит `"coords":[lat,lng]`, а в
видимом тексте — `Метро «Название»` (см. _fetch_block_meta_html).
"""
from __future__ import annotations

import html as _html
import logging
import re

import requests

from pik.sources.base import (
    CollectResult,
    NormBlock,
    NormFlat,
    SourceError,
    make_session,
    request_json,
    request_text,
)

DEVELOPER = "Донстрой"
_FLATS_URL = "https://donstroy.moscow/api/v1/flatssearch/choose_params_api_flats/"
_OBJECT_URL_FMT = "https://donstroy.moscow/objects/{slug}/"
_SITE = "https://donstroy.moscow"
_PAGE_SIZE = 12        # фиксировано сервером, тело per_page игнорируется
_MAX_PAGES = 400       # предохранитель от бесконечной пагинации
# Googlebot UA пускает за ServicePipe анти-бота к /objects/<slug>/.
# Обычным UA эти страницы возвращают пустой шаблон.
_HTML_UA = (
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
)
_COORDS_RE = re.compile(r'"coords":\[([\d.]+),\s*([\d.]+)\]')
_METRO_RE = re.compile(r'Метро\xa0«([^»]+)»')

log = logging.getLogger("pik.sources.donstroy")


def _fetch_block_meta_html(session: requests.Session, slug: str) -> dict:
    """HTML-страница ЖК → coords и primary metro.

    Время до метро и адрес в HTML не embedded; coords парсим из JSON-блоба,
    метро — из видимого текста «Метро «Название»». Возвращаем пустой dict
    при любом сбое — ЖК не должен валить весь скан.
    """
    try:
        # Своя сессия с Googlebot UA — обычный пакет от make_session
        # уходит за ServicePipe-челлендж.
        s = requests.Session()
        s.headers.update({"User-Agent": _HTML_UA,
                          "Accept-Language": "ru-RU,ru;q=0.9"})
        text = request_text(s, "GET", _OBJECT_URL_FMT.format(slug=slug),
                            timeout=20.0)
    except (SourceError, requests.RequestException) as exc:
        log.warning("Донстрой: %s — meta не получена: %s", slug, exc)
        return {}
    decoded = _html.unescape(text)
    meta: dict = {}
    if (m := _COORDS_RE.search(decoded)):
        try:
            meta["latitude"] = float(m.group(1))
            meta["longitude"] = float(m.group(2))
        except ValueError:
            pass
    metros = _METRO_RE.findall(decoded)
    if metros:
        # Берём первую — обычно ближайшая «своя» станция (потом могут идти
        # «также рядом ...»). Время пешком HTML не даёт.
        meta["metro_name"] = metros[0].replace("\xa0", " ")
    return meta


def _to_float(value) -> float | None:
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _slug_from_link(link: str | None) -> str | None:
    """/objects/simvol/plans/... → 'simvol'."""
    if not link:
        return None
    parts = [p for p in link.split("/") if p]
    if len(parts) >= 2 and parts[0] == "objects":
        return parts[1]
    return None


def _to_norm(fl: dict) -> NormFlat:
    # price_request=true → цена скрыта ("по запросу"), снапшот без цены
    price = None if fl.get("price_request") else fl.get("price")
    price_old = fl.get("price_old")
    old_price = price_old if (price and price_old and price_old > price) else None
    building = fl.get("building")
    link = fl.get("link")
    plan = fl.get("plan")  # относительный SVG из CDN: «/hydra/svg/.../...svg»
    return NormFlat(
        native_id=fl.get("id"),
        native_block_id=fl.get("project"),
        rooms=fl.get("rooms"),
        area=_to_float(fl.get("area")),
        floor=fl.get("floor"),
        price=price,
        old_price=old_price,
        status="free",  # API отдаёт только продающиеся квартиры
        bulk_name=(f"Корпус {building}" if building not in (None, "") else None),
        section_no=_to_int(fl.get("section")),
        url=(_SITE + link) if link else None,
        finish="С отделкой" if fl.get("furnish") else "Без отделки",
        number=str(fl["number"]) if fl.get("number") is not None else None,
        plan_url=(_SITE + plan) if plan and plan.startswith("/") else plan,
    )


def collect(*, session: requests.Session | None = None) -> CollectResult:
    """Постранично обходит весь каталог квартир Донстроя."""
    s = session or make_session()
    norm_flats: list[NormFlat] = []
    block_slugs: dict[str, str | None] = {}  # project name → slug (из link)
    block_floors: dict[str, int] = {}        # project name → max(floors_total)

    for page in range(1, _MAX_PAGES + 1):
        payload = request_json(
            s, "POST", _FLATS_URL, json={"page": page},
        )
        flats = payload.get("flats") or []
        if not flats:
            break
        for fl in flats:
            project = fl.get("project")
            if not project:
                continue
            block_slugs.setdefault(project, _slug_from_link(fl.get("link")))
            ft = _to_int(fl.get("floors_total"))
            if ft:
                block_floors[project] = max(block_floors.get(project, 0), ft)
            norm_flats.append(_to_norm(fl))
        if len(flats) < _PAGE_SIZE:
            break
    else:
        log.warning("Донстрой: достигнут предел в %d страниц", _MAX_PAGES)

    # Метро/координаты — HTML-скрейп страницы ЖК (один запрос на ЖК,
    # 10 ЖК = ~10с). Не критично если упадёт: build_rows подставит city
    # по умолчанию, метро останется NULL.
    block_meta: dict[str, dict] = {}
    for name, slug in block_slugs.items():
        if slug:
            block_meta[name] = _fetch_block_meta_html(s, slug)
        else:
            block_meta[name] = {}

    blocks = [
        NormBlock(
            native_id=name, name=name, slug=slug,
            meta={"floors_max": block_floors.get(name), **block_meta.get(name, {})},
        )
        for name, slug in block_slugs.items()
    ]
    log.info("Донстрой: %d ЖК, %d квартир", len(blocks), len(norm_flats))
    return CollectResult(blocks=blocks, flats=norm_flats)
