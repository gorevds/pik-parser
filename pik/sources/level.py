"""Источник данных Level Group (level.ru).

Открытый JSON API (Django REST Framework): `/api/flat/` отдаёт квартиры
с limit/offset-пагинацией и полем `next`. Ответы медленные — держим
небольшой limit и щедрый таймаут.
"""
from __future__ import annotations

import logging
from html import unescape

import requests

from pik.sources.base import (
    CollectResult,
    NormBlock,
    NormFlat,
    make_session,
    request_json,
)


DEVELOPER = "Level"
_FLATS_URL = "https://level.ru/api/flat/"
_PROJECTS_URL = "https://level.ru/api/project/"
_SITE = "https://level.ru"
_PAGE_LIMIT = 100   # limit>=500 у API таймаутит
_MAX_PAGES = 60

log = logging.getLogger("pik.sources.level")


def _coords(raw: str | None) -> tuple[float, float] | None:
    if not raw or "," not in raw:
        return None
    try:
        lat, lng = raw.split(",", 1)
        return float(lat.strip()), float(lng.strip())
    except (ValueError, AttributeError):
        return None


def _fetch_project_meta(session: requests.Session) -> dict[str, dict]:
    """{slug → meta} с метро/координатами/адресом. ОДИН HTTP-вызов на всё."""
    out: dict[str, dict] = {}
    try:
        payload = request_json(session, "GET", _PROJECTS_URL, timeout=30.0)
    except Exception as exc:  # noqa: BLE001
        log.warning("Level: список проектов не получен: %s", exc)
        return out
    items = payload if isinstance(payload, list) else (payload.get("results") or [])
    for p in items:
        slug = p.get("slug")
        # У Level ДВА slug-пространства: /api/flat/ отдаёт короткие коды
        # («bauman», «akadem», «sav17»), а /api/project/ — полные имена
        # («baumanskaya», «akademicheskaya», «savvinskaya-17»). short_slug
        # — соединительное поле; кладём meta под обоими ключами, иначе
        # 13 из 15 ЖК останутся без metro/координат.
        short_slug = p.get("short_slug")
        if not slug and not short_slug:
            continue
        meta: dict = {}
        if (lat_lng := _coords(p.get("coords"))):
            meta["latitude"], meta["longitude"] = lat_lng
        if p.get("address"):
            # API возвращает «Большая Почтовая&nbsp;ул.» — расшифровываем
            meta["address"] = unescape(p["address"]).replace("\xa0", " ")
        metro = p.get("metro") or {}
        if metro.get("name"):
            meta["metro_name"] = metro.get("name")
        # time_to_metro_min — int минут пешком
        tm = p.get("time_to_metro_min")
        if isinstance(tm, int):
            meta["metro_time_foot"] = tm
        if slug:
            out[slug] = meta
        if short_slug and short_slug != slug:
            out[short_slug] = meta
    return out


def _settlement(fl: dict) -> str | None:
    year = fl.get("completion_year")
    quarter = fl.get("completion_quarter")
    if year and quarter:
        return f"{quarter} кв. {year}"
    return str(year) if year else None


def _section_no(fl: dict) -> int | None:
    """section_title бывает простым числом («2») и составным («1-1»).

    Парсим в int только цельные числовые, иначе оставляем None — иначе
    int(«1-1») упадёт, и будет потеря данных по другим полям квартиры.
    """
    raw = fl.get("section_title")
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _to_norm(fl: dict) -> NormFlat:
    price = fl.get("price")
    old = fl.get("old_price")
    old_price = int(old) if (price and old and old > price) else None
    building = fl.get("building")
    url = fl.get("url")
    return NormFlat(
        native_id=fl["pk"],
        native_block_id=fl.get("project_slug") or fl.get("project"),
        rooms=fl.get("room"),
        area=fl.get("area"),
        floor=fl.get("floor"),
        price=price,
        meter_price=fl.get("ppm"),
        old_price=old_price,
        status="free" if fl.get("status") == 1 else str(fl.get("status")),
        bulk_name=(f"Корпус {building}" if building else None),
        section_no=_section_no(fl),
        settlement_date=_settlement(fl),
        url=(_SITE + url) if url else None,
        finish=fl.get("renovation"),
        number=fl.get("section_title"),
        plan_url=fl.get("plan") or fl.get("floor_plan"),
    )


def collect(*, session: requests.Session | None = None) -> CollectResult:
    """Обходит весь каталог квартир Level по limit/offset-пагинации."""
    s = session or make_session()
    norm_flats: list[NormFlat] = []
    blocks: dict[str, str] = {}  # slug → project name
    block_floors: dict[str, int] = {}  # slug → max(floors_section_total)

    url: str | None = _FLATS_URL
    params: dict | None = {"limit": _PAGE_LIMIT, "offset": 0}
    for _ in range(_MAX_PAGES):
        payload = request_json(s, "GET", url, params=params, timeout=60.0)
        params = None  # `next` уже содержит limit/offset
        for fl in payload.get("results") or []:
            slug = fl.get("project_slug") or fl.get("project")
            if not fl.get("pk") or not slug:
                continue
            blocks.setdefault(slug, fl.get("project") or slug)
            fst = fl.get("floors_section_total")
            if isinstance(fst, int) and fst > 0:
                block_floors[slug] = max(block_floors.get(slug, 0), fst)
            norm_flats.append(_to_norm(fl))
        url = payload.get("next")
        if not url:
            break
    else:
        log.warning("Level: достигнут предел в %d страниц", _MAX_PAGES)

    project_meta = _fetch_project_meta(s)
    norm_blocks = [
        NormBlock(
            native_id=slug, name=name, slug=slug,
            meta={"floors_max": block_floors.get(slug), **project_meta.get(slug, {})},
        )
        for slug, name in blocks.items()
    ]
    log.info("Level: %d ЖК, %d квартир", len(norm_blocks), len(norm_flats))
    return CollectResult(blocks=norm_blocks, flats=norm_flats)
