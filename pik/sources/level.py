"""Источник данных Level Group (level.ru).

Открытый JSON API (Django REST Framework): `/api/flat/` отдаёт квартиры
с limit/offset-пагинацией и полем `next`. Ответы медленные — держим
небольшой limit и щедрый таймаут.
"""
from __future__ import annotations

import logging

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
_SITE = "https://level.ru"
_PAGE_LIMIT = 100   # limit>=500 у API таймаутит
_MAX_PAGES = 60

log = logging.getLogger("pik.sources.level")


def _settlement(fl: dict) -> str | None:
    year = fl.get("completion_year")
    quarter = fl.get("completion_quarter")
    if year and quarter:
        return f"{quarter} кв. {year}"
    return str(year) if year else None


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
        section_no=None,  # section_title у Level нечисловой ("1-1")
        settlement_date=_settlement(fl),
        url=(_SITE + url) if url else None,
        finish=fl.get("renovation"),
        number=fl.get("section_title"),
    )


def collect(*, session: requests.Session | None = None) -> CollectResult:
    """Обходит весь каталог квартир Level по limit/offset-пагинации."""
    s = session or make_session()
    norm_flats: list[NormFlat] = []
    blocks: dict[str, str] = {}  # slug → project name

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
            norm_flats.append(_to_norm(fl))
        url = payload.get("next")
        if not url:
            break
    else:
        log.warning("Level: достигнут предел в %d страниц", _MAX_PAGES)

    norm_blocks = [
        NormBlock(native_id=slug, name=name, slug=slug)
        for slug, name in blocks.items()
    ]
    log.info("Level: %d ЖК, %d квартир", len(norm_blocks), len(norm_flats))
    return CollectResult(blocks=norm_blocks, flats=norm_flats)
