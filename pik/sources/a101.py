"""Источник данных А101 (a101.ru).

Открытый JSON API (Django REST Framework): `/api/flats/` отдаёт все
квартиры с limit/offset-пагинацией и полем `next`. Цена — `actual_price`
(текущая со скидкой), `price` — прайсовая (база до скидки).
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


DEVELOPER = "А101"
_FLATS_URL = "https://a101.ru/api/flats/"
_PAGE_LIMIT = 1000
_MAX_PAGES = 50  # предохранитель: 50 * 1000 квартир с большим запасом

log = logging.getLogger("pik.sources.a101")


def _to_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_norm(fl: dict) -> NormFlat:
    price = fl.get("actual_price") or fl.get("price")
    base = fl.get("price") or fl.get("base_price")
    old_price = base if (price and base and base > price) else None
    building = fl.get("building_number") or fl.get("building")
    return NormFlat(
        native_id=fl["id"],
        native_block_id=fl.get("project_slug") or fl.get("project"),
        # А101 считает студию как 1 комнату — ориентируемся на флаг studio
        rooms=0 if fl.get("studio") else fl.get("room"),
        area=fl.get("area"),
        floor=_to_int(fl.get("floor")),
        price=price,
        meter_price=fl.get("actual_ppm") or fl.get("ppm"),
        old_price=old_price,
        status="free" if fl.get("status") == 4 else str(fl.get("status")),
        bulk_name=(f"Корпус {building}" if building else None),
        section_no=_to_int(fl.get("section_number")),
        settlement_date=fl.get("stage_key_transfer_date"),
        url=None,  # публичного per-flat URL в API нет
        finish="WhiteBox" if fl.get("whitebox") else None,
        number=str(fl["number"]) if fl.get("number") is not None
               else fl.get("article"),
    )


def collect(*, session: requests.Session | None = None) -> CollectResult:
    """Обходит весь каталог квартир А101 по limit/offset-пагинации."""
    s = session or make_session()
    norm_flats: list[NormFlat] = []
    blocks: dict[str, str] = {}  # slug → project name

    url: str | None = _FLATS_URL
    params: dict | None = {"limit": _PAGE_LIMIT, "offset": 0, "order": "actual_price"}
    for _ in range(_MAX_PAGES):
        payload = request_json(s, "GET", url, params=params)
        params = None  # `next` уже содержит limit/offset
        for fl in payload.get("results") or []:
            if not fl.get("id") or not fl.get("project_slug"):
                continue
            blocks.setdefault(fl["project_slug"], fl.get("project") or fl["project_slug"])
            norm_flats.append(_to_norm(fl))
        url = payload.get("next")
        if not url:
            break
    else:
        log.warning("А101: достигнут предел в %d страниц", _MAX_PAGES)

    norm_blocks = [
        NormBlock(native_id=slug, name=name, slug=slug)
        for slug, name in blocks.items()
    ]
    log.info("А101: %d ЖК, %d квартир", len(norm_blocks), len(norm_flats))
    return CollectResult(blocks=norm_blocks, flats=norm_flats)
