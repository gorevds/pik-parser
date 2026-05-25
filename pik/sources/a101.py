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
    safe_next_url,
)


DEVELOPER = "А101"
_FLATS_URL = "https://a101.ru/api/flats/"
_PROJECTS_URL = "https://a101.ru/api/projects/"
_PROJECT_URL_FMT = "https://a101.ru/api/projects/{slug}/"
_PAGE_LIMIT = 1000
_MAX_PAGES = 50  # предохранитель: 50 * 1000 квартир с большим запасом

log = logging.getLogger("pik.sources.a101")


def _coords(raw: str | None) -> tuple[float, float] | None:
    """Парсит «55.601127,37.220517» → (lat, lng). API возвращает строкой."""
    if not raw or "," not in raw:
        return None
    try:
        lat, lng = raw.split(",", 1)
        return float(lat.strip()), float(lng.strip())
    except (ValueError, AttributeError):
        return None


def _project_meta_from_detail(p: dict) -> dict:
    """Извлекает метро/координаты/адрес из /api/projects/<slug>/ payload.

    metro_set: [{metro_station: {name, line_color}, time_on_foot, time_on_car}]
    Берём первую станцию — это primary (ближайшая по умолчанию у А101).
    """
    meta: dict = {}
    if (lat_lng := _coords(p.get("coords"))):
        meta["latitude"], meta["longitude"] = lat_lng
    if p.get("address"):
        meta["address"] = p["address"]
    primary = (p.get("metro_set") or [None])[0]
    if primary:
        station = primary.get("metro_station") or {}
        if station.get("name"):
            meta["metro_name"] = station["name"]
        # time_on_foot null у проектов далеко от метро — fallback на time_on_car
        meta["metro_time_foot"] = primary.get("time_on_foot")
        meta["metro_time_transport"] = primary.get("time_on_car")
    return meta


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
        # per-flat страница на сайте: /kvartiry/<id>/ (проверено: 200 OK)
        url=f"https://a101.ru/kvartiry/{fl['id']}/",
        finish="WhiteBox" if fl.get("whitebox") else None,
        number=str(fl["number"]) if fl.get("number") is not None
               else fl.get("article"),
        plan_url=fl.get("floor_plan") or fl.get("big_layout_png"),
    )


def collect(*, session: requests.Session | None = None) -> CollectResult:
    """Обходит весь каталог квартир А101 по limit/offset-пагинации."""
    s = session or make_session()
    norm_flats: list[NormFlat] = []
    blocks: dict[str, str] = {}  # slug → project name
    block_floors: dict[str, int] = {}  # slug → max(max_floor) по всем корпусам

    url: str | None = _FLATS_URL
    # Без явного `order`: сортировка по умолчанию у API детерминирована и
    # привязана к неизменному ключу. Сортировать по `actual_price` нельзя —
    # цена меняется, и квартиры «переезжали» бы через границу offset между
    # запросами страниц (часть терялась бы или дублировалась за один скан).
    params: dict | None = {"limit": _PAGE_LIMIT, "offset": 0}
    for _ in range(_MAX_PAGES):
        payload = request_json(s, "GET", url, params=params)
        params = None  # `next` уже содержит limit/offset
        for fl in payload.get("results") or []:
            if not fl.get("id") or not fl.get("project_slug"):
                continue
            slug = fl["project_slug"]
            blocks.setdefault(slug, fl.get("project") or slug)
            mf = _to_int(fl.get("max_floor"))
            if mf:
                block_floors[slug] = max(block_floors.get(slug, 0), mf)
            norm_flats.append(_to_norm(fl))
        nxt = payload.get("next")
        if not nxt:
            break
        # Defense-in-depth: API может отдать произвольный URL в next.
        # Доверяем только a101.ru и поддоменам — иначе прерываем обход.
        url = safe_next_url(nxt, "a101.ru")
        if not url:
            log.warning("А101: подозрительный next %r, прерываю обход", nxt)
            break
    else:
        log.warning("А101: достигнут предел в %d страниц", _MAX_PAGES)

    # Метро/координаты/адрес — только в detail-эндпоинте проекта; list-эндпоинт
    # отдаёт metro_set, но без coords. Тянем detail по каждому из 9 проектов
    # — суммарно ~1 сек, делается раз в сутки.
    project_meta: dict[str, dict] = {}
    for slug in blocks:
        try:
            payload = request_json(
                s, "GET", _PROJECT_URL_FMT.format(slug=slug)
            )
        except Exception as exc:  # noqa: BLE001 — один сбойный проект не должен валить весь скан
            log.warning("А101: проект %s — meta не получена: %s", slug, exc)
            project_meta[slug] = {}
            continue
        project_meta[slug] = _project_meta_from_detail(payload)

    norm_blocks = [
        NormBlock(
            native_id=slug, name=name, slug=slug,
            meta={"floors_max": block_floors.get(slug), **project_meta.get(slug, {})},
        )
        for slug, name in blocks.items()
    ]
    log.info("А101: %d ЖК, %d квартир", len(norm_blocks), len(norm_flats))
    return CollectResult(blocks=norm_blocks, flats=norm_flats)
