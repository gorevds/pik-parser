"""Источник данных ГК ФСК (fsk.ru).

Открытый JSON API: `/api/complex/` отдаёт все ЖК, `/api/v3/flats/all`
с параметром `complex_slug` — все доступные квартиры одного ЖК.
Москва и область — это `city_id == 1`.
"""
from __future__ import annotations

import logging

import requests

from pik.sources.base import (
    CollectResult,
    NormBlock,
    NormFlat,
    SourceError,
    make_session,
    request_json,
)


DEVELOPER = "ГК ФСК"
_COMPLEX_URL = "https://fsk.ru/api/complex/"
_FLATS_URL = "https://fsk.ru/api/v3/flats/all"
_MSK_CITY_ID = 1

log = logging.getLogger("pik.sources.fsk")


def _to_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _finish_label(fl: dict) -> str | None:
    if not fl.get("finishing"):
        return "Без отделки"
    return "С отделкой и мебелью" if fl.get("furniture") else "С отделкой"


def _status_label(raw) -> str | None:
    """API отдаёт только продающиеся квартиры (status 0).

    Прочее на всякий случай сохраняем как есть; отсутствующий статус — None,
    а не строка «None» (иначе в snapshots осел бы мусорный литерал).
    """
    if raw == 0:
        return "free"
    return None if raw is None else str(raw)


def _to_norm(fl: dict, block_slug: str) -> NormFlat:
    price = fl.get("price")
    wo_discount = fl.get("priceWoDiscount")
    # priceWoDiscount имеет смысл как «старая цена» только когда он выше текущей
    old_price = wo_discount if (price and wo_discount and wo_discount > price) else None
    corpus = fl.get("corpus") if isinstance(fl.get("corpus"), dict) else {}
    section = fl.get("section") if isinstance(fl.get("section"), dict) else {}
    native_id = fl.get("externalId") or fl.get("_id")
    # per-flat URL: `?id={externalId}` — рабочий шаблон, проверено curl-ом.
    # Раньше отдавали ссылку на листинг ЖК — пользователю было не понятно
    # к какой квартире она ведёт.
    url = (f"https://fsk.ru/{block_slug}/flats?id={native_id}"
           if native_id else f"https://fsk.ru/{block_slug}/flats")
    return NormFlat(
        native_id=native_id,
        native_block_id=block_slug,
        rooms=fl.get("rooms"),
        area=fl.get("areaTotal"),
        floor=fl.get("floorNumber"),
        price=price,
        meter_price=fl.get("pricePerMeter"),
        old_price=old_price,
        status=_status_label(fl.get("status")),
        bulk_name=(f"Корпус {corpus['number']}" if corpus.get("number") else None),
        section_no=_to_int(section.get("number")),
        settlement_date=corpus.get("dateDelivery"),
        url=url,
        finish=_finish_label(fl),
        number=fl.get("number"),
        plan_url=fl.get("plan"),  # абсолютный SVG cdn.fsk.ru
        # crmObjectType бывает «Квартира» / «Апартамент» / др.
        is_apartment=("апарт" in (fl.get("crmObjectType") or "").lower()),
    )


def collect(
    *, session: requests.Session | None = None, city_id: int = _MSK_CITY_ID
) -> CollectResult:
    """Обходит все ЖК ФСК заданного региона и собирает квартиры."""
    s = session or make_session()
    complexes = request_json(s, "GET", _COMPLEX_URL)
    if not isinstance(complexes, list):
        complexes = complexes.get("data") or complexes.get("results") or []

    region = [c for c in complexes if c.get("city_id") == city_id]
    log.info("ФСК: %d ЖК в регионе city_id=%d", len(region), city_id)

    blocks: list[NormBlock] = []
    flats: list[NormFlat] = []
    for c in region:
        slug = c.get("slug")
        if not slug:
            continue
        # ЖК без квартир в продаже (сдан/распродан) — пропускаем запрос
        if not (c.get("flats") or {}).get("all"):
            continue
        try:
            raw = request_json(
                s, "GET", _FLATS_URL,
                params={"complex_slug": slug, "limit": 5000},
            )
        except SourceError as exc:
            # один сбойный ЖК не должен ронять весь обход застройщика
            log.warning("ФСК: %s — пропущен из-за ошибки: %s", slug, exc)
            continue
        items = raw if isinstance(raw, list) else (raw.get("data") or [])
        if not items:
            continue
        # FSK API не отдаёт floors_max явно — оцениваем как MAX(floorNumber)
        # по квартирам ЖК (нижняя граница реальной этажности здания).
        floors = [_to_int(it.get("floorNumber")) for it in items]
        floors = [f for f in floors if f]
        blocks.append(NormBlock(
            native_id=slug,
            name=c.get("title") or slug,
            slug=slug,
            meta={
                "latitude": c.get("lat"),
                "longitude": c.get("lng"),
                "address": c.get("post_address"),
                "floors_max": max(floors) if floors else None,
            },
        ))
        flats.extend(_to_norm(fl, slug) for fl in items)
        log.info("ФСК: %s — %d квартир", slug, len(items))

    log.info("ФСК: всего %d ЖК, %d квартир", len(blocks), len(flats))
    return CollectResult(blocks=blocks, flats=flats)
