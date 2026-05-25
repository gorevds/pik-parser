"""Источник данных Инград (ingrad.ru).

Открытый REST на отдельном поддомене `new-api.ingrad.ru/api/flats`. Один
вызов отдаёт `allCount` + `list`. Фильтр `type=flat` отсекает офисы и
кладовки (API смешивает их). Поле `link` уже содержит per-flat путь —
префиксим хостом `ingrad.ru`.

Координаты — на уровне house (корпуса), не ЖК; берём первый встретившийся
корпус каждого ЖК. Метро (slug-латиницей) кладём как есть — кириллица
подтянется post-hoc через `_assign_nearest_metro` если есть lat/lng.
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

DEVELOPER = "Инград"
_FLATS_URL = "https://new-api.ingrad.ru/api/flats"
# www.ingrad.ru канонический — без `www` отдаёт 301 redirect.
# Trailing slash тоже не нужен (308 на версию без него).
_SITE = "https://www.ingrad.ru"


def _has_cyrillic(s: str) -> bool:
    return any('а' <= c.lower() <= 'я' or c.lower() == 'ё' for c in s)
_PAGE_SIZE = 500          # сейчас вся выдача (~2k) влезает в один запрос, но
_MAX_PAGES = 20           # пагинируем на случай роста

log = logging.getLogger("pik.sources.ingrad")


def _to_int(value) -> int | None:
    try: return int(value)
    except (TypeError, ValueError): return None


def _coords_pair(raw) -> tuple[float, float] | None:
    """API даёт coords как строку 'lat,lng' либо как литерал 'None' — фильтруем."""
    if not raw or raw == "None" or not isinstance(raw, str) or "," not in raw:
        return None
    try:
        lat, lng = raw.split(",", 1)
        return float(lat.strip()), float(lng.strip())
    except (ValueError, AttributeError):
        return None


def _settlement_from_house(house: dict | None) -> str | None:
    if not house:
        return None
    y = house.get("settlement_year")
    q = house.get("settlement_quarter")
    if y and q:
        return f"{q} кв. {y}"
    # fallback на «settling_text» — но он часто длинный «1 квартал 2024 г.»
    s = house.get("settling_text")
    return s if s else (str(y) if y else None)


def _to_norm(fl: dict) -> NormFlat | None:
    """Возвращает None если квартира — НЕ жилое (office/storeroom)."""
    if fl.get("type") != "flat" or fl.get("isStoreroom"):
        return None
    estate = fl.get("estateId") or {}
    house = fl.get("houseId") or {}
    section = fl.get("section") or {}
    slug = estate.get("code") or str(estate.get("id") or "")
    if not slug:
        return None
    price = _to_int(fl.get("price"))
    base = _to_int(fl.get("priceNoDiscount"))
    old_price = base if (price and base and base > price) else None
    link = (fl.get("link") or "").rstrip("/")  # без trailing slash → 200 без redirect
    url = (_SITE + link) if link.startswith("/") else (link or None)
    return NormFlat(
        native_id=fl.get("id"),
        native_block_id=slug,
        rooms=_to_int(fl.get("rooms")),
        area=fl.get("square"),
        floor=_to_int(fl.get("floorNum")),
        price=price,
        meter_price=_to_int(fl.get("squareCost")),
        old_price=old_price,
        status="free" if fl.get("status") == "free" else str(fl.get("status")),
        bulk_name=(house.get("name") or None),
        section_no=_to_int(section.get("number") or fl.get("sectionNum")),
        settlement_date=_settlement_from_house(house),
        url=url,
        # `finish` приходит уже человеческой строкой («Без отделки», «WhiteBox»)
        finish=fl.get("finish"),
        number=str(fl["number"]) if fl.get("number") is not None else None,
        plan_url=fl.get("planning") or fl.get("imageMain"),
        # Инград — массовый жилой застройщик, апартаментов не строит.
        # isPenthouse — премиум-планировка, но юридически жилая квартира.
        is_apartment=False,
    )


def _project_meta_from_estate_and_house(estate: dict, house: dict) -> dict:
    """meta: {latitude, longitude, address, metro_name, metro_time_foot}."""
    meta: dict = {}
    if (ll := _coords_pair(house.get("coords"))):
        meta["latitude"], meta["longitude"] = ll
    # address на корпусе полный («Московская область, г. Мытищи, …»),
    # на estate чаще пустой — берём house в приоритете.
    addr = house.get("address") if house.get("address") not in (None, "None") else None
    if not addr and estate.get("address") not in (None, "None"):
        addr = estate.get("address")
    if addr:
        meta["address"] = addr
    if (m := estate.get("metro")) and m not in (None, "None"):
        # slug API даёт латиницей («medvedkovo»). Записываем metro_name
        # ТОЛЬКО если значение кириллицей — иначе оставляем NULL, и
        # store._assign_nearest_metro post-hoc подберёт ближайшую станцию
        # из PIK-словаря по координатам (там кириллица).
        if _has_cyrillic(m):
            meta["metro_name"] = m
            tt = _to_int(estate.get("timeToMetro"))
            # timeToMetroType: 'foot' / 'transport'
            if tt:
                if estate.get("timeToMetroType") == "foot":
                    meta["metro_time_foot"] = tt
                else:
                    meta["metro_time_transport"] = tt
    floors = _to_int(house.get("floorsCount"))
    if floors:
        meta["floors_max"] = floors
    return meta


def collect(*, session: requests.Session | None = None) -> CollectResult:
    """Постранично обходит каталог квартир Инграда (только type=flat)."""
    s = session or make_session()
    norm_flats: list[NormFlat] = []
    project_meta: dict[str, dict] = {}
    block_names: dict[str, str] = {}

    page = 1
    for _ in range(_MAX_PAGES):
        # API ингреда регулярно отвечает 30-60 сек на полный list — дефолт
        # 40s даёт false-negative при нормальной работе. 90s + ретраи из
        # request_json дают разумный шанс пройти медленный peak.
        payload = request_json(
            s, "GET", _FLATS_URL, timeout=90.0,
            params={"numberElementsPage": _PAGE_SIZE, "page": page, "type": "flat"},
        )
        items = payload.get("list") or []
        if not items:
            break
        for fl in items:
            nf = _to_norm(fl)
            if not nf: continue
            norm_flats.append(nf)
            slug = nf.native_block_id
            estate = fl.get("estateId") or {}
            house = fl.get("houseId") or {}
            if slug not in block_names:
                block_names[slug] = estate.get("name") or slug
            # meta инициализируем от ПЕРВОГО встретившегося корпуса; для
            # последующих корпусов «доливаем» только те поля, которых не
            # хватает (первый корпус мог быть без coords/metro/address —
            # без долива поле залипнет навсегда NULL).
            if slug not in project_meta:
                project_meta[slug] = _project_meta_from_estate_and_house(estate, house)
            else:
                pm = project_meta[slug]
                extra = _project_meta_from_estate_and_house(estate, house)
                for k in ("latitude", "longitude", "address", "metro_name",
                          "metro_time_foot", "metro_time_transport"):
                    if k not in pm and k in extra:
                        pm[k] = extra[k]
                # floors_max — максимум по всем корпусам ЖК
                if (fc := _to_int(house.get("floorsCount"))):
                    cur = pm.get("floors_max") or 0
                    if fc > cur:
                        pm["floors_max"] = fc
        if len(items) < _PAGE_SIZE:
            break
        page += 1
    else:
        log.warning("Инград: достигнут предел в %d страниц", _MAX_PAGES)

    norm_blocks = [
        NormBlock(native_id=slug, name=name, slug=slug,
                  meta=project_meta.get(slug, {}))
        for slug, name in block_names.items()
    ]
    log.info("Инград: %d ЖК, %d квартир", len(norm_blocks), len(norm_flats))
    return CollectResult(blocks=norm_blocks, flats=norm_flats)
