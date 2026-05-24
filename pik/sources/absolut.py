"""Источник данных ГК «Абсолют» / Абсолют Недвижимость (absrealty.ru).

Открытый GraphQL: POST `/graphql/`, запрос `allFlats` с Relay-курсорной
пагинацией (`first` / `after`). Цены приходят дробными — округляем.
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


DEVELOPER = "Абсолют"
_GRAPHQL_URL = "https://www.absrealty.ru/graphql/"
_PAGE_SIZE = 100
_MAX_PAGES = 60

_ALL_FLATS_QUERY = """
query allFlats($first: Int, $after: String, $orderBy: String) {
  allFlats(first: $first, after: $after, orderBy: $orderBy) {
    totalCount
    pageInfo { endCursor hasNextPage }
    edges { node {
      pk number rooms area price originPrice hasDiscount facing
      plan planPng mortgageMinRate
      buildingFloor { number }
      project {
        slug name title address coords
        projectmetroSet { walkingTime timeOnCar metro { name } }
      }
      building { number completionYear completionQuarter }
      section { number }
      floor { number }
    } }
  }
}
""".strip()

log = logging.getLogger("pik.sources.absolut")


def _round_price(value) -> int | None:
    try:
        return round(float(value))
    except (TypeError, ValueError):
        return None


def _project_meta(project: dict) -> dict:
    """Метро/координаты/адрес из node.project (Абсолют GraphQL)."""
    meta: dict = {}
    coords = project.get("coords")
    if coords and isinstance(coords, str) and "," in coords:
        try:
            lat, lng = coords.split(",", 1)
            meta["latitude"] = float(lat.strip())
            meta["longitude"] = float(lng.strip())
        except (ValueError, AttributeError):
            pass
    if project.get("address"):
        meta["address"] = project["address"]
    primary = (project.get("projectmetroSet") or [None])[0]
    if primary:
        m = primary.get("metro") or {}
        if m.get("name"):
            meta["metro_name"] = m["name"]
        wt = primary.get("walkingTime")
        if isinstance(wt, int):
            meta["metro_time_foot"] = wt
        tc = primary.get("timeOnCar")
        if isinstance(tc, int):
            meta["metro_time_transport"] = tc
    return meta


def _settlement(building: dict) -> str | None:
    year = building.get("completionYear")
    quarter = building.get("completionQuarter")
    if year and quarter:
        return f"{quarter} кв. {year}"
    return str(year) if year else None


def _to_norm(node: dict) -> NormFlat:
    project = node.get("project") or {}
    building = node.get("building") or {}
    section = node.get("section") or {}
    floor = node.get("floor") or {}
    price = _round_price(node.get("price"))
    # доверяем флагу hasDiscount: originPrice бывает выше price и без скидки
    old_price = _round_price(node.get("originPrice")) if node.get("hasDiscount") else None
    bnum = building.get("number")
    return NormFlat(
        native_id=node["pk"],
        native_block_id=project.get("slug"),
        rooms=node.get("rooms"),
        area=node.get("area"),
        floor=floor.get("number"),
        price=price,
        old_price=old_price,
        status="free",  # allFlats отдаёт только доступные квартиры
        bulk_name=(f"Корпус {bnum}" if bnum not in (None, "") else None),
        section_no=section.get("number"),
        settlement_date=_settlement(building),
        url=None,
        finish="С отделкой" if node.get("facing") else "Без отделки",
        number=node.get("number"),
        plan_url=node.get("plan") or node.get("planPng"),
    )


def collect(*, session: requests.Session | None = None) -> CollectResult:
    """Курсорно обходит весь каталог квартир Абсолюта через GraphQL."""
    s = session or make_session()
    s.headers.update({"Origin": "https://www.absrealty.ru"})
    norm_flats: list[NormFlat] = []
    blocks: dict[str, str] = {}  # slug → project name
    block_floors: dict[str, int] = {}  # slug → max(buildingFloor)
    project_meta: dict[str, dict] = {}  # slug → метро/координаты/адрес

    after: str | None = None
    for _ in range(_MAX_PAGES):
        payload = request_json(
            s, "POST", _GRAPHQL_URL,
            timeout=60.0,  # GraphQL-страница на 100 квартир отвечает небыстро
            json={
                "operationName": "allFlats",
                "query": _ALL_FLATS_QUERY,
                "variables": {"first": _PAGE_SIZE, "after": after,
                              "orderBy": "price"},
            },
        )
        if payload.get("errors"):
            raise SourceError(f"GraphQL errors: {payload['errors']}")
        conn = (payload.get("data") or {}).get("allFlats") or {}
        for edge in conn.get("edges") or []:
            node = edge.get("node") or {}
            slug = (node.get("project") or {}).get("slug")
            if not node.get("pk") or not slug:
                continue
            project = node["project"]
            blocks.setdefault(slug, project.get("name") or project.get("title") or slug)
            # project повторяется в каждом edge, парсим один раз на slug
            if slug not in project_meta:
                project_meta[slug] = _project_meta(project)
            # buildingFloor — объект {number: int}; floors_max не отдаётся
            # напрямую, агрегируем MAX(buildingFloor.number) как нижнюю оценку
            bf_obj = node.get("buildingFloor") or {}
            bf = bf_obj.get("number") if isinstance(bf_obj, dict) else None
            if isinstance(bf, int) and bf > 0:
                block_floors[slug] = max(block_floors.get(slug, 0), bf)
            norm_flats.append(_to_norm(node))
        page_info = conn.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
    else:
        log.warning("Абсолют: достигнут предел в %d страниц", _MAX_PAGES)

    norm_blocks = [
        NormBlock(
            native_id=slug, name=name, slug=slug,
            meta={"floors_max": block_floors.get(slug), **project_meta.get(slug, {})},
        )
        for slug, name in blocks.items()
    ]
    log.info("Абсолют: %d ЖК, %d квартир", len(norm_blocks), len(norm_flats))
    return CollectResult(blocks=norm_blocks, flats=norm_flats)
