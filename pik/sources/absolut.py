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
      project { slug name title }
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
    )


def collect(*, session: requests.Session | None = None) -> CollectResult:
    """Курсорно обходит весь каталог квартир Абсолюта через GraphQL."""
    s = session or make_session()
    s.headers.update({"Origin": "https://www.absrealty.ru"})
    norm_flats: list[NormFlat] = []
    blocks: dict[str, str] = {}  # slug → project name

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
            norm_flats.append(_to_norm(node))
        page_info = conn.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
    else:
        log.warning("Абсолют: достигнут предел в %d страниц", _MAX_PAGES)

    norm_blocks = [
        NormBlock(native_id=slug, name=name, slug=slug)
        for slug, name in blocks.items()
    ]
    log.info("Абсолют: %d ЖК, %d квартир", len(norm_blocks), len(norm_flats))
    return CollectResult(blocks=norm_blocks, flats=norm_flats)
