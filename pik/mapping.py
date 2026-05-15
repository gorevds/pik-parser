"""JSON ответы api.pik.ru → строки SQLite."""
from __future__ import annotations

import re
from typing import Optional


_RATE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*%")


def _finish_label(finish) -> Optional[str]:
    if not isinstance(finish, dict):
        return None
    if finish.get("whiteBox"):
        return "WhiteBox"
    if finish.get("isFinish"):
        return "С отделкой и мебелью" if finish.get("furniture") else "С отделкой"
    return "Без отделки"


def _parse_rate(name: Optional[str]) -> Optional[float]:
    if not name:
        return None
    match = _RATE_RE.search(name)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def _best_mortgage(item: dict) -> tuple[Optional[float], Optional[str]]:
    benefits = item.get("benefits") or {}
    if not isinstance(benefits, dict):
        return None, None
    mortgages = benefits.get("mortgage") or []

    main = next(
        (m for m in mortgages if isinstance(m, dict) and m.get("isMain")),
        None,
    )
    if main is not None:
        return _parse_rate(main.get("name")), main.get("name")

    rated = [
        (rate, m.get("name"))
        for m in mortgages
        if isinstance(m, dict)
        for rate in [_parse_rate(m.get("name"))]
        if rate is not None
    ]
    if not rated:
        return None, None
    rate, name = min(rated, key=lambda x: x[0])
    return rate, name


def to_flat_row(item: dict, *, first_seen: str) -> dict:
    bulk = item.get("bulk") if isinstance(item.get("bulk"), dict) else {}
    section = item.get("section") if isinstance(item.get("section"), dict) else {}
    layout = item.get("layout") if isinstance(item.get("layout"), dict) else {}

    plan_url = layout.get("flat_plan_svg") or layout.get("flat_plan_render")

    return {
        "id":              item["id"],
        "guid":            item["guid"],
        "block_id":        item["block_id"],
        "bulk_id":         item.get("bulk_id"),
        "section_id":      item.get("section_id"),
        "layout_id":       item.get("layout_id"),
        "bulk_name":       bulk.get("name"),
        "section_no":      section.get("number"),
        "floor":           item.get("floor"),
        "rooms":           str(item["rooms"]) if item.get("rooms") is not None else None,
        "rooms_fact":      item.get("rooms_fact"),
        "is_studio":       item.get("is_studio"),
        "area":            item.get("area"),
        "area_kitchen":    item.get("areaKitchen"),
        "area_living":     item.get("areaLiving"),
        "number":          item.get("number"),
        "name":            item.get("name"),
        "url":             item.get("url"),
        "pdf_url":         item.get("pdf"),
        "plan_url":        plan_url,
        "ceiling_height":  item.get("ceilingHeight"),
        "settlement_date": item.get("settlementDate") or bulk.get("settlement_date") or None,
        "first_seen":      first_seen,
    }


def to_snapshot_row(item: dict, *, scan_date: str, scan_ts: str) -> dict:
    rate, name = _best_mortgage(item)
    return {
        "flat_id":            item["id"],
        "scan_date":          scan_date,
        "scan_ts":            scan_ts,
        "status":             item.get("status"),
        "price":              item.get("price"),
        "meter_price":        item.get("meterPrice"),
        "old_price":          item.get("oldPrice"),
        "discount":           item.get("discount"),
        "finish":             _finish_label(item.get("finish")),
        "mortgage_min_rate":  rate,
        "mortgage_best_name": name,
        "updated_at":         item.get("updatedAt"),
    }
