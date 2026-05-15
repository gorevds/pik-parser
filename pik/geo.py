"""Гео-вычисления: расстояние до центра, парсинг метро из PIK API."""
from __future__ import annotations

import math
from typing import Optional


# Центры городов (для расчёта дистанции через haversine)
CITY_CENTERS: dict[str, tuple[float, float]] = {
    "msk":               (55.7520, 37.6175),   # Кремль
    "spb":               (59.9343, 30.3351),   # Дворцовая
    "ekb":               (56.8389, 60.6057),   # Площадь 1905 года
    "kazan":             (55.7887, 49.1221),   # Кремль (Казанский)
    "nn":                (56.3287, 44.0020),   # Нижегородский кремль
    "yaroslavl":         (57.6261, 39.8845),
    "vladivostok":       (43.1198, 131.8869),
    "khabarovsk":        (48.4827, 135.0838),
    "novorossiisk":      (44.7167, 37.7833),
    "yuzhno-sakhalinsk": (46.9588, 142.7388),
    "ulan-ude":          (51.8335, 107.5842),
    "blagoveshchensk":   (50.2906, 127.5272),
    "tyumen":            (57.1530, 65.5343),
    "obninsk":           (55.0958, 36.6133),
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Расстояние по дуге большого круга, км."""
    r = 6371.0
    p = math.pi / 180
    a = (
        0.5 - math.cos((lat2 - lat1) * p) / 2
        + math.cos(lat1 * p) * math.cos(lat2 * p)
        * (1 - math.cos((lon2 - lon1) * p)) / 2
    )
    return r * 2 * math.asin(math.sqrt(a))


# Карта line.type → человекочитаемая метка
LINE_TYPE_LABEL = {
    1: "M",         # метро
    2: "МЦК",
    3: "МЦД",
    4: "электричка",
}


def _to_int(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def primary_metro(stations: list) -> Optional[dict]:
    """Вернуть самую близкую станцию по timeOnFoot."""
    if not stations:
        return None
    def fkey(s):
        t = _to_int(s.get("timeOnFoot"))
        return t if t is not None else 99999
    return sorted(stations, key=fkey)[0]


def derive_city(slug: Optional[str]) -> str:
    """slug 'kazan/siberovo' -> 'kazan'; 'narvin' -> 'msk'."""
    if not slug:
        return "msk"
    if "/" in slug:
        return slug.split("/", 1)[0]
    return "msk"


def extract_block_meta(item: dict, *, slug: Optional[str] = None) -> dict:
    """Из v2/flat item достать координаты, метро, адрес, дистанцию до центра."""
    bulk = item.get("bulk") or {}
    block = item.get("block") or {}
    stations = item.get("metroStationsServiceNew") or []

    p = primary_metro(stations)
    p_line = (p or {}).get("line") or {}

    lat = (
        _to_float(bulk.get("latitude"))
        or _to_float(block.get("latitude"))
    )
    lon = (
        _to_float(bulk.get("longitude"))
        or _to_float(block.get("longitude"))
    )

    distance_km = None
    city = derive_city(slug)
    if lat is not None and lon is not None and city in CITY_CENTERS:
        c_lat, c_lon = CITY_CENTERS[city]
        distance_km = round(haversine_km(lat, lon, c_lat, c_lon), 1)

    floors_max = _to_int(bulk.get("floors"))

    return {
        "metro_name":         (p or {}).get("name"),
        "metro_line_name":    p_line.get("name"),
        "metro_line_type":    _to_int(p_line.get("type")),
        "metro_time_foot":    _to_int((p or {}).get("timeOnFoot")),
        "metro_time_transport": _to_int((p or {}).get("timeOnTransport")),
        "latitude":           lat,
        "longitude":          lon,
        "address":            item.get("address") or bulk.get("build_adress"),
        "distance_km":        distance_km,
        "floors_max":         floors_max,
    }
