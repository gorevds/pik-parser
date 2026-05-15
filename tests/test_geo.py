import math
from pik.geo import (
    extract_block_meta, haversine_km, primary_metro, derive_city, CITY_CENTERS,
)


def test_haversine_kremlin_to_self_is_zero():
    lat, lon = CITY_CENTERS["msk"]
    assert haversine_km(lat, lon, lat, lon) == 0


def test_haversine_known_distance():
    # Кремль → Водный стадион ≈ 12 км
    msk = CITY_CENTERS["msk"]
    d = haversine_km(msk[0], msk[1], 55.8400, 37.4880)
    assert 11 < d < 14


def test_derive_city_from_slug():
    assert derive_city("kazan/siberovo") == "kazan"
    assert derive_city("narvin") == "msk"
    assert derive_city(None) == "msk"
    assert derive_city("spb/aeronaut") == "spb"


def test_primary_metro_picks_closest_on_foot():
    stations = [
        {"name": "Водный стадион", "timeOnFoot": "10", "line": {"name": "Замоскворецкая", "type": 1}},
        {"name": "Коптево",         "timeOnFoot": "20", "line": {"name": "МЦК",           "type": 2}},
        {"name": "Войковская",      "timeOnFoot": "5",  "line": {"name": "Замоскворецкая", "type": 1}},
    ]
    p = primary_metro(stations)
    assert p["name"] == "Войковская"


def test_primary_metro_handles_missing_time():
    stations = [
        {"name": "A", "timeOnFoot": None, "line": {"type": 1}},
        {"name": "B", "timeOnFoot": "12", "line": {"type": 1}},
    ]
    assert primary_metro(stations)["name"] == "B"


def test_extract_block_meta_full():
    item = {
        "address": "САО, г. Москва, Головинский, б-р Кронштадтский",
        "bulk": {
            "latitude": "55.8400", "longitude": "37.4880",
            "build_adress": "Кронштадтский", "floors": "38",
        },
        "block": {"latitude": 55.84, "longitude": 37.488},
        "metroStationsServiceNew": [
            {"name": "Водный стадион", "timeOnFoot": "10",
             "line": {"name": "Замоскворецкая", "type": 1}},
            {"name": "Коптево", "timeOnFoot": "20",
             "line": {"name": "МЦК", "type": 2}},
        ],
    }
    m = extract_block_meta(item, slug="narvin")
    assert m["metro_name"] == "Водный стадион"
    assert m["metro_line_type"] == 1
    assert m["metro_time_foot"] == 10
    assert m["floors_max"] == 38
    assert 11 < m["distance_km"] < 14
    assert m["address"].startswith("САО")


def test_extract_handles_empty():
    m = extract_block_meta({}, slug=None)
    assert m["metro_name"] is None
    assert m["distance_km"] is None


def test_extract_for_non_moscow_uses_local_center():
    item = {
        "bulk": {"latitude": "55.7887", "longitude": "49.1221"},
        "block": {},
        "metroStationsServiceNew": [],
    }
    m = extract_block_meta(item, slug="kazan/siberovo")
    # координаты ровно в центре Казани → 0 км
    assert m["distance_km"] == 0.0
