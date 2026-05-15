import json

from pik.backfill_wayback import (
    _to_api_v2_shape,
    _wayback_date,
    _wayback_iso,
    build_urls,
    extract_flats_from_html,
)


def test_build_urls_substitutes_slug():
    urls = build_urls("foo-bar")
    assert "https://www.pik.ru/foo-bar" in urls
    assert "https://www.pik.ru/search/foo-bar" in urls
    assert all("foo-bar" in u for u in urls)


def test_wayback_timestamp_to_date():
    assert _wayback_date("20250629055318") == "2025-06-29"


def test_wayback_timestamp_to_iso():
    assert _wayback_iso("20250629055318") == "2025-06-29T05:53:18+00:00"


def test_extract_flats_returns_empty_on_no_next_data():
    assert extract_flats_from_html("<html><body>no script</body></html>") == []


def test_extract_flats_parses_minimal_next_data():
    payload = {
        "props": {
            "pageProps": {
                "initialState": {
                    "searchService": {
                        "filteredFlats": {
                            "data": {
                                "flats": [
                                    {"id": 1, "guid": "g", "price": 100,
                                     "rooms": 1, "floor": 5, "area": 30.0,
                                     "status": "free", "blockSlug": "narvin"}
                                ]
                            }
                        }
                    }
                }
            }
        }
    }
    html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'
    flats = extract_flats_from_html(html)
    assert len(flats) == 1
    assert flats[0]["id"] == 1


def test_to_api_v2_shape_converts_wayback_flat_to_api_shape():
    wb_flat = {
        "id": 980273,
        "guid": "c216c6a8",
        "area": 42.9,
        "floor": 18,
        "price": 16786770,
        "meterPrice": 391300,
        "oldPrice": None,
        "rooms": 1,
        "status": "free",
        "typeId": 1,
        "bulkName": "Корпус 1.3",
        "blockName": "Нарвин",
        "blockSlug": "narvin",
        "sectionNumber": 3,
        "settlementDate": "2029-03-10T00:00:00+00:00",
        "href": "/flat/980273",
    }
    api = _to_api_v2_shape(wb_flat, block_id=1165)
    assert api["id"] == 980273
    assert api["block_id"] == 1165
    assert api["floor"] == 18
    assert api["price"] == 16786770
    assert api["meterPrice"] == 391300
    assert api["status"] == "free"
    assert api["url"] == "https://www.pik.ru/flat/980273"
    assert api["bulk"]["name"] == "Корпус 1.3"
    assert api["section"]["number"] == 3
    assert api["settlementDate"] == "2029-03-10T00:00:00+00:00"
