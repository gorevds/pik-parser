import json
import sqlite3

from bin.backfill import _blocks_with_slug, _run_all_blocks
from pik.backfill_wayback import (
    _to_api_v2_shape,
    _wayback_date,
    _wayback_iso,
    build_urls,
    extract_flats_from_html,
)
from pik.blocks_meta import upsert_block_meta
from pik.store import apply_schema


def test_blocks_with_slug_skips_missing_file(tmp_path):
    assert _blocks_with_slug(tmp_path / "nope.db") == []


def test_blocks_with_slug_filters_null_and_blank(tmp_path):
    db = tmp_path / "pik.db"
    conn = sqlite3.connect(db)
    apply_schema(conn)
    upsert_block_meta(conn, block_id=1, name="A", developer="ПИК", slug="narvin", meta={}, scan_ts="t")
    upsert_block_meta(conn, block_id=2, name="B", developer="ПИК", slug=None, meta={}, scan_ts="t")
    upsert_block_meta(conn, block_id=3, name="C", developer="ПИК", slug="  ", meta={}, scan_ts="t")
    upsert_block_meta(
        conn, block_id=4, name="D", developer="ПИК", slug="kazan/siberovo", meta={}, scan_ts="t"
    )
    conn.close()
    assert _blocks_with_slug(db) == [(1, "narvin"), (4, "kazan/siberovo")]


def test_run_all_blocks_counts_failures(tmp_path, monkeypatch):
    def fake_backfill(db_path, *, slug, block_id, **kw):
        if block_id == 2:
            raise RuntimeError("boom")
        return {"snapshots": 5, "unique_flats": 3, "dates": 2, "errors": 0}

    monkeypatch.setattr("bin.backfill.backfill", fake_backfill)
    bad = _run_all_blocks(
        tmp_path / "x.db", [(1, "a"), (2, "b"), (3, "c")],
        from_date="20250601", to_date="20260601", sleep=0, workers=3,
    )
    assert bad == 1


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
