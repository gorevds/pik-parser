import json
from pathlib import Path

from pik.mapping import to_flat_row, to_snapshot_row

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "sample_flat.json").read_text("utf-8")
)


def test_to_flat_row_extracts_stable_fields():
    row = to_flat_row(FIXTURE, first_seen="2026-05-15")
    assert row["id"] == FIXTURE["id"]
    assert row["block_id"] == 1165
    assert row["bulk_id"] == FIXTURE["bulk_id"]
    assert row["section_id"] == FIXTURE["section_id"]
    assert row["floor"] == FIXTURE["floor"]
    assert row["area"] == FIXTURE["area"]
    # rooms is always TEXT in DB regardless of int/str in JSON
    assert row["rooms"] == str(FIXTURE["rooms"])
    assert row["url"] == FIXTURE["url"]
    assert row["first_seen"] == "2026-05-15"
    # nested bulk.name -> bulk_name
    assert row["bulk_name"] == FIXTURE["bulk"]["name"]
    # nested section.number -> section_no
    assert row["section_no"] == FIXTURE["section"]["number"]
    # plan URL comes from layout.flat_plan_svg
    assert row["plan_url"] == FIXTURE["layout"]["flat_plan_svg"]
    # kitchen / living area
    assert row["area_kitchen"] == FIXTURE["areaKitchen"]
    assert row["area_living"] == FIXTURE["areaLiving"]
    assert row["ceiling_height"] == FIXTURE["ceilingHeight"]
    # settlement_date prefers top-level, falls back to bulk.settlement_date
    expected_settle = FIXTURE.get("settlementDate") or FIXTURE["bulk"]["settlement_date"]
    assert row["settlement_date"] == expected_settle


def test_to_flat_row_settlement_date_falls_back_to_bulk():
    item = {
        "id": 1, "guid": "g", "block_id": 1165, "rooms": 1,
        "bulk": {"name": "K", "settlement_date": "2029-03-10"},
        "section": {}, "layout": {},
    }
    row = to_flat_row(item, first_seen="2026-05-15")
    assert row["settlement_date"] == "2029-03-10"


def test_to_snapshot_row_extracts_volatile_fields():
    row = to_snapshot_row(
        FIXTURE, scan_date="2026-05-15", scan_ts="2026-05-15T06:00:00+03:00"
    )
    assert row["flat_id"] == FIXTURE["id"]
    assert row["scan_date"] == "2026-05-15"
    assert row["price"] == FIXTURE["price"]
    assert row["meter_price"] == FIXTURE["meterPrice"]
    assert row["status"] == FIXTURE["status"]
    # finish is encoded as a human-readable label, not raw dict
    assert isinstance(row["finish"], str) and row["finish"]


def test_finish_label_is_human_readable():
    item = dict(FIXTURE)
    item["finish"] = {"isFinish": True, "whiteBox": False, "furniture": False}
    row = to_snapshot_row(item, scan_date="x", scan_ts="x")
    assert row["finish"] == "С отделкой"

    item["finish"] = {"isFinish": False, "whiteBox": True, "furniture": False}
    row = to_snapshot_row(item, scan_date="x", scan_ts="x")
    assert row["finish"] == "WhiteBox"

    item["finish"] = {"isFinish": True, "whiteBox": False, "furniture": True}
    row = to_snapshot_row(item, scan_date="x", scan_ts="x")
    assert row["finish"] == "С отделкой и мебелью"

    item["finish"] = None
    row = to_snapshot_row(item, scan_date="x", scan_ts="x")
    assert row["finish"] is None


def test_to_snapshot_row_picks_main_mortgage_and_parses_rate_from_name():
    item = dict(FIXTURE)
    item["benefits"] = {
        "mortgage": [
            {"name": "Стандартная ипотека 18%",        "rate": 0, "isMain": False},
            {"name": "Семейная ипотека 6%",            "rate": 0, "isMain": True},
            {"name": "Ипотека 11,9% на весь срок",     "rate": 0, "isMain": False},
        ]
    }
    row = to_snapshot_row(item, scan_date="x", scan_ts="x")
    assert row["mortgage_best_name"] == "Семейная ипотека 6%"
    assert row["mortgage_min_rate"] == 6.0


def test_to_snapshot_row_falls_back_to_min_rate_when_no_main():
    item = dict(FIXTURE)
    item["benefits"] = {
        "mortgage": [
            {"name": "Стандартная 18%",  "rate": 0, "isMain": False},
            {"name": "IT-ипотека 5%",    "rate": 0, "isMain": False},
            {"name": "Семейная 11,9%",   "rate": 0, "isMain": False},
        ]
    }
    row = to_snapshot_row(item, scan_date="x", scan_ts="x")
    assert row["mortgage_min_rate"] == 5.0
    assert row["mortgage_best_name"] == "IT-ипотека 5%"


def test_to_snapshot_row_handles_missing_benefits():
    item = dict(FIXTURE)
    item["benefits"] = None
    row = to_snapshot_row(item, scan_date="x", scan_ts="x")
    assert row["mortgage_min_rate"] is None
    assert row["mortgage_best_name"] is None


def test_to_flat_row_handles_studio():
    item = dict(FIXTURE, rooms="studio", is_studio=1)
    row = to_flat_row(item, first_seen="2026-05-15")
    assert row["rooms"] == "studio"
    assert row["is_studio"] == 1


def test_promo_detection_for_known_discounted_flat():
    """Flat 980492 на pik.gorev.space: price=20428980, meterPrice=442866, area=42.9
    дают ~7% скидку (совпадает с benefitDiscount=7 из /v1/flat/980492).
    """
    item = {
        "id": 980492, "guid": "g", "block_id": 1165, "rooms": 1,
        "bulk": {}, "section": {}, "layout": {},
        "price": 20_428_980, "meterPrice": 442_866, "area": 42.9,
        "status": "free",
    }
    row = to_snapshot_row(item, scan_date="2026-05-15", scan_ts="t")
    assert row["promo_price"] == 18_998_951  # 442_866 * 42.9
    assert row["base_meter_price"] == 476_200  # 20_428_980 / 42.9
    assert 6.9 < row["discount_pct"] < 7.1
    assert row["has_promo"] == 1


def test_no_promo_when_prices_match():
    """Flat 979630: price=18472960, meterPrice=524800, area=35.2 — нет скидки."""
    item = {
        "id": 979630, "guid": "g", "block_id": 1165, "rooms": 1,
        "bulk": {}, "section": {}, "layout": {},
        "price": 18_472_960, "meterPrice": 524_800, "area": 35.2,
        "status": "free",
    }
    row = to_snapshot_row(item, scan_date="x", scan_ts="t")
    assert row["has_promo"] == 0
    assert row["discount_pct"] == 0.0
    assert row["promo_price"] is not None


def test_promo_fields_none_when_data_missing():
    item = {
        "id": 1, "guid": "g", "block_id": 1165, "rooms": 1,
        "bulk": {}, "section": {}, "layout": {},
        "price": None, "meterPrice": None, "area": None,
        "status": "free",
    }
    row = to_snapshot_row(item, scan_date="x", scan_ts="t")
    assert row["promo_price"] is None
    assert row["base_meter_price"] is None
    assert row["discount_pct"] is None
    assert row["has_promo"] == 0
