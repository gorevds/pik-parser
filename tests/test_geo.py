from pik.geo import (
    CITY_CENTERS,
    city_from_address,
    derive_city,
    extract_block_meta,
    haversine_km,
    primary_metro,
    validate_city_by_coords,
)


def test_validate_city_keeps_correct_city():
    # Реальный Благовещенск (координаты центра) с city=blagoveshchensk — ок.
    lat, lon = CITY_CENTERS["blagoveshchensk"]
    assert validate_city_by_coords("blagoveshchensk", lat, lon) == "blagoveshchensk"


def test_validate_city_overrides_moscow_amurskaya_to_msk():
    # «ул. Амурская» в Москве → ложный blagoveshchensk; координаты московские.
    assert validate_city_by_coords("blagoveshchensk", 55.805865, 37.753913) == "msk"


def test_validate_city_overrides_spb_mislabeled_msk():
    # Объект на Кантемировской в СПб, ошибочно помеченный msk.
    assert validate_city_by_coords("msk", 59.981838, 30.338658) == "spb"


def test_validate_city_keeps_mo_within_threshold():
    # Дальнее Подмосковье (~120 км от Кремля) остаётся mo, не перебивается.
    assert validate_city_by_coords("mo", 55.7520 - 1.05, 37.6175) == "mo"


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


def test_city_from_address_moscow_vs_mo():
    """Подстрока 'Москва' есть в 'Г.Москва', но НЕ в 'Московская'."""
    assert city_from_address("Россия, г. Москва, ВАО") == "msk"
    assert city_from_address(" Г.Москва, СВАО") == "msk"
    # «Московская область» не должна перехватываться как 'msk'
    assert city_from_address("Московская область, г.о. Котельники") == "mo"
    assert city_from_address("Московская обл., г. Котельники") == "mo"
    assert city_from_address("МО, Ленинский район, пос. Развилка") == "mo"


def test_city_from_address_regions():
    assert city_from_address("Сахалинская область, г. Южно-Сахалинск") == "yuzhno-sakhalinsk"
    assert city_from_address(" Приморский край, г. Владивосток") == "vladivostok"
    assert city_from_address("Свердловская область, г. Екатеринбург") == "ekb"
    assert city_from_address("Республика Татарстан, г. Казань") == "kazan"
    assert city_from_address("Санкт-Петербург, Красногвардейский район") == "spb"
    assert city_from_address("Ярославская область, г. Ярославль") == "yaroslavl"
    assert city_from_address("Хабаровский край, г. Хабаровск") == "khabarovsk"


def test_city_from_address_empty_defaults_to_msk():
    # не-PIK застройщики у нас все Москва, адрес часто пустой
    assert city_from_address(None) == "msk"
    assert city_from_address("") == "msk"


def test_city_from_address_unknown_is_other():
    assert city_from_address("Республика Калмыкия, г. Элиста") == "other"


def test_extract_block_meta_uses_address_not_slug_for_city():
    """Раньше distance_km считался от Кремля для всех slug'ов без '/' — даже
    для сахалинских ЖК давало ~6700 км мусора. Теперь — от центра по адресу."""
    item = {
        "address": "Сахалинская область, г. Южно-Сахалинск, ул. Жириновского",
        "block": {"latitude": 46.9588, "longitude": 142.7388},
        "metroStationsServiceNew": [],
    }
    m = extract_block_meta(item, slug="uyun-park")  # slug без префикса города
    assert m["city"] == "yuzhno-sakhalinsk"
    assert m["distance_km"] == 0.0  # координаты в центре Южно-Сахалинска


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
