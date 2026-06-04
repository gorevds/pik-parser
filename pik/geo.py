"""Гео-вычисления: расстояние до центра, парсинг метро из PIK API."""
from __future__ import annotations

import math

# Центры городов (для расчёта дистанции через haversine)
CITY_CENTERS: dict[str, tuple[float, float]] = {
    "msk":               (55.7520, 37.6175),   # Кремль
    "mo":                (55.7520, 37.6175),   # МО — считаем от Кремля как опоры
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
    "krasnodar":         (45.0355, 38.9753),
    "tyumen":            (57.1530, 65.5343),
    "obninsk":           (55.0958, 36.6133),
    "kaluga":            (54.5135, 36.2614),
    "ufa":               (54.7388, 55.9721),
    "chelyabinsk":       (55.1644, 61.4368),
    # Добавлено для Брусники (multi-region: ещё 6 городов)
    "nsk":               (55.0084, 82.9357),   # Новосибирск, пл. Ленина
    "surgut":            (61.2540, 73.3958),
    "omsk":              (54.9885, 73.3242),
    "kurgan":            (55.4500, 65.3411),
    "lipetsk":           (52.6088, 39.5994),
    "perm":              (58.0105, 56.2502),
}


# Подстроки в адресе → код города. Регионы идут первыми, чтобы «Московская
# обл.» не перехватывало «Свердловскую область» и т.п. Та же логика мириорится
# SQL-CASE'ом в pik.store._migrate_blocks (backfill для старых строк) и в
# pik/schema.sql косвенно через колонку blocks.city.
_CITY_PATTERNS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("Санкт-Петербург",),                 "spb"),
    (("Татарстан", "Казан"),               "kazan"),
    (("Свердловская", "Екатеринбург"),     "ekb"),
    (("Ярослав",),                         "yaroslavl"),
    (("Сахалин",),                         "yuzhno-sakhalinsk"),
    (("Приморский", "Владивосток"),        "vladivostok"),
    (("Хабаров",),                         "khabarovsk"),
    (("Новороссийск",),                    "novorossiisk"),
    (("Краснодар",),                       "krasnodar"),
    (("Тюмен",),                           "tyumen"),
    (("Обнинск",),                         "obninsk"),
    (("Калуг",),                           "kaluga"),
    (("Нижегород", "Нижний Новгород"),     "nn"),
    (("Башкортостан", "г. Уфа", "г.Уфа"),  "ufa"),
    (("Челяб",),                           "chelyabinsk"),
    (("Улан-Удэ", "Бурят"),                "ulan-ude"),
    (("Благовещен", "Амурская"),           "blagoveshchensk"),
    # МО — после регионов
    (("Московская обл", "Московская область", "МО, ", "МО,"), "mo"),
    # Москва — подстрока 'Москва' (но не 'Московская'!). Идёт последней.
    (("Москва",),                          "msk"),
)


def city_from_address(address: str | None) -> str:
    """Извлечь код города из адреса блока (PIK/FSK API).

    Возвращает ключ из CITY_CENTERS либо 'other'. Пустой адрес → 'msk':
    у не-PIK застройщиков адрес мы не собираем, но их источники сами по себе
    московские (FSK city_id=1, остальные — DRF/GraphQL по московским ЖК).
    """
    if not address:
        return "msk"
    for needles, code in _CITY_PATTERNS:
        if any(n in address for n in needles):
            return code
    return "other"


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


def primary_metro(stations: list) -> dict | None:
    """Вернуть самую близкую станцию по timeOnFoot."""
    if not stations:
        return None
    def fkey(s):
        t = _to_int(s.get("timeOnFoot"))
        return t if t is not None else 99999
    return sorted(stations, key=fkey)[0]


def derive_city(slug: str | None) -> str:
    """LEGACY: slug 'kazan/siberovo' -> 'kazan'; 'narvin' -> 'msk'.

    Историческая логика: PIK теоретически мог отдавать slug с префиксом города,
    но на практике все актуальные slug'и (Нарвин, Саларьево парк, Уюн парк
    и т.д.) — без '/'. Использовать в новом коде НЕ нужно — есть
    `city_from_address`, которая правильно работает для всех ЖК.
    """
    if not slug:
        return "msk"
    if "/" in slug:
        return slug.split("/", 1)[0]
    return "msk"


# Если ЖК дальше этого от центра приписанного города — город определён неверно
# (ложный матч подстроки в адресе). Подмосковье от Кремля ~140 км, край МО
# ~250 → порог 250 безопасен и недостижим для легитимного объекта.
CITY_MISMATCH_KM = 250.0


def validate_city_by_coords(city: str, lat: float, lon: float) -> str:
    """Сверить город с координатами; при грубом расхождении — ближайший центр.

    Возвращает исходный city, если он в справочнике и до его центра
    ≤ CITY_MISMATCH_KM. Иначе — ближайший по координатам известный центр.
    msk↔mo (общий центр-Кремль) не перебиваем: при совпадении центров
    оставляем исходный город.
    """
    if city in CITY_CENTERS:
        if haversine_km(lat, lon, *CITY_CENTERS[city]) <= CITY_MISMATCH_KM:
            return city
    nearest = min(CITY_CENTERS, key=lambda c: haversine_km(lat, lon, *CITY_CENTERS[c]))
    # Если ДАЖЕ ближайший известный центр дальше порога — координаты не
    # принадлежат ни одному охваченному городу (легитимный 'other'-регион,
    # напр. Калмыкия/Элиста). НЕ выдумываем далёкий город: иначе distance_km
    # пересчитается на сотни км и гео-гейт (GEO_MAX_KM) выбросит весь блок.
    if haversine_km(lat, lon, *CITY_CENTERS[nearest]) > CITY_MISMATCH_KM:
        return city
    if city in CITY_CENTERS and CITY_CENTERS[city] == CITY_CENTERS[nearest]:
        return city
    return nearest


def extract_block_meta(data: dict, *, slug: str | None = None) -> dict:
    """Из v2/flat-payload (или flat-item) достать координаты, метро, адрес.

    Принимает либо полный response payload `{block: {…}, flats: […]}`, либо
    единичный flat item (Wayback-формат, где `metroStationsServiceNew` лежит
    в самом flat).
    """
    block = data.get("block") or {}
    flat = data.get("flats", [{}])[0] if isinstance(data.get("flats"), list) else data
    bulk = (
        block.get("bulk")
        or flat.get("bulk")
        or data.get("bulk")
        or {}
    )

    # current v2 API: block.metroStationsService
    # legacy / Wayback HTML: flat.metroStationsServiceNew
    stations = (
        block.get("metroStationsService")
        or data.get("metroStationsServiceNew")
        or flat.get("metroStationsServiceNew")
        or []
    )

    p = primary_metro(stations)
    p_line = (p or {}).get("line") or {}

    lat = (
        _to_float(block.get("latitude"))
        or _to_float(bulk.get("latitude"))
        or _to_float(flat.get("latitude"))
    )
    lon = (
        _to_float(block.get("longitude"))
        or _to_float(bulk.get("longitude"))
        or _to_float(flat.get("longitude"))
    )

    floors_max = _to_int(bulk.get("floors"))
    address = (
        data.get("address")
        or flat.get("address")
        or bulk.get("build_adress")
    )

    # Город — из адреса (надёжно). Slug — fallback для legacy-источников,
    # где адреса в payload нет, но slug несёт префикс города (теоретически).
    city = city_from_address(address)
    if city == "msk" and slug and "/" in slug:
        city = derive_city(slug)

    # Координатная валидация города. Подстрочный матч по адресу иногда даёт
    # ложный город: московская «ул. Амурская» (Гольяново) → blagoveshchensk,
    # СПб-объект на «Кантемировской» → msk. Если координаты есть, а до центра
    # «своего» города > CITY_MISMATCH_KM, берём ближайший известный центр.
    if lat is not None and lon is not None:
        city = validate_city_by_coords(city, lat, lon)

    distance_km = None
    if lat is not None and lon is not None and city in CITY_CENTERS:
        c_lat, c_lon = CITY_CENTERS[city]
        distance_km = round(haversine_km(lat, lon, c_lat, c_lon), 1)

    # Fallbacks: block-level time/metro если структура metroStationsService пуста
    metro_name = (p or {}).get("name") or block.get("metro")
    time_foot = _to_int((p or {}).get("timeOnFoot") or block.get("timeOnFoot"))
    time_transport = _to_int(
        (p or {}).get("timeOnTransport") or block.get("timeOnTransport")
    )

    return {
        "metro_name":         metro_name,
        "metro_line_name":    p_line.get("name"),
        "metro_line_type":    _to_int(p_line.get("type")),
        "metro_time_foot":    time_foot,
        "metro_time_transport": time_transport,
        "latitude":           lat,
        "longitude":          lon,
        "address":            address,
        "city":               city,
        "distance_km":        distance_km,
        "floors_max":         floors_max,
    }
