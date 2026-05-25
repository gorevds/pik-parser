"""Источник данных Брусника (brusnika.ru).

Multi-region застройщик: 11 региональных поддоменов
(moskva, spb, tyumen, ekaterinburg, sibakademstroy, surgut, omsk, kurgan,
lipetsk, perm, chelyabinsk) — у каждого свой `/api/filter/flats/` и
`/api/projects/`. Идём последовательно: сбой одного региона не валит
остальные.

Особенности маппинга:
— Все цены приходят строкой (Decimal-as-string).
— `finish_type` отдельного поля нет: отделка лежит в `tags[]` как один
  из элементов («Предчистовая отделка», «Без отделки», «С отделкой»,
  «White Box», «Чистовая отделка»).
— Поддомен сразу же даёт нам city для post-hoc determination.
"""
from __future__ import annotations

import logging

import requests

from pik.sources.base import (
    CollectResult,
    NormBlock,
    NormFlat,
    make_session,
    request_json,
)

DEVELOPER = "Брусника"

# Поддомен → код города из pik.geo.CITY_CENTERS (нужен чтобы build_rows
# правильно посчитал distance_km — иначе non-MSK залипнут как 'msk').
_REGIONS: dict[str, str] = {
    "moskva":           "msk",
    "spb":              "spb",
    "tyumen":           "tyumen",
    "ekaterinburg":     "ekb",
    "sibakademstroy":   "nsk",        # Новосибирск — в CITY_CENTERS нет, см. ниже
    "surgut":           "surgut",
    "omsk":             "omsk",
    "kurgan":           "kurgan",
    "lipetsk":          "lipetsk",
    "perm":             "perm",
    "chelyabinsk":      "chelyabinsk",
}

_PAGE_LIMIT = 500
_MAX_PAGES = 30

# Маркеры отделки в tags[] (мап на канонические значения view-фильтра).
# ВАЖЕН ПОРЯДОК: «предчистовая» ДО «чистовая отделка», иначе вторая поймает
# первое как substring; «с отделкой и мебелью» ДО «с отделкой».
_FINISH_TAGS = [
    ("с отделкой и мебелью", "С отделкой и мебелью"),
    ("предчистовая",         "Предчистовая отделка"),
    ("чистовая отделка",     "С отделкой"),
    ("с отделкой",           "С отделкой"),
    ("white box",            "WhiteBox"),
    ("whitebox",             "WhiteBox"),
    ("без отделки",          "Без отделки"),
]

log = logging.getLogger("pik.sources.brusnika")


def _to_int(v) -> int | None:
    try: return int(v)
    except (TypeError, ValueError): return None


def _to_float(v) -> float | None:
    if v in (None, "", "None"): return None
    try: return float(v)
    except (TypeError, ValueError): return None


def _money(v) -> int | None:
    f = _to_float(v)
    return round(f) if f is not None else None


def _finish_from_tags(tags) -> str | None:
    if not isinstance(tags, list): return None
    lower = " | ".join(str(t).lower() for t in tags)
    for needle, label in _FINISH_TAGS:
        if needle in lower:
            return label
    return None


def _to_norm(fl: dict) -> NormFlat:
    price = _money(fl.get("price_marketing")) or _money(fl.get("price"))
    base = _money(fl.get("price_old")) or _money(fl.get("price"))
    old_price = base if (price and base and base > price) else None
    rooms = _to_int(fl.get("rooms"))
    return NormFlat(
        native_id=fl.get("flat_id") or fl.get("pk"),
        native_block_id=str(fl.get("complex") or ""),
        rooms=rooms,
        area=_to_float(fl.get("square")),
        floor=_to_int(fl.get("floor")),
        price=price,
        old_price=old_price,
        # is_booked=True значит «забронировано», но всё ещё в выдаче — статус
        # «забронировано» юзеру важно увидеть отдельно от свободных.
        status="reserved" if fl.get("is_booked") else "free",
        bulk_name=fl.get("building_name"),
        section_no=_to_int(fl.get("section_number")),
        # «Срок сдачи: 3 квартал 2026» → удалить префикс, оставить «3 квартал 2026»
        settlement_date=_clean_settlement(fl.get("delivery_title")
                                          or fl.get("completion_date")),
        url=fl.get("page_url"),
        finish=_finish_from_tags(fl.get("tags")),
        number=str(fl["flat_id"]) if fl.get("flat_id") else None,
        plan_url=fl.get("main_flat_image"),
    )


def _clean_settlement(raw) -> str | None:
    if not raw: return None
    s = str(raw)
    # «Срок сдачи: 3 квартал 2026» → «3 квартал 2026»
    if s.lower().startswith("срок сдачи"):
        _, _, tail = s.partition(":")
        return tail.strip() or None
    return s


def _fetch_projects(session: requests.Session, region: str) -> dict[str, dict]:
    """{complex_id_str → meta} из /api/projects/ конкретного региона."""
    out: dict[str, dict] = {}
    try:
        payload = request_json(session, "GET",
                               f"https://{region}.brusnika.ru/api/projects/")
    except Exception as exc:  # noqa: BLE001
        log.warning("Брусника [%s]: проекты не получены: %s", region, exc)
        return out
    items = payload if isinstance(payload, list) else (payload.get("results") or [])
    for p in items:
        pid = str(p.get("id") or "")
        if not pid: continue
        meta: dict = {}
        lat, lng = p.get("latitude"), p.get("longitude")
        if lat is not None and lng is not None:
            try:
                meta["latitude"], meta["longitude"] = float(lat), float(lng)
            except (ValueError, TypeError):
                pass
        # API /api/projects/ адрес НЕ отдаёт (только description/intro,
        # координаты и метро). Колонка «адрес» в today_all для Брусники
        # будет NULL — это OK, city и distance_km заполнены по lat/lng.
        subway = p.get("subway") or []
        # Берём первое метро (primary). Названия кириллицей у Брусники.
        if subway and isinstance(subway[0], dict):
            mn = subway[0].get("name")
            if mn: meta["metro_name"] = mn
        out[pid] = meta
    return out


def _collect_region(session: requests.Session, region: str, city_code: str) -> CollectResult:
    """Один регион → CollectResult. Slug блока префиксим регионом, чтобы
    Метроном (Москва) и Метроном (Тюмень) не схлопывались в один."""
    norm_flats: list[NormFlat] = []
    blocks: dict[str, str] = {}              # prefixed_slug → name
    base_url = f"https://{region}.brusnika.ru/api/filter/flats/"

    page = 0   # offset
    for _ in range(_MAX_PAGES):
        payload = request_json(
            session, "GET", base_url,
            params={"limit": _PAGE_LIMIT, "offset": page * _PAGE_LIMIT},
        )
        items = payload.get("results") or []
        if not items:
            break
        for fl in items:
            nf = _to_norm(fl)
            # Битая цена (нет ни price_marketing, ни price) — снапшот без
            # цены бесполезен для аналитики, пропускаем. Также проверяем id.
            if not nf.native_id or not nf.native_block_id or not nf.price:
                continue
            # Префиксим И slug блока, И flat_id регионом. Иначе при пересечении
            # числовых пространств id (Москва flat_id=100 и Тюмень flat_id=100)
            # to_global_id даёт одинаковый namespaced_id — вторая квартира
            # затирает первую как dup в build_rows. Префикс гарантирует, что
            # stable_int_id хеширует "moskva:100" и "tyumen:100" в разные числа.
            pref_block = f"{region}:{nf.native_block_id}"
            pref_flat = f"{region}:{nf.native_id}"
            blocks.setdefault(pref_block,
                              fl.get("complex_name") or pref_block)
            # Подменяем оба native id на префиксированные
            norm_flats.append(NormFlat(
                **{**nf.__dict__,
                   "native_id": pref_flat,
                   "native_block_id": pref_block}
            ))
        if len(items) < _PAGE_LIMIT:
            break
        page += 1
    else:
        log.warning("Брусника [%s]: достигнут предел %d страниц", region, _MAX_PAGES)

    proj_meta = _fetch_projects(session, region)
    norm_blocks = []
    for pref_block, name in blocks.items():
        complex_id = pref_block.split(":", 1)[1]
        meta = dict(proj_meta.get(complex_id, {}))
        meta["city"] = city_code  # принудительно: поддомен знает city
        norm_blocks.append(NormBlock(native_id=pref_block, name=name,
                                     slug=pref_block, meta=meta))
    log.info("Брусника [%s]: %d ЖК, %d квартир", region, len(norm_blocks), len(norm_flats))
    return CollectResult(blocks=norm_blocks, flats=norm_flats)


def collect(*, session: requests.Session | None = None) -> CollectResult:
    """Обходит все 11 регионов; сбой одного не валит остальные."""
    s = session or make_session()
    all_blocks: list[NormBlock] = []
    all_flats: list[NormFlat] = []
    for region, city in _REGIONS.items():
        try:
            r = _collect_region(s, region, city)
        except Exception as exc:  # noqa: BLE001 — per-region изоляция
            # Любой сбой (SourceError, RequestException, неожиданный
            # формат payload → AttributeError/KeyError) — пропускаем
            # регион, остальные продолжают.
            log.warning("Брусника [%s] — регион пропущен: %s", region, exc)
            continue
        all_blocks.extend(r.blocks)
        all_flats.extend(r.flats)
    log.info("Брусника: всего %d ЖК, %d квартир по %d регионам",
             len(all_blocks), len(all_flats), len(_REGIONS))
    return CollectResult(blocks=all_blocks, flats=all_flats)
