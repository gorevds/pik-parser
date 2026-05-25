"""Источник данных PIK (api.pik.ru).

До 2026-05-25 PIK-парсинг жил в `bin/scan.py` (orchestration) + `pik/mapping.py`
(per-flat маппинг). Этот модуль приводит PIK к тому же контракту, что другие
9 застройщиков: `collect(*, block_ids, …) -> CollectResult`. Архитектурно
PIK теперь — peer-source, не специальный случай. См. docs/refactor-de-pik-plan.md.

Особенность PIK: список block_id не известен заранее (REST не даёт directory),
а собирается из ранее сосканированных блоков (таблица `blocks` где
`developer='ПИК'`). Поэтому signature принимает явный `block_ids`. Для
обычного daily-scan сценария вызывающий код (bin/scan.py / scan_dev) сам
их пред-резолвит из БД и передаёт сюда.

Параллелизм: внутри collect() ThreadPoolExecutor параллельно фетчит
блоки (68 ЖК / 6 workers ≈ 2 мин вместо 30 мин последовательно). PikClient
per-worker — он создаёт свою requests.Session.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from pik.client import PikApiError, PikClient
from pik.geo import extract_block_meta
from pik.mapping import _best_mortgage, _detect_promo, _finish_label
from pik.sources.base import CollectResult, NormBlock, NormFlat

DEVELOPER = "ПИК"
DEFAULT_WORKERS = 6


log = logging.getLogger("pik.sources.pik")


def _norm_flat(item: dict) -> NormFlat:
    """PIK API item → NormFlat. Cохраняем всю промо-семантику оригинального
    `to_snapshot_row` (см. pik/mapping.py): meter_price = ставка с программой,
    price = нал. promo_price = round(meter_price * area).
    """
    bulk = item.get("bulk") if isinstance(item.get("bulk"), dict) else {}
    section = item.get("section") if isinstance(item.get("section"), dict) else {}
    layout = item.get("layout") if isinstance(item.get("layout"), dict) else {}

    rate, mort_name = _best_mortgage(item)
    price = item.get("price")
    meter_price = item.get("meterPrice")
    area = item.get("area")
    promo_price, _base_meter, discount_pct, _has_promo = _detect_promo(
        price, meter_price, area
    )

    rooms_raw = item.get("rooms")
    # PIK API: rooms 0 — студия. Сохраняем int.
    if rooms_raw is None:
        rooms = None
    elif isinstance(rooms_raw, int):
        rooms = rooms_raw
    else:
        try:
            rooms = int(rooms_raw)
        except (TypeError, ValueError):
            rooms = None

    # Эвристика «апартаменты» на уровне квартиры (build_rows добавит блочную)
    is_apart_flat = any(
        "апарт" in (s or "").lower()
        for s in (bulk.get("name"), item.get("name"),
                  (item.get("block") or {}).get("name"))
    )

    return NormFlat(
        native_id=item["id"],
        native_block_id=item["block_id"],
        rooms=rooms,
        area=area,
        floor=item.get("floor"),
        price=price,
        meter_price=meter_price,
        old_price=item.get("oldPrice"),
        status=item.get("status"),
        bulk_name=bulk.get("name"),
        section_no=section.get("number"),
        settlement_date=item.get("settlementDate") or bulk.get("settlement_date"),
        url=item.get("url"),
        finish=_finish_label(item.get("finish")),
        number=item.get("number"),
        plan_url=layout.get("flat_plan_svg") or layout.get("flat_plan_render"),
        is_apartment=is_apart_flat,
        ceiling_height=item.get("ceilingHeight"),
        area_kitchen=item.get("areaKitchen"),
        area_living=item.get("areaLiving"),
        mortgage_min_rate=rate,
        mortgage_best_name=mort_name,
        pdf_url=item.get("pdf"),
        bulk_id=item.get("bulk_id"),
        section_id=item.get("section_id"),
        layout_id=item.get("layout_id"),
        updated_at=item.get("updatedAt"),
        promo_price=promo_price,
        discount_pct=discount_pct,
    )


def _norm_block(items: list[dict], block_id: int) -> NormBlock | None:
    """Имя/slug/гео-meta берём из первого item (у всех квартир одного ЖК
    block-секция одинаковая). Если в ЖК нет квартир, возвращаем None —
    block-payload собирать не из чего."""
    if not items:
        return None
    b = items[0].get("block")
    if not isinstance(b, dict):
        return None
    name = b.get("name") or f"ЖК {block_id}"
    slug = (b.get("url") or "").strip("/") or None
    meta = extract_block_meta(items[0], slug=slug)
    # v2 API убрал bulk.floors из payload, делаем нижнюю оценку через max(floor)
    if not meta.get("floors_max"):
        floors = []
        for it in items:
            try:
                floors.append(int(it.get("floor")))
            except (TypeError, ValueError):
                pass
        if floors:
            meta["floors_max"] = max(floors)
    return NormBlock(native_id=block_id, name=name, slug=slug, meta=meta)


def _fetch_one(block_id: int, types: tuple[int, ...]) -> tuple[int, list[dict]]:
    """Скачать все квартиры одного блока в собственной сессии (thread-safe).
    Возвращает (block_id, items) — block_id чтобы маппить future→исходник."""
    client = PikClient()
    return block_id, client.fetch_block_flats(block_id=block_id, types=types)


def collect(
    *,
    block_ids: Iterable[int],
    types: tuple[int, ...] = (1,),
    workers: int = DEFAULT_WORKERS,
    session: requests.Session | None = None,  # noqa: ARG001 — для совместимости
) -> CollectResult:
    """Скачать перечень PIK-блоков и собрать `CollectResult`.

    `types=(1,)` — только 1-комнатные и студии (исторически витрина была
    про инвест-аналитику Нарвина). Расширение до многокомнатных — отдельная
    задача (нужно поднять max_returned_rows Datasette и подумать про UI).

    `block_ids` обязателен: PIK API не отдаёт directory, у нас нет способа
    «обойти всех». Источник этих id — таблица `blocks` или явный список
    в env/argv.
    """
    bids = list(block_ids)
    n_workers = max(1, min(workers, len(bids)))
    blocks: list[NormBlock] = []
    flats: list[NormFlat] = []
    failed: list[int] = []

    if not bids:
        return CollectResult(blocks=[], flats=[])

    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = {ex.submit(_fetch_one, bid, types): bid for bid in bids}
        for fut in as_completed(futures):
            bid = futures[fut]
            try:
                _, items = fut.result()
            except PikApiError as exc:
                log.error("PIK block %d: %s", bid, exc)
                failed.append(bid)
                continue
            nb = _norm_block(items, bid)
            if nb is not None:
                blocks.append(nb)
            for it in items:
                flats.append(_norm_flat(it))

    log.info("ПИК: %d блок(ов), %d квартир%s",
             len(blocks), len(flats),
             f", {len(failed)} failed: {failed}" if failed else "")
    return CollectResult(blocks=blocks, flats=flats)
