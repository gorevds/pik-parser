"""Data-quality контроль: предикаты «логически валидной» строки.

Единственный источник правды для порогов. Применяется в `build_rows`
(единое горлышко всех 10 источников) ДО записи в БД: данные, которые
заведомо неверны по логике (нулевая/абсурдная цена, ЖК за тысячи км от
«своего» города), не должны попадать ни в `flats`/`snapshots`, ни в
витрину `today_all`.

Пороги выбраны по реальным данным прода (на 2026-05-30):
- цена: min ненулевая ~4.14 млн ₽, max ~536 млн ₽ → вилка [0.5 млн, 5 млрд]
  имеет огромный запас и ловит только 0/NULL и мусор парсинга (например
  meter_price, ошибочно записанный в price);
- гео: крупнейший охваченный регион — Москва+МО (МО считается от Кремля).
  Наблюдаемый максимум легитимного ЖК — 119.7 км; географический край МО
  от центра ~250 км. Порог 300 км даёт запас над этим и недостижим для
  реальной ошибки (перепутанные lat/lon, не тот город ≈ 1000+ км).
"""
from __future__ import annotations

from dataclasses import dataclass

# Цена квартиры в рублях: всё вне [PRICE_MIN, PRICE_MAX] — логический мусор.
PRICE_MIN = 500_000
PRICE_MAX = 5_000_000_000

# Максимальное расстояние ЖК от центра приписанного города (км).
GEO_MAX_KM = 300.0


@dataclass
class DataQualityStats:
    """Счётчики отбракованных при сборке строк (для лога и scan_runs).

    rejected_price — квартир выброшено по цене;
    rejected_geo   — квартир выброшено из-за гео-невалидного ЖК;
    geo_bad_blocks — самих ЖК выброшено по гео.
    """
    rejected_price: int = 0
    rejected_geo: int = 0
    geo_bad_blocks: int = 0

    @property
    def total_rejected_flats(self) -> int:
        return self.rejected_price + self.rejected_geo


def price_ok(price: int | float | None) -> bool:
    """True, если цену можно записывать.

    Отбраковываются: None, 0/отрицательные и значения вне разумной вилки
    (слишком дёшево — обычно meter_price вместо price; слишком дорого —
    лишний разряд при парсинге).
    """
    if price is None:
        return False
    return PRICE_MIN <= price <= PRICE_MAX


def geo_ok(distance_km: float | None) -> bool:
    """True, если ЖК на допустимом расстоянии от центра города.

    None означает «координат нет, судить не о чем» — НЕ отбраковываем
    (у не-PIK источников без lat/lng distance_km остаётся NULL штатно).
    """
    if distance_km is None:
        return True
    return distance_km <= GEO_MAX_KM
