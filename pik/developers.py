"""Реестр застройщиков и неймспейсинг id.

`flats.id` и `blocks.id` — единое INTEGER-пространство на ВСЕ застройщики.
Чтобы native-id с разных сайтов (у каждого свой счётчик) не сталкивались,
каждому застройщику выделен непересекающийся диапазон:

    synthetic_id = offset * ID_NAMESPACE + native_id

ПИК имеет offset 0 — его исторические id остаются как есть, миграция БД
не нужна. Для застройщиков без числового id есть `stable_int_id` —
детерминированный хеш строкового ключа (slug/guid) в тот же диапазон.
"""
from __future__ import annotations

import hashlib

# Размер диапазона id одного застройщика. PIK flat_id ~7 знаков, block_id ~4 —
# 1e12 даёт огромный запас и при 9 застройщиках (9e12) спокойно помещается
# в знаковый 64-битный INTEGER SQLite (предел ~9.2e18).
ID_NAMESPACE = 1_000_000_000_000

PIK = "ПИК"

# Канонические имена застройщиков → offset. Порядок = рейтинг ЕРЗ.РФ.
# ПИК = 0: native id без сдвига, обратная совместимость с существующей БД.
DEVELOPERS: dict[str, int] = {
    PIK:        0,
    "Самолёт":  1,
    "MR Group": 2,
    "Донстрой": 3,
    "ГК ФСК":   4,
    "А101":     5,
    "Level":    6,
    "Абсолют":  7,
    "ЛСР":      8,
    "Гранель":  9,
    "Инград":   10,
    "Брусника": 11,
}


def namespaced_id(developer: str, native_id: int) -> int:
    """native id застройщика → глобально уникальный id для общих таблиц."""
    try:
        offset = DEVELOPERS[developer]
    except KeyError:
        raise ValueError(f"unknown developer: {developer!r}") from None
    if not 0 <= native_id < ID_NAMESPACE:
        raise ValueError(
            f"native_id {native_id} вне диапазона [0, {ID_NAMESPACE})"
        )
    return offset * ID_NAMESPACE + native_id


def split_id(global_id: int) -> tuple[str, int]:
    """Обратное преобразование: global id → (developer, native_id)."""
    offset, native = divmod(global_id, ID_NAMESPACE)
    for dev, off in DEVELOPERS.items():
        if off == offset:
            return dev, native
    raise ValueError(f"id {global_id}: неизвестный offset застройщика {offset}")


def stable_int_id(key: str) -> int:
    """Детерминированный int-id из строкового ключа (slug/guid).

    Для застройщиков без числового id. В отличие от встроенного hash()
    стабилен между запусками процесса. Результат лежит в [0, ID_NAMESPACE).
    """
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return int(digest, 16) % ID_NAMESPACE
