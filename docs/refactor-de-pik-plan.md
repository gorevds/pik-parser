# Де-PIK рефактор — план

## Цель
Превратить «PIK с прицепом из 9 других застройщиков» в «10+ равноправных
застройщиков, один из которых — ПИК». Сохранить domain (`pik.gorev.space`)
и filename БД (`pik.db`) — переименование инфры — отдельный спринт.

## Принципы
- Никаких изменений в id-пространстве существующих PIK-данных (offset 0,
  native_id = raw int). Иначе вся история PIK ушла бы в orphan.
- Никаких изменений в URL-маршрутах Datasette (`/pik/...`) — фронт стоит
  на этих путях.
- Систем-юниты `pik-*.service` и каталог `/opt/pik/` остаются — это
  деплоймент-имена, отдельный сюжет.

## Фазы

### R-A. PIK становится source-модулем (`pik/sources/pik.py`)
**Что**: PIK-API парсинг переезжает из `bin/scan.py`+`pik/mapping.py` в
`pik/sources/pik.py:collect()`, возвращающий `CollectResult` как все
остальные. `NormFlat` расширяется опциональными полями
(ceiling_height, area_kitchen, area_living, mortgage_min_rate,
mortgage_best_name, pdf_url) — переиспользуем структуру вместо raw
flat_row/snap_row dicts.

**Почему**: единый контракт для всех источников. Сейчас `build_rows` —
для не-PIK, а `to_flat_row`/`to_snapshot_row` — для PIK. Дрейф между
двумя путями породил уже несколько багов (промо-detect — `_detect_promo`
vs `_detect_discount`).

### R-B. Унификация сканера
**Что**: `bin/scan_dev.py` принимает `--developer "ПИК"` (или любое
другое имя из реестра). Для PIK резолвит блок-id'ы из БД (как сейчас
делает `bin/scan.py --all-blocks`). `bin/scan.py` становится тонким
deprecation-шимом, который зовёт `scan_dev` с правильными аргументами.

**Почему**: одна команда — один контракт. Меньше шансов рассинхрона.

### R-C. Снос PIK-defaults
**Что**: `blocks_meta.upsert_block_meta` теряет `developer="ПИК"` по
умолчанию — параметр становится обязательным. View `today_all` теряет
`COALESCE(developer, 'ПИК')` (после миграции старые блоки имеют
явный developer). Миграция блоков с NULL developer одноразовая.

**Почему**: дефолт «'ПИК' если не сказано» делал любую опечатку в новом
источнике невидимой — квартиры тихо приписывались к ПИК.

### R-D. Тесты + ревью + деплой

## Не делаем в этом рефакторе
- Переименование Python-пакета `pik/` → `realty/` — большой mass-find-replace
  с риском пропустить динамические импорты. Отложено до domain-rename спринта.
- Переименование БД-файла `pik.db` → нейтральное — связано с Datasette URL и
  фронтендом. Domain-rename вместе.
- Переименование systemd unit'ов и `/opt/pik/` пути — деплоймент-имена,
  меняются вместе с инфрой.

## Совместимость
- Старые PIK-блоки в `blocks` уже имеют `developer='ПИК'` (миграция отработала).
- Старый `bin/scan.py --all-blocks` продолжит работать (станет shim →
  scan_dev), systemd unit `pik-scan.service` не меняется.
- `pik-scan-dev.service` (через `OnSuccess` от pik-scan) тоже не меняется.
- Внешний контракт: `https://pik.gorev.space/pik/today_all.json` — без изменений.
