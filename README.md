# pik-parser

Ежедневный снимок цен и параметров квартир любого ЖК девелопера ПИК → SQLite
→ публичная фильтруемая витрина через Datasette.

По умолчанию настроен на **ЖК «Нарвин»** (`block_id=1165`), но это просто
default. Любой другой проект ПИК поднимается одной командой.

Живая витрина для Нарвина: **https://pik.gorev.space**

## Что внутри

- `pik/client.py` — пагинированный клиент к `api.pik.ru/v2/flat` с retry на 502/503/504
- `pik/mapping.py` — нормализует JSON → строки SQLite (`flats` + `snapshots`)
- `pik/store.py` — схема и идемпотентный upsert в одной транзакции
- `pik/backfill_wayback.py` — ретро-история из архивов pik.ru через web.archive.org
- `bin/scan.py` — однопроходный сканер (cron-friendly)
- `bin/backfill.py` — одноразовая заливка истории
- `metadata.yml` — конфиг Datasette
- `deploy/` — systemd unit + nginx + install.sh

## Локальный quick start

```bash
python3.12 -m venv venv
. venv/bin/activate
pip install -e .[serve,test]
pytest                                # 25 тестов
python -m bin.scan --db data/pik.db   # ~1 минута, реальный API
datasette serve data/pik.db -m metadata.yml --port 5051
```

`http://127.0.0.1:5051/pik/today_one_room` — сегодняшняя витрина (1-комн).

### Ретро-история из Wayback Machine

```bash
python -m bin.backfill --db data/pik.db --slug narvin --block-id 1165
```

Для Нарвина даёт ~9 исторических срезов с июня 2025 (~160 уникальных квартир,
рост цен 1к ~+28% Jun 2025 → Jan 2026).

## Другой ЖК

Нужны только две вещи: **PIK block_id** и **URL slug** проекта.

Найти их можно через один любой `flat/{id}` любого выбранного дома:

```bash
curl -A "Mozilla/5.0" "https://api.pik.ru/v1/flat/<flat-id>" | jq '{block_id, url}'
```

`block_id` подставляется в `--block-id`, slug из URL (`https://www.pik.ru/<slug>`)
— в `--slug`. Тогда:

```bash
python -m bin.scan --db data/pik.db --block-id <BID>
python -m bin.backfill --db data/pik.db --slug <slug> --block-id <BID>
```

Или сразу несколько проектов в одном прогоне:

```bash
PIK_BLOCK_ID=1165 python -m bin.scan --block-id 1165,5686
```

БД содержит `flats.block_id` — все ЖК живут в одной таблице.

## На сервере

DNS A-запись для домена → IP сервера должна быть. Дальше:

```bash
sudo REPO_DIR=$PWD bash deploy/install.sh
sudo certbot --nginx -d <domain>
sudo nginx -t && sudo systemctl reload nginx
```

Скан ежедневно в 06:00 МСК через `pik-scan.timer`. Состояние:

```bash
systemctl status pik.service pik-scan.timer
systemctl list-timers pik-scan.timer
journalctl -u pik-scan.service -n 50
```

## Источник данных

```
GET https://api.pik.ru/v2/flat?block_id=<BID>&types=1&page=N
```

50 элементов на страницу, требует браузерный User-Agent (его выставляет
`PikClient`). Сайт `pik.ru` под QRATOR — публичный сайт не парсим, JSON-API
этого не требует.

## Схема БД

- `flats` — стабильные характеристики (этаж, площадь, корпус, секция); один ряд на квартиру
- `snapshots` — ежедневный срез (цена, статус, отделка, лучшая ипотека); PK = (flat_id, scan_date)
- `today_one_room` — view: последний срез × `rooms='1'`, с русскими колонками
- Все ЖК в одной БД, фильтруйте по `flats.block_id`

## Документация решения

- `docs/superpowers/specs/2026-05-15-pik-narvin-parser-design.md`
- `docs/superpowers/plans/2026-05-15-pik-narvin-parser-plan.md`

## Лицензия

Apache-2.0 (см. `LICENSE`).
