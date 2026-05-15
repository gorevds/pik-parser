# pik-narvin-parser

Daily snapshots of 1-bedroom flat prices in PIK «Нарвин» residential complex.

Public dashboard: **https://pik.gorev.space**

## Что внутри

- `pik/` — Python-пакет: HTTP-клиент к `api.pik.ru`, маппинг JSON → SQLite, upsert
- `bin/scan.py` — однопроходный сканер (одна транзакция, идемпотентен в рамках дня)
- `metadata.yml` — конфиг Datasette (заголовок, описания, history-query)
- `deploy/` — systemd units + nginx config + install.sh

## Локально

```bash
python3.12 -m venv venv
. venv/bin/activate
pip install -e .[serve,test]
pytest
python -m bin.scan --db data/pik.db
datasette serve data/pik.db -m metadata.yml --port 5051
```

Тогда `http://127.0.0.1:5051/pik/today_one_room` — сегодняшняя витрина.

## На сервере

DNS A-запись `pik.gorev.space → <server IP>` должна быть. Дальше:

```bash
sudo REPO_DIR=$PWD bash deploy/install.sh
sudo certbot --nginx -d pik.gorev.space
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
GET https://api.pik.ru/v2/flat?block_id=1165&types=1&page=N
```

Нарвин = `block_id=1165`, ~281 квартира, 6 страниц по 50. Требует браузерный
User-Agent (его выставляет `PikClient`).

## Схема БД

- `flats` — стабильные характеристики (этаж, площадь, корпус, секция, метро)
- `snapshots` — ежедневный срез (цена, статус, отделка, лучшая ипотека)
- `today_one_room` — view: сегодня + только 1-комн., с русскими названиями колонок

## Спецификация и план

- `docs/superpowers/specs/2026-05-15-pik-narvin-parser-design.md`
- `docs/superpowers/plans/2026-05-15-pik-narvin-parser-plan.md`
