#!/usr/bin/env bash
set -euo pipefail

# Запускать на сервере под root. Idempotent (можно перезапускать).
# Предусловия:
#   - DNS A-record pik.gorev.space -> server IP
#   - Установлены nginx, certbot, python3.12, sqlite3, rsync
#   - В рабочей директории есть актуальный клон pik-parser (REPO_DIR=$PWD)

REPO_DIR="${REPO_DIR:-$PWD}"
APP_DIR="/opt/pik"
SVC_USER="pik"

# OnSuccess=/OnFailure= в pik-scan.service требуют systemd >= 249
# (Ubuntu 22.04 LTS / Debian 12 / RHEL 9). На более старых системах
# чейн pik-scan-dev молча не сработает (никакой ошибки в daemon-reload).
# Прерываемся ДО изменений: лучше явный fail, чем тихая регрессия.
SYSTEMD_VER=$(systemctl --version | awk 'NR==1{print $2}')
if [ "${SYSTEMD_VER:-0}" -lt 249 ]; then
  echo "ERROR: systemd $SYSTEMD_VER < 249 — нужен Ubuntu 22.04+/Debian 12+ для OnSuccess= в pik-scan.service" >&2
  exit 1
fi

# 1. Системный пользователь
id -u "$SVC_USER" &>/dev/null || useradd --system --home "$APP_DIR" --shell /usr/sbin/nologin "$SVC_USER"

# 2. Раскладка кода (исключаем рабочие артефакты)
install -d -o "$SVC_USER" -g "$SVC_USER" "$APP_DIR" "$APP_DIR/data" "$APP_DIR/static"
rsync -a --delete \
  --exclude='data/' --exclude='.git/' --exclude='venv/' --exclude='__pycache__/' \
  --exclude='.pytest_cache/' --exclude='*.egg-info' \
  "$REPO_DIR/" "$APP_DIR/"
chown -R "$SVC_USER":"$SVC_USER" "$APP_DIR"

# 3. venv с datasette
sudo -u "$SVC_USER" python3.12 -m venv "$APP_DIR/venv"
sudo -u "$SVC_USER" "$APP_DIR/venv/bin/pip" install --upgrade pip
sudo -u "$SVC_USER" "$APP_DIR/venv/bin/pip" install -e "$APP_DIR[serve]"

# 4. systemd
install -m 644 "$APP_DIR/deploy/pik.service"          /etc/systemd/system/pik.service
install -m 644 "$APP_DIR/deploy/pik-scan.service"     /etc/systemd/system/pik-scan.service
install -m 644 "$APP_DIR/deploy/pik-scan.timer"       /etc/systemd/system/pik-scan.timer
install -m 644 "$APP_DIR/deploy/pik-scan-dev.service" /etc/systemd/system/pik-scan-dev.service
install -m 644 "$APP_DIR/deploy/pik-scan-dev.timer"   /etc/systemd/system/pik-scan-dev.timer
install -m 644 "$APP_DIR/deploy/pik-backup.service"   /etc/systemd/system/pik-backup.service
install -m 644 "$APP_DIR/deploy/pik-backup.timer"     /etc/systemd/system/pik-backup.timer
systemctl daemon-reload

# pik-scan-dev переехал с собственного 06:30-таймера на OnSuccess/OnFailure-
# триггер из pik-scan.service (см. unit-файл). Это гарантирует сериализацию
# writer'ов независимо от длительности скана. На существующих машинах
# старый таймер нужно выключить, иначе он отстреливает второй раз в 06:30.
if systemctl is-enabled --quiet pik-scan-dev.timer 2>/dev/null; then
  systemctl disable --now pik-scan-dev.timer
  echo ">>> pik-scan-dev.timer отключён (теперь чейнится из pik-scan.service)"
fi

# 5. Первый прогон сканов (обновляет схему + наполняет БД).
# pik-scan-dev стартует автоматически через OnSuccess/OnFailure из
# pik-scan.service — повторно запускать его руками не нужно (это удваивало
# квоту обращений к API внешних застройщиков на каждом install).
systemctl start pik-scan.service
journalctl -u pik-scan.service --no-pager | tail -20
# Wait for OnSuccess chain
sleep 2
journalctl -u pik-scan-dev.service --no-pager | tail -20

# Перезапуск Datasette после изменения схемы (он держит соединение с pik.db)
systemctl restart pik.service 2>/dev/null || true

# 6. Поднимаем datasette + ежедневный таймер + бэкап-таймер
systemctl enable --now pik.service
systemctl enable --now pik-scan.timer
systemctl enable --now pik-backup.timer
# pik-scan-dev.timer НЕ enable'им: pik-scan-dev запускается из pik-scan
# через OnSuccess=. Файл таймера оставлен в системе для возможности
# ручного `systemctl start pik-scan-dev.service`.
systemctl status pik.service --no-pager | head -10

# 7. Nginx — двухшаговая раскатка для первичного выпуска TLS-сертификата
ln -sf /etc/nginx/sites-available/pik.gorev.space /etc/nginx/sites-enabled/pik.gorev.space

if [ ! -e /etc/letsencrypt/live/pik.gorev.space/fullchain.pem ]; then
  echo ">>> TLS-сертификат не найден — ставим HTTP-only и зовём certbot."
  install -m 644 "$APP_DIR/deploy/nginx-pik.gorev.space-http.conf" /etc/nginx/sites-available/pik.gorev.space
  mkdir -p /var/www/certbot
  nginx -t
  systemctl reload nginx

  certbot certonly --webroot -w /var/www/certbot -d pik.gorev.space \
    --non-interactive --agree-tos --email "${LETSENCRYPT_EMAIL:-dmitrii@gorev.space}"
fi

install -m 644 "$APP_DIR/deploy/nginx-pik.gorev.space.conf" /etc/nginx/sites-available/pik.gorev.space
nginx -t
systemctl reload nginx
echo ">>> Готово. Откройте https://pik.gorev.space"
