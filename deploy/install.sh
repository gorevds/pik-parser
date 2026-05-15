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

# 1. Системный пользователь
id -u "$SVC_USER" &>/dev/null || useradd --system --home "$APP_DIR" --shell /usr/sbin/nologin "$SVC_USER"

# 2. Раскладка кода (исключаем рабочие артефакты)
install -d -o "$SVC_USER" -g "$SVC_USER" "$APP_DIR" "$APP_DIR/data"
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
install -m 644 "$APP_DIR/deploy/pik.service"      /etc/systemd/system/pik.service
install -m 644 "$APP_DIR/deploy/pik-scan.service" /etc/systemd/system/pik-scan.service
install -m 644 "$APP_DIR/deploy/pik-scan.timer"   /etc/systemd/system/pik-scan.timer
systemctl daemon-reload

# 5. Первый прогон скана (наполняет БД)
systemctl start pik-scan.service
journalctl -u pik-scan.service --no-pager | tail -20

# 6. Поднимаем datasette + ежедневный таймер
systemctl enable --now pik.service
systemctl enable --now pik-scan.timer
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
    --non-interactive --agree-tos --email "${LETSENCRYPT_EMAIL:-oscar@dolotov.com}"
fi

install -m 644 "$APP_DIR/deploy/nginx-pik.gorev.space.conf" /etc/nginx/sites-available/pik.gorev.space
nginx -t
systemctl reload nginx
echo ">>> Готово. Откройте https://pik.gorev.space"
