#!/usr/bin/env bash
# Ежедневный бэкап pik.db через sqlite3 ".backup" (online, безопасно
# с активным writer'ом) + gzip + ротация (14 дней).
#
# Запускается из pik-backup.service (см. deploy/pik-backup.service).
# RPO = 24ч (один бэкап в сутки), RTO ~30с (gunzip + sqlite restore).
#
# До этого скрипта вся БД жила в одном файле без копий: машина потеряна
# = вся история цен (155 дат, 66k снапшотов) утрачена безвозвратно.
set -euo pipefail

DB=${PIK_DB:-/opt/pik/data/pik.db}
DEST_DIR=${PIK_BACKUP_DIR:-/opt/pik/data/backups}
KEEP_DAYS=${PIK_BACKUP_KEEP_DAYS:-14}

mkdir -p "$DEST_DIR"

TODAY=$(date +%Y%m%d)
TARGET="$DEST_DIR/pik-${TODAY}.db"

# sqlite3 ".backup" безопаснее cp — снимает консистентный снэпшот через
# Backup API, не блокируя writer надолго (по 100 страниц за итерацию).
sqlite3 "$DB" ".backup $TARGET"

# gzip даёт ~10x compression для SQLite (много текста, повторов).
gzip -f "$TARGET"

# Ротация: удаляем gz-файлы старше KEEP_DAYS дней. Имя файла включает
# дату — мы это не используем для ротации (полагаемся на mtime), но это
# делает каталог человекочитаемым.
find "$DEST_DIR" -maxdepth 1 -name 'pik-*.db.gz' -type f -mtime "+${KEEP_DAYS}" -delete

echo "backup done: $TARGET.gz ($(du -h "$TARGET.gz" | cut -f1))"
