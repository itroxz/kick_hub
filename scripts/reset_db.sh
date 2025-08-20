#!/bin/sh
# Simple helper to backup and remove SQLite DB files in /data volume inside the monitor container.
# Usage: ./scripts/reset_db.sh
set -e
CONTAINER=${1:-fds_monitor}
BACKUP_DIR=${2:-./backups}
mkdir -p "$BACKUP_DIR"
TIMESTAMP=$(date +%Y%m%d%H%M%S)
echo "Backing up /data/kick_monitor.sqlite3 and /data/fds_bot.db from $CONTAINER to $BACKUP_DIR"
docker cp "$CONTAINER":/data/kick_monitor.sqlite3 "$BACKUP_DIR/kick_monitor.sqlite3.$TIMESTAMP" || true
docker cp "$CONTAINER":/data/fds_bot.db "$BACKUP_DIR/fds_bot.db.$TIMESTAMP" || true

echo "Removing DB files from container $CONTAINER:/data"
docker exec -it "$CONTAINER" sh -c "rm -f /data/kick_monitor.sqlite3 /data/fds_bot.db"

echo "Done. Restarting container $CONTAINER"
docker restart "$CONTAINER"

echo "Reset complete. Backups are in $BACKUP_DIR"
