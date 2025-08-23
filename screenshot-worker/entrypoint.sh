#!/bin/sh
set -e

if [ -z "$CHANNELS" ] && [ $# -eq 0 ]; then
  echo "Provide channels via CHANNELS env or as args (comma separated)"
  exit 2
fi

if [ -n "$1" ]; then
  ARGS="$@"
else
  ARGS="--channels=$CHANNELS"
fi

mkdir -p "$SCREENSHOT_DIR"

while true; do
  echo "[screenshot-worker] running capture: $(date -u)"
  node /app/capture.js $ARGS || true
  echo "[screenshot-worker] sleeping for ${INTERVAL_SECONDS}s"
  sleep ${INTERVAL_SECONDS}
done
