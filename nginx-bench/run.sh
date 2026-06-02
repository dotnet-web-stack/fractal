#!/usr/bin/env bash
# Launch (or stop) the optimized benchmark nginx, rooted entirely in this directory.
# Unprivileged: no system service is touched, nothing needs root, it listens on :8081.
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-start}" in
    start) exec nginx -p "$PWD/" -c nginx.conf -g 'daemon off;' ;;
    stop)  nginx -p "$PWD/" -c nginx.conf -s quit ;;
    test)  nginx -p "$PWD/" -c nginx.conf -t ;;            # validate the config
    *)     echo "usage: $0 [start|stop|test]"; exit 1 ;;
esac
