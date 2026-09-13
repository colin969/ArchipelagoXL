#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DC="$SCRIPT_DIR/dc.sh"

echo "== Starting Analytics =="
"$DC" up -d prometheus
"$DC" up -d grafana
"$DC" up -d loki
echo "== Analytics is Ready =="
