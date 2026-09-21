#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DC="$SCRIPT_DIR/dc.sh"

if [ ! -f "$SCRIPT_DIR/config.yaml" ]; then
    echo "ERROR: config.yaml not found in $SCRIPT_DIR"
    echo "Please create config.yaml before starting the AP WebHost."
    exit 1
fi

echo "== Starting database =="
"$DC" up -d mariadb
"$DC" up -d vol-setup
echo "Waiting for mariadb..."
until "$DC" exec mariadb healthcheck.sh --connect --innodb_initialized > /dev/null 2>&1; do
    sleep 1
done

echo "== Starting AP WebHost =="
"$DC" up -d ap-webhost
echo "Waiting for AP WebHost..."
until "$DC" exec ap-webhost curl -sf http://127.0.0.1:9888/ > /dev/null 2>&1; do
    sleep 2
done
echo "== AP Webhost is Ready =="

