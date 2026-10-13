#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DC="$SCRIPT_DIR/dc.sh"

echo "== Ensuring APX database exists =="
"$DC" exec postgres psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'apx'" | grep -q 1 \
  || "$DC" exec postgres psql -U postgres -c "CREATE DATABASE apx"
echo "== Starting APX =="
"$DC" up -d apx
echo "== APX is Ready =="
