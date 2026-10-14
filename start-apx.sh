#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DC="$SCRIPT_DIR/dc.sh"

echo "== Ensuring APX database exists =="
"$DC" up -d postgres
"$DC" exec postgres sh -c "until pg_isready -U postgres; do sleep 1; done" > /dev/null 2>&1
"$DC" exec postgres psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'apx'" | grep -q 1 \
  || "$DC" exec postgres psql -U postgres -c "CREATE DATABASE apx"
echo "== Starting APX =="
"$DC" up -d apx
echo "== APX is Ready =="
