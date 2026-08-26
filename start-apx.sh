#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DC="$SCRIPT_DIR/dc.sh"

if [[ ! -s ".env.community" ]]; then
    echo ""
    echo "== Community setup =="
    echo "1. Go to http://localhost:8000 and create a room in the lobby"
    echo "2. Upload YAMLs and generate the game"
    echo "3. Go to http://localhost:9888 and host the generated game"
    echo ""
    echo "Paste the lobby room ID:"
    read -r LOBBY_ROOM_ID
    echo "Paste the AP room ID:"
    read -r AP_ROOM_ID

    cat > .env.community <<EOF
LOBBY_ROOM_ID=$LOBBY_ROOM_ID
AP_ROOM_ID=$AP_ROOM_ID
EOF
    echo "Saved to .env.community"
fi

set -a
. ./.env.community
set +a

echo "== Starting APX =="
"$DC" up -d apx
echo "== APX is Ready =="
