#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DC="$SCRIPT_DIR/dc.sh"

if [[ ! -f "./Rocket.community.toml" ]]; then
    echo "== FIRST SETUP =="
    echo
    echo "What public URL will the lobby be reachable at?"
    echo "  - For local dev, just press enter (defaults to http://127.0.0.1:8010)"
    echo "  - For a real deployment, enter the URL your reverse proxy serves,"
    echo "    e.g. https://ap-tools.example.com  (no trailing slash, no path)"
    read -r _public_url
    _public_url=${_public_url:-http://127.0.0.1:8010}

    echo
    echo "Go to https://discord.com/developers/applications"
    echo "Create an application (or select an existing one)"
    echo "In oauth2 add a redirect to \"${_public_url}/auth/oauth\""
    echo "  (this MUST match the redirect_uri written to Rocket.toml exactly)"
    echo "Click on \"save changes\" then \"Reset Secret\""
    echo

    echo "Paste the client ID:"
    read -r _client_id
    echo "Paste the client secret:"
    read -r _client_secret
    echo "Paste your (fully numeric) discord user ID:"
    read -r _admin_id

    cat > Rocket.community.toml <<EOF
[default.oauth.discord]
provider = "Discord"
client_id = "$_client_id"
client_secret = "$_client_secret"
redirect_uri = "${_public_url}/auth/oauth"
admins = [$_admin_id]
EOF

fi

echo "== Starting Community AP Tools =="
"$DC" up -d community-ap-tools
echo "== Community AP Tools are Ready =="