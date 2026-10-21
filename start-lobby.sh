#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DC="$SCRIPT_DIR/dc.sh"

if [[ ! -f "./Rocket.toml" ]]; then
    echo "== FIRST SETUP =="
    echo
    echo "What public URL will the lobby be reachable at?"
    echo "  - For local dev, just press enter (defaults to http://127.0.0.1:8000)"
    echo "  - For a real deployment, enter the URL your reverse proxy serves,"
    echo "    e.g. https://lobby.example.com  (no trailing slash, no path)"
    read -r _public_url
    _public_url=${_public_url:-http://127.0.0.1:8000}

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

    cat > Rocket.toml <<EOF
[default.oauth.discord]
provider = "Discord"
client_id = "$_client_id"
client_secret = "$_client_secret"
redirect_uri = "${_public_url}/auth/oauth"
admins = [$_admin_id]
EOF

fi

# Stop docker compose getting upset when starting services before we've made it later
touch .env.community

echo "== Starting base services =="
"$DC" up -d postgres valkey
"$DC" up -d vol-setup
echo "Waiting for postgres and valkey..."
"$DC" exec postgres sh -c "until pg_isready -U postgres; do sleep 1; done" > /dev/null 2>&1
"$DC" exec valkey sh -c "until valkey-cli ping | grep -q PONG; do sleep 1; done" > /dev/null 2>&1

# Create APX database if it doesn't exist
"$DC" exec postgres psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'apx'" | grep -q 1 \
    || "$DC" exec postgres psql -U postgres -c "CREATE DATABASE apx"

echo "== Starting lobby =="
"$DC" up -d lobby
echo "Waiting for lobby to be healthy..."
until "$DC" exec lobby curl -sf http://127.0.0.1:8000/health > /dev/null 2>&1; do
    sleep 2
done
echo "Lobby is up."

echo "== Starting workers =="
"$DC" up -d yaml-checker generator option-generator yaml-checker-0.6.8 generator-0.6.8 option-generator-0.6.8

echo "== Lobby is Ready =="
