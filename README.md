Archipelago XL
=================

A complete software package for hosting extra large Archipelago games.

This includes forks of:
- Ionium Lobby, for managing yaml submissions
- Archipelago Webhost, for the base hosting of games
- APX, a proxy for the Webhost to provide extra optimizations and extended functionality like per-slot passwords
- Community AP Tool, for collaborative tools to review yaml submissions, and manage live games

# Running this project

## Setup
Clone with submodules
`git clone --recurse-submodules`

Make a copy of the docker compose template, and edit the environmental variables for keys / security, then build the docker images. Building may take a while.
Please see [Required Secrets](#required-secrets)
```
cp docker-compose.yml.example docker-compose.yml
touch .env.community
docker compose build
```

The start scripts below are produced for easy setup. You should ideally be more familair with how to stop and start services individually with docker compose.

**It is heavily recommended to restrict how many ports are exposed. In production you should only really need the lobby, community ap tools, and apx websocket servers exposed publicly. You are responsible for properly isolating services.**

## Lobby

```
./start-lobby.sh
```

The first start will require you to create a Discord application for OAuth2 (see [Discord OAuth](#discord-oauth) below); `start-lobby.sh` will prompt you and write `Rocket.toml` for you.
The first start will also download all apworlds in the index, which might take a while.

## Archipelago Webhost

```
./start-webhost.sh
```

Only required for running community syncs. This fork of Archipelago has some improvements to performance and stability, and reduces tracker cache time from 60 seconds to 30 seconds.

This runs on Port 9888 by default.

## Community AP Tools

```
./start-community-ap-tools.sh
```

Lobby expected to the running. Only required for running community syncs. It's rocket config will be at `./Rocket.community.toml` which by default is identical to the lobby configuration. 

This can run without the webhost/apx being active, but the dashboard will return a 500.

Dashboard - `/`

Review Tools = `/admin/rooms`

You must set up a Team at `/admin/teams` for the discord server, tie to a lobby id, and then any approved users can go to the review tools page and see the room and edit and approve the yamls.

## Community Sync Guide

### Collecting yamls
- Start the lobby container, collect yamls in a lobby room
- Start the community ap tools, create a team, register your helpers, and then have them review the yamls
- Close the lobby room

### Hosting the game
- Download the yamls from the lobby, generate a seed
- Upload the gen to the lobby with `python3 ./upload-room-gen.py <lobby_id> <genned_zip>`, where lobby id is the one used to collect yamls. This will match all patches to the correct slots.
- Start the webhost, upload the gen, create a webhost room
- Edit `.env.community` to reflect the new room id and port
- Start the apx container. Users will not be able to connect yet.
- Restart the community ap tools container to make the dashboard available at `/`
- Press Generate Passwords to create passwords to every slot. This will now allow people to join.
- Run `./add-room-info.sh <lobby_id> <host> <port>` where lobby id is the one used to collect yamls, and the host / port is that of the APX websocket server. You will manually have to tell users what port the reduced traffic version of APX is running on for now.
- Users will now see the password and download links for any patch files on the lobby page, if they own the slot and are authed to it

## Managing the Sync

The dashboard at `/` for the community ap tools has a few handy things to know
- You can right click a user row to get a list of actions
- You can block which users are allowed to send deathlinks, and see a count of how many they've sent. (This includes ones the server blocks)
- Reseting a slot password will disconnect anyone connected to that slot
  - If you're doing this to change ownership, **change the slot owner first**

## APX (Archipelago Expanded)

```
./start-apx.sh
```

Lobby and Webhost expected to the running. Only required for running community syncs. The container will only run successfuly once the webhost is live with a room created. First run will create a `.env.community` file with the lobby id, ap room port and ap room id. This must be changed manually for subsequent syncs. 

# Required secrets

Generate strong random values for each row. `openssl rand -hex 32` or `openssl rand -base64 32` are both fine. Use a fresh value per row unless **Notes** says to share.

| Variable | Where (in `docker-compose.yml`) | Notes |
|---|---|---|
| `POSTGRES_PASSWORD` | `postgres` service env | Must also be reflected in the lobby's `DATABASE_URL`. |
| `DATABASE_URL` | `lobby` service env | Form: `postgres://postgres:<POSTGRES_PASSWORD>@postgres:5432/aplobby`. Keep the password in sync with `POSTGRES_PASSWORD`. |
| `ROCKET_SECRET_KEY` | `lobby` service env | Signs encrypted session cookies. Must be exactly 44 base64 chars (32 raw bytes), 88 base64 chars (64 raw bytes), or 64 hex chars (32 raw bytes). Generate with `openssl rand -base64 32`. Other lengths fail at startup with `InvalidLength`. |
| `ADMIN_TOKEN` | `lobby` service env | Auth for admin endpoints (`X-Api-Key` header / Basic Auth). |
| `LOBBY_API_KEY` | `generator` service env | **Must equal `ADMIN_TOKEN`** — the generator worker authenticates back to the lobby API with this. |
| `YAML_VALIDATION_QUEUE_TOKEN` | `lobby` and `yaml-checker` services | Same value in both places (queue auth between lobby and worker). |
| `GENERATION_QUEUE_TOKEN` | `lobby` and `generator` services | Same value in both places. |
| `OPTIONS_GEN_QUEUE_TOKEN` | `lobby` and `option-generator` services | Same value in both places. |
| `SECRET_KEY` (in `webhost-config.yaml`) | community/webhost only | Skip if not running community mode. |

## Other deployment-specific config

| Variable | Purpose |
|---|---|
| `VALKEY_URL` | Connection string for valkey/redis. |
| `APWORLDS_INDEX_REPO_URL` | Your fork of the apworld index repo. |
| `APWORLDS_INDEX_REPO_BRANCH` | Branch to track on the index repo. |
| `GENERATION_OUTPUT_DIR` | Path inside the lobby container where generated worlds are written. |

## Optional

| Variable | Effect when set |
|---|---|
| `SENTRY_DSN` | Enables Sentry error reporting. |
| `OTLP_ENDPOINT` | Enables OpenTelemetry / OTLP tracing. |
| `RUST_LOG` | Log filter, e.g. `info,ap_lobby=debug`. The compose example sets `debug`. |
| `SKIP_APWORLDS_UPDATE` | If set, skips fetching the apworld index on startup (useful for offline dev). |
| `PRELOAD_OPTIONS_DEFS` | If set, eagerly preloads option schemas into Redis at startup. |

## Discord OAuth

Configured in `Rocket.toml` (gitignored). On first run, `./start.sh` will prompt for credentials and write the file. To do it by hand:

```toml
[default.oauth.discord]
provider = "Discord"
client_id = "<your discord app's client id>"
client_secret = "<your discord app's client secret>"
redirect_uri = "https://<your-deployment-host>/auth/oauth"
admins = [<your discord user id>, ...]
banned_users = []   # optional
```

The `redirect_uri` must exactly match a redirect URI registered in your Discord developer application.

# Running apdiff-viewer standalone

`apdiff-viewer` renders side-by-side diffs of `.apworld` zip contents for PR reviewers. It ships as a separate service with its own postgres and a host directory for the apworld blob store, so it deploys independently of the lobby. Source under [apdiff-viewer/](apdiff-viewer/).

Bring it up from a fresh clone:

```
cargo build --release --bin apdiff-viewer
cp target/release/apdiff-viewer taskcluster/docker/apdiff-viewer/build-result
cd apdiff-viewer
cp docker-compose.yml.example docker-compose.yml
# edit changeme placeholders; set APDIFF_PUBLIC_BASE_URL to the externally
# reachable URL — it must match what your PR-validation CI side will POST to
docker compose up -d
```

The diesel migration applies on first start. Smoke-test the POST path:

```
mkdir -p /tmp/sub/{apworlds,annotations}
cp some.apworld /tmp/sub/apworlds/some-0.1.0.apworld
echo '{"pr_number":1,"commit_sha":"deadbeef","added":[{"world_name":"some","version":"0.1.0"}]}' > /tmp/sub/manifest.json
echo '{"worlds":{"some":{"world_name":"Some","added_versions":["0.1.0"],"removed_versions":[],"checksums":{}}}}' > /tmp/sub/changes.json
tar -C /tmp/sub -cf /tmp/sub.tar .
curl -fsS -X POST -H "X-Api-Key: changeme" --data-binary @/tmp/sub.tar http://localhost:8001/api/submissions
```

The response is `{ "id": "...", "url": "..." }`. Open the URL in a browser to see the rendered diff.

### Bootstrap import

A separate single-artifact endpoint exists for one-shot seeding of the dedup table — used by an index-walker job that uploads the latest version of every apworld so future PR submissions have historical from-versions to point at. Same `APDIFF_API_KEY` auth as the submission endpoint; no manifest, no `submissions` rows are created — only `apworld_artifacts` is populated.

```
curl -fsS -X POST \
  -H "X-Api-Key: $APDIFF_API_KEY" \
  --data-binary @some-0.1.0.apworld \
  "http://localhost:8001/api/import?world=some&version=0.1.0"
```

Response: `{ "world": "some", "version": "0.1.0", "sha256": "...", "size_bytes": N, "stored": true }`. `stored: false` means the `(world, version, sha256)` tuple was already in the dedup table (re-upload is idempotent).

## Required secrets (apdiff-viewer)

| Variable | Where | Notes |
|---|---|---|
| `POSTGRES_PASSWORD` (apdiff stack) | `postgres` service env | Mirror in this stack's `DATABASE_URL`. Independent of the lobby's postgres. |
| `DATABASE_URL` | `apdiff-viewer` service env | `postgres://postgres:<password>@postgres:5432/apdiff` |
| `ROCKET_SECRET_KEY` | `apdiff-viewer` service env | Rocket release-mode startup check; same encoding rules as the lobby (44 base64 / 88 base64 / 64 hex chars). The service itself doesn't use session cookies, but the check fires anyway. |
| `APDIFF_API_KEY` | `apdiff-viewer` service env | Auth for `POST /api/submissions`. Share with the CI side that POSTs tarballs. |
| `FUZZ_API_KEY` | `apdiff-viewer` service env | Inherited from the legacy fuzz-results endpoints. Required at startup even if those routes go unused. |

## Other deployment-specific config (apdiff-viewer)

| Variable | Purpose |
|---|---|
| `APDIFF_STORAGE_ROOT` | Path inside the container for the blob store. Bind-mount it from a host dir. |
| `APDIFF_PUBLIC_BASE_URL` | The externally-reachable URL prefix. Used to build the `url` field of the POST response so CI can post it into PR comments. |
| `ROCKET_ADDRESS` | Set to `0.0.0.0` to expose the service from the container. |
| `TASKCLUSTER_ROOT_URL` (plus credentials) | Optional. Set only if you also want the legacy `GET /<task_id>` routes to serve from a Taskcluster instance. Leave unset for submission-only deployments — those routes will return an error at request time. |

Both `apdiff-viewer/docker-compose.yml` and the bind-mount dirs (`pgdata/`, `blobs/`) are gitignored once created, so future `git pull`s won't clobber your edits or data.

# Caveats

When working on the `ap-worker`, if you change the python dependencies, you
have to rerun `docker compose build` and restart everything.

# APX

APX acts as a proxy service sitting between the Archipelago server and clients.

See /apx/README.md for more info
