use std::collections::HashSet;
use std::time::Instant;

use anyhow::anyhow;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::deadpool::Pool as DieselPool;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rocket::{State, routes, serde::json::Json};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Config, TRACKER_CACHE_TTL, TrackerInfoCache, fetch_full_feed_slots, fetch_deathlinks, fetch_exclusions, fetch_incomplete_sphere1s};
use crate::auth::{AdminSession, LoggedInSession, ModeratorSession};
use crate::error;
use crate::guards::{ApRoom, LobbyRoom, MergedSlotInfo};
use crate::review::Role;
use crate::review::db;

#[derive(Deserialize)]
struct SetRoomPresetRequest {
    preset_id: i32,
}

#[rocket::get("/room/<room_id>/preset")]
async fn get_room_preset(
    session: LoggedInSession,
    room_id: &str,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<Json<Option<db::RoomReviewConfig>>> {
    let room_id: Uuid = room_id
        .parse()
        .map_err(|_| error::bad_request("Invalid room ID"))?;
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    session
        .require_room_role(room_id, Role::Viewer, &mut conn)
        .await?;
    let config = db::get_room_config(room_id, &mut conn).await?;
    Ok(Json(config))
}

#[rocket::put("/room/<room_id>/preset", data = "<body>")]
async fn set_room_preset(
    _session: AdminSession,
    room_id: &str,
    body: Json<SetRoomPresetRequest>,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<()> {
    let room_id: Uuid = room_id.parse().map_err(|_| anyhow!("Invalid room ID"))?;
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    db::set_room_preset(room_id, body.preset_id, &mut conn).await?;
    Ok(())
}

#[rocket::delete("/room/<room_id>/preset")]
async fn remove_room_preset(
    _session: AdminSession,
    room_id: &str,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<()> {
    let room_id: Uuid = room_id.parse().map_err(|_| anyhow!("Invalid room ID"))?;
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    db::remove_room_preset(room_id, &mut conn).await?;
    Ok(())
}

#[rocket::get("/games")]
async fn proxy_games(
    _session: LoggedInSession,
    config: &State<Config>,
) -> crate::error::Result<Json<serde_json::Value>> {
    let client = reqwest::Client::new();
    let url = config.lobby_root_url.join("/api/games")?;
    let resp = client
        .get(url)
        .header("x-api-key", &config.lobby_api_key)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow!("Failed to fetch games: {}", resp.status()).into());
    }
    let data: serde_json::Value = resp.json().await?;
    Ok(Json(data))
}

#[rocket::get("/games/<apworld>/options")]
async fn proxy_game_options(
    _session: LoggedInSession,
    apworld: &str,
    config: &State<Config>,
) -> crate::error::Result<Json<serde_json::Value>> {
    let client = reqwest::Client::new();
    let url = config
        .lobby_root_url
        .join(&format!("/api/games/{}/options", apworld))?;
    let resp = client
        .get(url)
        .header("x-api-key", &config.lobby_api_key)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow!("Failed to fetch game options: {}", resp.status()).into());
    }
    let data: serde_json::Value = resp.json().await?;
    Ok(Json(data))
}

#[derive(Deserialize, Serialize)]
struct LobbyYamlDetail {
    content: String,
    game: String,
    player_name: String,
    edited_content: Option<String>,
    last_edited_by_name: Option<String>,
    last_edited_at: Option<String>,
}

#[rocket::get("/review/<room_id>/yaml/<yaml_id>")]
async fn proxy_yaml_content(
    session: LoggedInSession,
    room_id: &str,
    yaml_id: &str,
    config: &State<Config>,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<Json<LobbyYamlDetail>> {
    let room_id: Uuid = room_id
        .parse()
        .map_err(|_| error::bad_request("Invalid room ID"))?;
    let yaml_id: Uuid = yaml_id
        .parse()
        .map_err(|_| error::bad_request("Invalid YAML ID"))?;
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    session
        .require_room_role(room_id, Role::Viewer, &mut conn)
        .await?;

    let client = reqwest::Client::new();
    let url = config
        .lobby_root_url
        .join(&format!("/api/room/{}/info/{}", room_id, yaml_id))?;
    let resp = client
        .get(url)
        .header("x-api-key", &config.lobby_api_key)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow!("Failed to fetch YAML: {}", resp.status()).into());
    }
    let data: LobbyYamlDetail = resp.json().await?;
    Ok(Json(data))
}

#[derive(Serialize)]
struct ReviewStatusResponse {
    room_id: Uuid,
    yaml_id: Uuid,
    status: String,
    changed_by: i64,
    changed_by_name: Option<String>,
    changed_at: String,
}

#[rocket::get("/review/<room_id>/statuses")]
async fn get_review_statuses(
    session: LoggedInSession,
    room_id: &str,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<Json<Vec<ReviewStatusResponse>>> {
    let room_id: Uuid = room_id
        .parse()
        .map_err(|_| error::bad_request("Invalid room ID"))?;
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    session
        .require_room_role(room_id, Role::Viewer, &mut conn)
        .await?;

    let statuses = db::get_review_statuses(room_id, &mut conn).await?;

    let response: Vec<ReviewStatusResponse> = statuses
        .into_iter()
        .map(|s| ReviewStatusResponse {
            room_id: s.room_id,
            yaml_id: s.yaml_id,
            status: s.status,
            changed_by: s.changed_by,
            changed_by_name: s.changed_by_name,
            changed_at: s.changed_at.to_rfc3339(),
        })
        .collect();

    Ok(Json(response))
}

#[derive(Deserialize)]
struct SetReviewStatusRequest {
    status: String,
}

#[rocket::put("/review/<room_id>/status/<yaml_id>", data = "<body>")]
async fn set_review_status(
    session: LoggedInSession,
    room_id: &str,
    yaml_id: &str,
    body: Json<SetReviewStatusRequest>,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<Json<ReviewStatusResponse>> {
    let room_id: Uuid = room_id
        .parse()
        .map_err(|_| error::bad_request("Invalid room ID"))?;
    let yaml_id: Uuid = yaml_id
        .parse()
        .map_err(|_| error::bad_request("Invalid YAML ID"))?;
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    session
        .require_room_role(room_id, Role::Reviewer, &mut conn)
        .await?;

    let req = body.into_inner();

    let valid_statuses = ["unreviewed", "reported", "ok", "nok"];
    if !valid_statuses.contains(&req.status.as_str()) {
        return Err(error::bad_request(format!(
            "Invalid status: {}",
            req.status
        )));
    }

    let user_id = session.user_id();
    let username = session.username();
    let status =
        db::set_review_status(room_id, yaml_id, &req.status, user_id, username, &mut conn).await?;

    Ok(Json(ReviewStatusResponse {
        room_id: status.room_id,
        yaml_id: status.yaml_id,
        status: status.status,
        changed_by: status.changed_by,
        changed_by_name: status.changed_by_name,
        changed_at: status.changed_at.to_rfc3339(),
    }))
}

#[derive(Deserialize)]
struct EditYamlProxyRequest {
    content: String,
}

#[derive(Serialize)]
struct EditYamlLobbyRequest {
    content: String,
    edited_by: i64,
    edited_by_name: String,
}

#[rocket::put("/review/<room_id>/yaml/<yaml_id>/edit", data = "<body>")]
async fn proxy_yaml_edit(
    session: LoggedInSession,
    room_id: &str,
    yaml_id: &str,
    body: Json<EditYamlProxyRequest>,
    config: &State<Config>,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<Json<serde_json::Value>> {
    let room_id: Uuid = room_id
        .parse()
        .map_err(|_| error::bad_request("Invalid room ID"))?;
    let yaml_id: Uuid = yaml_id
        .parse()
        .map_err(|_| error::bad_request("Invalid YAML ID"))?;
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    session
        .require_room_role(room_id, Role::Editor, &mut conn)
        .await?;

    let req = body.into_inner();
    let lobby_req = EditYamlLobbyRequest {
        content: req.content,
        edited_by: session.user_id(),
        edited_by_name: session.username().to_string(),
    };

    let client = reqwest::Client::new();
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-api-key"),
        HeaderValue::from_str(&config.lobby_api_key)?,
    );

    let url = config
        .lobby_root_url
        .join(&format!("/api/room/{}/yaml/{}/edit", room_id, yaml_id))?;

    let response = client
        .put(url)
        .headers(headers)
        .json(&lobby_req)
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await?;

    if !status.is_success() {
        return Err(anyhow!("{}", text).into());
    }

    let data: serde_json::Value = serde_json::from_str(&text)?;
    Ok(Json(data))
}

#[rocket::delete("/review/<room_id>/yaml/<yaml_id>")]
async fn proxy_yaml_delete(
    session: LoggedInSession,
    room_id: &str,
    yaml_id: &str,
    config: &State<Config>,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<Json<serde_json::Value>> {
    let room_id: Uuid = room_id
        .parse()
        .map_err(|_| error::bad_request("Invalid room ID"))?;
    let yaml_id: Uuid = yaml_id
        .parse()
        .map_err(|_| error::bad_request("Invalid YAML ID"))?;
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    session
        .require_room_role(room_id, Role::Editor, &mut conn)
        .await?;

    let client = reqwest::Client::new();
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-api-key"),
        HeaderValue::from_str(&config.lobby_api_key)?,
    );

    let url = config
        .lobby_root_url
        .join(&format!("/api/room/{}/yaml/{}", room_id, yaml_id))?;

    let response = client.delete(url).headers(headers).send().await?;

    let status = response.status();
    let text = response.text().await?;

    if !status.is_success() {
        return Err(anyhow!("{}", text).into());
    }

    let data: serde_json::Value = serde_json::from_str(&text)?;
    Ok(Json(data))
}

#[rocket::get("/review/<room_id>/notes/<yaml_id>")]
async fn get_notes(
    session: LoggedInSession,
    room_id: Uuid,
    yaml_id: Uuid,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<Json<Vec<db::YamlReviewNote>>> {
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    session
        .require_room_role(room_id, Role::Viewer, &mut conn)
        .await?;
    Ok(Json(db::get_notes(room_id, yaml_id, &mut conn).await?))
}

#[derive(Deserialize)]
struct AddNoteRequest {
    content: String,
}

#[rocket::post("/review/<room_id>/notes/<yaml_id>", data = "<body>")]
async fn add_note(
    session: LoggedInSession,
    room_id: Uuid,
    yaml_id: Uuid,
    body: Json<AddNoteRequest>,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<Json<db::YamlReviewNote>> {
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    session
        .require_room_role(room_id, Role::Reviewer, &mut conn)
        .await?;
    let req = body.into_inner();

    if req.content.trim().is_empty() {
        return Err(error::bad_request("Note content cannot be empty"));
    }

    let note = db::add_note(
        room_id,
        yaml_id,
        &req.content,
        session.user_id(),
        session.username(),
        &mut conn,
    )
    .await?;

    Ok(Json(note))
}

#[rocket::delete("/review/<room_id>/notes/<note_id>")]
async fn delete_note(
    session: LoggedInSession,
    room_id: Uuid,
    note_id: i32,
    pool: &State<DieselPool<AsyncPgConnection>>,
) -> crate::error::Result<()> {
    let mut conn = pool.get().await.map_err(|e| anyhow!(e))?;
    session
        .require_room_role(room_id, Role::Reviewer, &mut conn)
        .await?;
    let deleted = db::delete_note(note_id, room_id, session.user_id(), &mut conn).await?;
    if deleted == 0 {
        return Err(error::not_found("Note not found or you are not the author"));
    }
    Ok(())
}

#[allow(unused_variables)]
#[rocket::get("/dashboard/<lobby_room_id>/tracker_info")]
async fn get_tracker_info(
    _session: ModeratorSession,
    lobby_room_id: &str,
    ap_room: ApRoom,
    lobby_room: LobbyRoom,
    config: &State<Config>,
    cache: &State<TrackerInfoCache>,
) -> crate::error::Result<Json<Vec<MergedSlotInfo>>> {
    // Check cache first
    let mut lock = cache.0.lock().await;

    if let Some((cached_at, ref data)) = *lock {
        if cached_at.elapsed() < TRACKER_CACHE_TTL {
            return Ok(Json(data.clone()));
        }
    }

    if lobby_room.yamls.len() != ap_room.tracker_info.slots.len() {
        return Err(error::internal_server_error("The AP room slot number doesn't match the lobby, this won't work"));
    }

    let room_id = lobby_room.id.to_string();
    let full_feed_slots = fetch_full_feed_slots(config, &room_id).await.unwrap_or_default();
    let deathlinks = fetch_deathlinks(config, &room_id).await.unwrap_or_default();
    let exclusions = fetch_exclusions(config, &room_id).await.unwrap_or_default();
    let incomplete_sphere1s: HashSet<usize> = fetch_incomplete_sphere1s(config, &room_id)    
        .await
        .unwrap_or_default()
        .into_iter()
        .collect();
    let deathlink_tag = String::from("DeathLink");

    let slots: Vec<MergedSlotInfo> = ap_room
        .tracker_info
        .slots
        .into_iter()
        .map(|slot| {
            let yaml = &lobby_room.yamls[slot.id - 1];

            MergedSlotInfo {
                // It's moved for `name:` later, feels stupid but it works to put this line higher
                deathlinks_sent: *deathlinks.get(&slot.id).unwrap_or(&0),
                deathlink_excluded: exclusions.get(&slot.id).map_or(false, |slots| slots.contains(&deathlink_tag)),
                incomplete_sphere1: incomplete_sphere1s.contains(&slot.id),
                id: slot.id,
                name: slot.name,
                game: slot.game,
                checks: slot.checks,
                status: slot.status,
                last_activity: slot.last_activity,
                lobby_slot_id: yaml.id,
                discord_handle: yaml.discord_handle.clone(),
                discord_id: yaml.discord_id.to_string(),
                has_patch: yaml.has_patch,
                full_feed: full_feed_slots.contains(&slot.id)
            }
        })
        .collect();


    // Save to cache
    *lock = Some((Instant::now(), slots.clone()));

    Ok(Json(slots))
}

#[rocket::get("/dashboard/<lobby_room_id>/spheres")]
async fn get_all_spheres(
    _session: ModeratorSession,
    lobby_room_id: &str,
    config: &State<Config>,
) -> crate::error::Result<Json<serde_json::Value>> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let url = format!("{}/api/{}/spheres", apx_api_root, lobby_room_id);
    let resp = client
        .get(&url)
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(anyhow!("Failed to fetch all spheres: {}", resp.status()).into());
    }

    let data: serde_json::Value = resp.json().await?;
    Ok(Json(data))
}

#[rocket::get("/dashboard/<lobby_room_id>/spheres/<slot_id>")]
async fn get_slot_spheres(
    _session: ModeratorSession,
    lobby_room_id: &str,
    slot_id: i32,
    config: &State<Config>,
) -> crate::error::Result<Json<serde_json::Value>> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let url = format!("{}/api/{}/spheres/{}", apx_api_root, lobby_room_id, slot_id);
    let resp = client
        .get(&url)
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(anyhow!("Failed to fetch spheres: {}", resp.status()).into());
    }

    let data: serde_json::Value = resp.json().await?;
    Ok(Json(data))
}

pub fn routes() -> Vec<rocket::Route> {
    routes![
        get_room_preset,
        set_room_preset,
        remove_room_preset,
        proxy_games,
        proxy_game_options,
        proxy_yaml_content,
        get_review_statuses,
        set_review_status,
        proxy_yaml_edit,
        proxy_yaml_delete,
        get_notes,
        add_note,
        delete_note,
        get_tracker_info,
        get_slot_spheres,
        get_all_spheres,
    ]
}
