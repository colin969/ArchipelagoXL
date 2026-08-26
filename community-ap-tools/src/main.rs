use std::{collections::{HashMap, HashSet}, str::FromStr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use askama::Template;
use askama_web::WebTemplate;
use auth::{ModeratorSession, Session};
use guards::{ApRoom, DATA_PACKAGE, LobbyRoom, SlotPasswords};
use reqwest::{
    Url,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use rocket::{catchers, tokio};
use rocket::{
    Request, State, catch, response::Redirect, routes, serde::json::Json,
};
use rocket::fs::{FileServer, relative};
use rocket_oauth2::OAuth2;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod auth;
mod datapackage;
mod error;
mod filters;
mod guards;
mod review;
mod schema;

use diesel_migrations::{EmbeddedMigrations, embed_migrations};

use crate::guards::{MergedSlotInfo, TrackerInfo};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations/");

pub const STATIC_VERSION: &str = std::env!("STATIC_VERSION");
pub struct Discord;

#[derive(Template, WebTemplate)]
#[template(path = "index.html")]
pub struct RunIndexTpl {
    lobby_room_id: Uuid,
    lobby_root_url: String,
    slot_passwords: SlotPasswords,
}

#[derive(Template, WebTemplate)]
#[template(path = "debugger.html")]
pub struct DebugSlotTpl {
    room_id: String,
    slot_id: i32,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Deathlink {
    pub slot: usize,
    pub source: String,
    pub cause: Option<String>,
    pub created_at: String,
}

#[derive(Deserialize, Debug)]
struct ExclusionsResponse(HashMap<usize, Vec<String>>);

#[derive(Deserialize, Serialize)]
struct ProbabilityResponse {
    probability: f64,
}

#[derive(Deserialize, Serialize)]
struct SetProbabilityRequest {
    probability: f64,
}

pub struct DeathlinksSlot {
    pub id: usize,
    pub name: String,
    pub game: String,
    pub discord_handle: String,
    pub is_excluded: bool,
    pub count: i32,
}

#[derive(Template, WebTemplate)]
#[template(path = "deathlinks.html")]
pub struct DeathlinksIndexTpl {
    lobby_room: LobbyRoom,
    lobby_root_url: String,
    slots: Vec<DeathlinksSlot>,
    total_deaths: i32,
}

#[catch(401)]
fn unauthorized(req: &Request) -> crate::error::Result<Redirect> {
    let session = Session::from_request_sync(req);
    if session.is_logged_in {
        Err(anyhow::anyhow!("You're not allowed here"))?
    }

    Ok(Redirect::to(format!(
        "/auth/login?redirect={}",
        req.uri().path()
    )))
}

#[rocket::get("/")]
async fn root_run(
    _session: ModeratorSession,
    lobby_room: LobbyRoom,
    ap_room: ApRoom,
    slot_passwords: SlotPasswords,
    config: &State<Config>,
) -> crate::error::Result<RunIndexTpl> {
    if lobby_room.yamls.len() != ap_room.tracker_info.slots.len() {
        Err(anyhow!(
            "The AP room slot number doesn't match the lobby, this won't work"
        ))?;
    }

    let lobby_root_url = config.lobby_public_url.as_deref()
        .unwrap_or(config.lobby_root_url.as_str())
        .to_string();

    let index = RunIndexTpl {
        lobby_room_id: lobby_room.id,
        lobby_root_url,
        slot_passwords,
    };

    Ok(index)
}

async fn fetch_full_feed_slots(config: &Config, room_id: &str) -> crate::error::Result<HashSet<usize>> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/{}/full_feed", apx_api_root, room_id))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    let raw: HashMap<usize, serde_json::Value> = response.json().await?;
    Ok(raw.into_keys().collect())
}

async fn fetch_deathlinks(config: &Config, room_id: &str) -> crate::error::Result<HashMap<usize, i32>> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/{}/deathlinks", apx_api_root, room_id))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    Ok(response.json().await?)
}

async fn fetch_exclusions(config: &Config, room_id: &str) -> crate::error::Result<HashMap<usize, Vec<String>>> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/{}/bounce_exclusions", apx_api_root, room_id))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    let data: ExclusionsResponse = response.json().await?;
    Ok(data.0)
}

async fn fetch_incomplete_sphere1s(config: &Config, room_id: &str) -> crate::error::Result<Vec<usize>> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/{}/incomplete_sphere1", apx_api_root, room_id))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    Ok(response.json().await?)
}

#[rocket::get("/api/password/<slot_id>")]
async fn get_password(
    _session: ModeratorSession,
    slot_id: i32,
    config: &State<Config>,
) -> crate::error::Result<(rocket::http::Status, rocket::serde::json::Json<serde_json::Value>)> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let url = format!(
        "{}api/{}/password/{}",
        apx_api_root, config.lobby_room_id, slot_id
    );
    let response = client
        .get(url)
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    let status = rocket::http::Status::from_code(response.status().as_u16())
        .unwrap_or(rocket::http::Status::InternalServerError);
    let body: serde_json::Value = response.json().await?;

    Ok((status, rocket::serde::json::Json(body)))
}

#[rocket::get("/deathlinks")]
async fn deathlinks(
    _session: ModeratorSession,
    lobby_room: LobbyRoom,
    ap_room: ApRoom,
    config: &State<Config>,
) -> crate::error::Result<DeathlinksIndexTpl> {
    let room_id = lobby_room.id.to_string();

    let deathlinks = fetch_deathlinks(config, &room_id).await.unwrap_or_default();
    let excluded_slots = fetch_exclusions(config, &room_id).await.unwrap_or_default();

    let deathlink_tag = String::from("DeathLink");
    let slots: Vec<DeathlinksSlot> = ap_room
        .tracker_info
        .slots
        .iter()
        .zip(lobby_room.yamls.iter())
        .map(|(slot, lobby_slot)| DeathlinksSlot {
            id: slot.id,
            name: slot.name.clone(),
            game: slot.game.clone(),
            discord_handle: lobby_slot.discord_handle.clone(),
            // Definitely a cleaner way of doing this
            is_excluded: excluded_slots.get(&slot.id).map_or(false, |slots| slots.contains(&deathlink_tag)),
            count: *deathlinks.get(&slot.id).unwrap_or(&0),
        })
        .collect();

    let total_deaths = deathlinks.values().sum();

    Ok(DeathlinksIndexTpl {
        lobby_room,
        lobby_root_url: config.lobby_root_url.to_string(),
        slots,
        total_deaths,
    })
}

#[rocket::post("/api/bounce_exclusions/<slot_id>/<tag_name>")]
async fn proxy_add_exclusion(
    _session: ModeratorSession,
    slot_id: i32,
    tag_name: &str,
    config: &State<Config>,
    cache: &State<TrackerInfoCache>,
) -> crate::error::Result<(rocket::http::Status, rocket::serde::json::Json<serde_json::Value>)> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let url = format!(
        "{}api/{}/bounce_exclusions/{}/{}",
        apx_api_root, config.lobby_room_id, slot_id, tag_name
    );
    let response = client
        .post(url)
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    let status = rocket::http::Status::from_code(response.status().as_u16())
        .unwrap_or(rocket::http::Status::InternalServerError);
    let body: serde_json::Value = response.json().await?;

    Ok((status, rocket::serde::json::Json(body)))
}

#[rocket::delete("/api/bounce_exclusions/<slot_id>/<tag_name>")]
async fn proxy_remove_exclusion(
    _session: ModeratorSession,
    slot_id: i32,
    tag_name: &str,
    config: &State<Config>,
    cache: &State<TrackerInfoCache>,
) -> crate::error::Result<rocket::http::Status> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .delete(format!(
            "{}api/{}/bounce_exclusions/{}/{}",
            apx_api_root, config.lobby_room_id, slot_id, tag_name
        ))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(rocket::http::Status::from_code(response.status().as_u16())
        .unwrap_or(rocket::http::Status::InternalServerError))
}

#[rocket::post("/api/full_feed/<slot_id>")]
async fn add_full_feed(
    _session: ModeratorSession,
    slot_id: i32,
    config: &State<Config>,
    cache: &State<TrackerInfoCache>,
) -> crate::error::Result<rocket::http::Status> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .post(format!(
            "{}api/{}/full_feed/{}",
            apx_api_root, config.lobby_room_id, slot_id
        ))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(rocket::http::Status::from_code(response.status().as_u16())
        .unwrap_or(rocket::http::Status::InternalServerError))
}

#[rocket::delete("/api/full_feed/<slot_id>")]
async fn remove_full_feed(
    _session: ModeratorSession,
    slot_id: i32,
    config: &State<Config>,
    cache: &State<TrackerInfoCache>,
) -> crate::error::Result<rocket::http::Status> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .delete(format!(
            "{}api/{}/full_feed/{}",
            apx_api_root, config.lobby_room_id, slot_id
        ))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(rocket::http::Status::from_code(response.status().as_u16())
        .unwrap_or(rocket::http::Status::InternalServerError))
}

#[rocket::get("/api/deathlink_probability")]
async fn get_deathlink_probability(
    _session: ModeratorSession,
    config: &State<Config>,
) -> crate::error::Result<Json<ProbabilityResponse>> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/{}/deathlink_probability", apx_api_root, config.lobby_room_id))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    let data: ProbabilityResponse = response.json().await?;
    Ok(Json(data))
}

#[rocket::post("/api/deathlink_probability", data = "<request>")]
async fn set_deathlink_probability(
    _session: ModeratorSession,
    config: &State<Config>,
    request: Json<SetProbabilityRequest>,
) -> crate::error::Result<Json<ProbabilityResponse>> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/{}/deathlink_probability", apx_api_root, config.lobby_room_id))
        .header("X-API-Key", apx_api_key)
        .json(&request.into_inner())
        .send()
        .await?;

    let data: ProbabilityResponse = response.json().await?;
    Ok(Json(data))
}

#[derive(Deserialize, Serialize)]
struct DeferredGamesResponse {
    games: Vec<String>,
}

#[derive(Deserialize, Serialize)]
struct AddDeferredGameRequest {
    game_name: String,
}

#[rocket::get("/api/apx/deferred_datapackage_games")]
async fn get_deferred_datapackage_games(
    _session: auth::AdminSession,
    config: &State<Config>,
) -> crate::error::Result<Json<DeferredGamesResponse>> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/api/{}/deferred_datapackage_games", apx_api_root, config.lobby_room_id))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    let data: DeferredGamesResponse = response.json().await?;
    Ok(Json(data))
}

#[rocket::post("/api/apx/deferred_datapackage_games", data = "<request>")]
async fn add_deferred_datapackage_game(
    _session: auth::AdminSession,
    config: &State<Config>,
    request: Json<AddDeferredGameRequest>,
) -> crate::error::Result<rocket::http::Status> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/{}/deferred_datapackage_games", apx_api_root, config.lobby_room_id))
        .header("X-API-Key", apx_api_key)
        .json(&request.into_inner())
        .send()
        .await?;

    Ok(rocket::http::Status::from_code(response.status().as_u16())
        .unwrap_or(rocket::http::Status::InternalServerError))
}

#[rocket::delete("/api/apx/deferred_datapackage_games/<game_name>")]
async fn remove_deferred_datapackage_game(
    _session: auth::AdminSession,
    game_name: &str,
    config: &State<Config>,
) -> crate::error::Result<rocket::http::Status> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let client = reqwest::Client::new();
    let mut url = apx_api_root.join(&format!("/api/{}/deferred_datapackage_games/", config.lobby_room_id))?;
    url.path_segments_mut()
        .map_err(|_| anyhow!("Invalid APX URL"))?
        .push(game_name);

    let response = client
        .delete(url)
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    Ok(rocket::http::Status::from_code(response.status().as_u16())
        .unwrap_or(rocket::http::Status::InternalServerError))
}

#[rocket::get("/hint/<ty>/<slot_name>/<item_name>")]
async fn hint(
    _session: ModeratorSession,
    ty: &str,
    slot_name: &str,
    item_name: &str,
    config: &State<Config>,
) -> crate::error::Result<Redirect> {
    if !["item", "location"].contains(&ty) {
        Err(anyhow::anyhow!(
            "Wrong hint type. Only item/location are supported"
        ))?;
    }

    let cmd = if ty == "item" {
        "/hint"
    } else {
        "/hint_location"
    };

    let cmd = format!(
        "{} {} {}",
        cmd,
        shlex::try_quote(slot_name)?,
        shlex::try_quote(item_name)?
    );

    ap_cmd(cmd, config).await?;

    Ok(Redirect::to("/"))
}

#[rocket::get("/give/<ty>/<slot_name>/<item_name>")]
async fn give(
    _session: ModeratorSession,
    ty: &str,
    slot_name: &str,
    item_name: &str,
    config: &State<Config>,
) -> crate::error::Result<Redirect> {
    if !["item", "location"].contains(&ty) {
        Err(anyhow::anyhow!(
            "Wrong give type. Only item/location are supported"
        ))?;
    }

    let cmd = if ty == "item" {
        "/send"
    } else {
        "/send_location"
    };

    let cmd = format!(
        "{} {} {}",
        cmd,
        shlex::try_quote(slot_name)?,
        shlex::try_quote(item_name)?
    );

    ap_cmd(cmd, config).await?;

    Ok(Redirect::to("/"))
}

async fn ap_cmd(cmd: String, config: &State<Config>) -> crate::error::Result<()> {
    let client = reqwest::Client::new();
    let form = reqwest::multipart::Form::new().text("cmd", cmd);

    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-api-key"),
        HeaderValue::from_str(&config.ap_admin_api_key)?,
    );

    // There's no point in looking at the response here. AP doesn't have a proper API for rooms
    // since sending a command just inserts something in database that gets polled by the room
    // process later on so they don't provide responses. If anything fails, it just ignores the
    // input and nothing happens...
    let _ = client
        .post(config.ap_room_url.clone())
        .multipart(form)
        .headers(headers)
        .send()
        .await?;

    Ok(())
}

#[rocket::get("/release/<slot_name>")]
async fn release(
    _session: ModeratorSession,
    slot_name: &str,
    config: &State<Config>,
) -> crate::error::Result<Redirect> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?;

    let mut url = apx_api_root
        .join(&format!("/api/{}/release/", config.lobby_room_id))?;
    url.path_segments_mut()
        .map_err(|_| anyhow!("Invalid APX URL"))?
        .push(slot_name);

    let response = reqwest::Client::new()
        .post(url)
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    if !response.status().is_success() {
        Err(anyhow!("Release failed: {}", response.status()))?;
    }

    Ok(Redirect::to("/"))
}

#[rocket::get("/completion/<ty>/<game_name>")]
async fn autocompletion(
    _session: ModeratorSession,
    ty: &str,
    game_name: &str,
) -> crate::error::Result<Json<Vec<String>>> {
    let datapackage = DATA_PACKAGE.get().context("No datapackage loaded")?;
    let game = datapackage
        .data
        .games
        .get(game_name)
        .context("Couldn't find game")?;
    let names = if ty == "item" {
        game.game_data.item_name_to_id.keys().cloned().collect()
    } else {
        game.game_data.location_name_to_id.keys().cloned().collect()
    };

    Ok(Json(names))
}

async fn notify_proxy_password_refresh(config: &State<Config>) {
    let (Some(apx_root), Some(apx_key)) = (&config.apx_api_root, &config.apx_api_key) else {
        return;
    };

    let apx_url = match apx_root.join(&format!("/api/{}/refresh_passwords", config.lobby_room_id)) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("[REFRESH_PASSWORDS] Failed to build APX URL: {}", e);
            return;
        }
    };

    let Ok(header_value) = HeaderValue::from_str(apx_key) else {
        eprintln!("[REFRESH_PASSWORDS] Invalid APX API key");
        return;
    };

    let result = reqwest::Client::new()
        .post(apx_url.clone())
        .header(HeaderName::from_static("x-api-key"), header_value)
        .send()
        .await;

    match result {
        Ok(resp) if !resp.status().is_success() => {
            eprintln!(
                "[REFRESH_PASSWORDS] APX API returned error: ({}) - {}",
                apx_url.as_str(), resp.status()
            );
        }
        Err(e) => {
            eprintln!("[REFRESH_PASSWORDS] Failed to notify APX API: {}", e);
        }
        _ => {}
    }
}

#[derive(Deserialize, Serialize)]
struct SetPasswordRequest {
    password: Option<String>,
}

#[rocket::post("/gen_all_passwords")]
async fn gen_all_passwords(
    _session: ModeratorSession,
    config: &State<Config>,
    cache: &State<TrackerInfoCache>,
) -> crate::error::Result<()> {
    let client = reqwest::Client::new();
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-api-key"),
        HeaderValue::from_str(&config.lobby_api_key)?,
    );

    let url = config.lobby_root_url.join(&format!(
        "/api/room/{}/gen_all_passwords",
        config.lobby_room_id
    ))?;

    let response = client
        .post(url)
        .headers(headers)
        .send()
        .await?;

    if !response.status().is_success() {
        Err(anyhow!("Failed to gen passwords: {}", response.status()))?;
    }

    notify_proxy_password_refresh(config).await;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(())
}

#[rocket::post("/set_password/<yaml_id>", data = "<request>")]
async fn set_password(
    _session: ModeratorSession,
    yaml_id: &str,
    request: Json<SetPasswordRequest>,
    config: &State<Config>,
    cache: &State<TrackerInfoCache>,
) -> crate::error::Result<()> {
    let client = reqwest::Client::new();
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-api-key"),
        HeaderValue::from_str(&config.lobby_api_key)?,
    );

    let url = config.lobby_root_url.join(&format!(
        "/api/room/{}/set_password/{}",
        config.lobby_room_id, yaml_id
    ))?;

    let response = client
        .post(url)
        .headers(headers)
        .json(&request.into_inner())
        .send()
        .await?;

    if !response.status().is_success() {
        Err(anyhow!("Failed to set password: {}", response.status()))?;
    }

    notify_proxy_password_refresh(config).await;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(())
}

fn deserialize_i64_from_string<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

#[derive(Deserialize, Serialize)]
struct ChangeYamlOwnerRequest {
    #[serde(deserialize_with = "deserialize_i64_from_string")]
    new_owner_id: i64,
    new_password: Option<String>,
}

#[rocket::put("/change_owner/<yaml_id>", data = "<request>")]
async fn change_yaml_owner(
    _session: ModeratorSession,
    yaml_id: &str,
    request: Json<ChangeYamlOwnerRequest>,
    config: &State<Config>,
    cache: &State<TrackerInfoCache>,
) -> crate::error::Result<()> {
    let client = reqwest::Client::new();
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-api-key"),
        HeaderValue::from_str(&config.lobby_api_key)?,
    );

    let url = config.lobby_root_url.join(&format!(
        "/api/room/{}/yaml/{}",
        config.lobby_room_id, yaml_id
    ))?;

    let response = client
        .put(url)
        .headers(headers)
        .json(&request.into_inner())
        .send()
        .await?;

    if !response.status().is_success() {
        Err(anyhow!(
            "Failed to change YAML owner: {}",
            response.status()
        ))?;
    }

    notify_proxy_password_refresh(config).await;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(())
}

#[rocket::get("/debug_slot/<slot_id>")]
async fn debug_slot_page(
    _session: ModeratorSession,
    slot_id: i32,
    config: &State<Config>,
) -> crate::error::Result<DebugSlotTpl> {
    Ok(DebugSlotTpl {
        room_id: config.lobby_room_id.to_string(),
        slot_id,
    })
}

#[rocket::get("/api/debug/slot/<slot_id>")]
async fn debug_slot_tap(
    _session: ModeratorSession,
    slot_id: i32,
    config: &State<Config>,
    ws: rocket_ws::WebSocket,
) -> crate::error::Result<rocket_ws::Channel<'static>> {
    use rocket::futures::{SinkExt, StreamExt};
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow!("APX API not configured"))?
        .clone();
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow!("APX API key not configured"))?
        .clone();

    let apx_url = apx_api_root
        .join(&format!("/api/{}/debug/slot/{}", config.lobby_room_id, slot_id))?;

    // Convert http(s) -> ws(s)
    let ws_url = apx_url.as_str().replacen("http", "ws", 1);

    Ok(ws.channel(move |mut client_stream| Box::pin(async move {
        let mut request = tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(ws_url.as_str())
            .map_err(|e| rocket_ws::result::Error::Io(std::io::Error::other(e)))?;

        request.headers_mut().insert(
            "X-API-Key",
            apx_api_key.parse().map_err(|e| rocket_ws::result::Error::Io(std::io::Error::other(e)))?,
        );

        let (mut apx_stream, _) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|e| rocket_ws::result::Error::Io(std::io::Error::other(e)))?;

        // Forward APX -> client
        loop {
            match apx_stream.next().await {
                Some(Ok(msg)) => {
                    let bytes = msg.into_data();
                    if client_stream.send(rocket_ws::Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
                _ => break,
            }
        }

        Ok(())
    })))
}


pub struct Config {
    pub lobby_root_url: Url,
    pub lobby_public_url: Option<String>,
    pub lobby_room_id: Uuid,
    pub lobby_api_key: String,
    pub ap_room_id: String,
    pub ap_room_url: Url,
    pub ap_api_root: Url,
    pub ap_room_host: String,
    pub ap_admin_api_key: String,
    pub apx_api_root: Option<Url>,
    pub apx_api_key: Option<String>,
}

pub struct TrackerInfoCache(pub Arc<tokio::sync::Mutex<Option<(Instant, Vec<MergedSlotInfo>)>>>);
pub struct ApRoomCache(pub Arc<Mutex<Option<(Instant, TrackerInfo)>>>);

const TRACKER_CACHE_TTL: Duration = Duration::from_secs(30);

#[rocket::main]
async fn main() -> crate::error::Result<()> {
    let _ = dotenvy::dotenv().ok();

    let lobby_root_url =
        std::env::var("LOBBY_ROOT_URL").expect("Provide a `LOBBY_ROOT_URL` env variable");
    let lobby_public_url = 
        std::env::var("LOBBY_PUBLIC_URL").ok();
    let lobby_room_id =
        std::env::var("LOBBY_ROOM_ID").expect("Provide a `LOBBY_ROOM_ID` env variable");
    let lobby_api_key =
        std::env::var("LOBBY_API_KEY").expect("Provide a `LOBBY_API_KEY` env variable");
    let ap_room_id = std::env::var("AP_ROOM_ID").expect("Provide an `AP_ROOM_ID` env variable");
    let ap_room_host =
        std::env::var("AP_ROOM_HOST").expect("Provide an `AP_ROOM_HOST` env variable");
    let ap_admin_api_key =
        std::env::var("AP_ADMIN_API_KEY").expect("Provide an `AP_ADMIN_AP_KEY` env variable");

    let ap_api_root = std::env::var("AP_API_ROOT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| Url::from_str(&format!("{}", ap_room_host)).unwrap());
    let ap_api_root = std::env::var("AP_API_ROOT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| Url::from_str(&format!("{}", ap_room_host)).unwrap());

    eprintln!("[STARTUP] AP_API_ROOT: {}", ap_api_root);
    eprintln!("[STARTUP] AP_ROOM_HOST: {}", ap_room_host);
    eprintln!("[STARTUP] AP_ROOM_ID: {}", ap_room_id);

    let apx_api_root = std::env::var("APX_API_ROOT")
        .ok()
        .and_then(|s| s.parse().ok());
    let apx_api_key = std::env::var("APX_API_KEY").ok();

    let db_url = std::env::var("DATABASE_URL").expect("Provide a `DATABASE_URL` env variable");
    let db_pool = common::db::get_database_pool(&db_url, MIGRATIONS).await?;

    let ap_room_url = ap_api_root.join(&format!("/room/{}", ap_room_id))?;
    eprintln!("[STARTUP] Constructed AP_ROOM_URL: {}", ap_room_url);

    let mut config = Config {
        lobby_root_url: lobby_root_url.parse()?,
        lobby_public_url,
        lobby_room_id: lobby_room_id.parse()?,
        lobby_api_key,
        ap_room_url,
        ap_api_root,
        ap_room_host,
        ap_room_id,
        ap_admin_api_key,
        apx_api_root,
        apx_api_key,
    };

    rocket::build()
        .mount(
            "/",
            routes![
                root_run,
                deathlinks,
                proxy_add_exclusion,
                proxy_remove_exclusion,
                get_deathlink_probability,
                set_deathlink_probability,
                release,
                hint,
                autocompletion,
                give,
                get_password,
                set_password,
                gen_all_passwords,
                change_yaml_owner,
                get_deferred_datapackage_games,
                add_deferred_datapackage_game,
                remove_deferred_datapackage_game,
                add_full_feed,
                remove_full_feed,
                debug_slot_page,
                debug_slot_tap,
            ],
        )
        .mount("/static", FileServer::from(relative!("static")))
        .mount("/auth", auth::routes())
        .mount("/", review::page::routes())
        .mount("/api", review::api::routes())
        .mount("/api/admin", review::api::admin_routes())
        .register("/", catchers![unauthorized])
        .manage(rocket::Config::figment())
        .manage(config)
        .manage(db_pool)
        .manage(TrackerInfoCache(Arc::new(tokio::sync::Mutex::new(None))))
        .manage(ApRoomCache(Arc::new(Mutex::new(None))))
        .attach(OAuth2::<Discord>::fairing("discord"))
        .launch()
        .await
        .unwrap();

    Ok(())
}
