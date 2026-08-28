use std::{collections::{BTreeMap, HashMap, HashSet}, str::FromStr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow};
use askama::Template;
use askama_web::WebTemplate;
use auth::{ModeratorSession, Session};
use guards::{ApRoom, ApxRoomInfo, LobbyRoom, SlotPasswords};
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

#[allow(unused_variables)]
#[rocket::get("/dashboard/<lobby_room_id>")]
async fn dashboard(
    _session: ModeratorSession,
    lobby_room_id: &str,
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

#[rocket::get("/api/dashboard/<lobby_room_id>/password/<slot_id>")]
async fn get_password(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
        apx_api_root, lobby_room_id, slot_id
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

#[allow(unused_variables)]
#[rocket::get("/dashboard/<lobby_room_id>/deathlinks")]
async fn deathlinks(
    _session: ModeratorSession,
    lobby_room_id: &str,
    lobby_room: LobbyRoom,
    ap_room: ApRoom,
    config: &State<Config>,
) -> crate::error::Result<DeathlinksIndexTpl> {
    let deathlinks = fetch_deathlinks(config, &lobby_room.id.to_string()).await.unwrap_or_default();
    let excluded_slots = fetch_exclusions(config, &lobby_room.id.to_string()).await.unwrap_or_default();

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

#[rocket::post("/api/dashboard/<lobby_room_id>/bounce_exclusions/<slot_id>/<tag_name>")]
async fn proxy_add_exclusion(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
        apx_api_root, lobby_room_id, slot_id, tag_name
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

#[rocket::delete("/api/dashboard/<lobby_room_id>/bounce_exclusions/<slot_id>/<tag_name>")]
async fn proxy_remove_exclusion(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
            apx_api_root, lobby_room_id, slot_id, tag_name
        ))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(rocket::http::Status::from_code(response.status().as_u16())
        .unwrap_or(rocket::http::Status::InternalServerError))
}

#[rocket::post("/api/dashboard/<lobby_room_id>/full_feed/<slot_id>")]
async fn add_full_feed(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
            apx_api_root, lobby_room_id, slot_id
        ))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(rocket::http::Status::from_code(response.status().as_u16())
        .unwrap_or(rocket::http::Status::InternalServerError))
}

#[rocket::delete("/api/dashboard/<lobby_room_id>/full_feed/<slot_id>")]
async fn remove_full_feed(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
            apx_api_root, lobby_room_id, slot_id
        ))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(rocket::http::Status::from_code(response.status().as_u16())
        .unwrap_or(rocket::http::Status::InternalServerError))
}

#[rocket::get("/api/dashboard/<lobby_room_id>/deathlink_probability")]
async fn get_deathlink_probability(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
        .get(format!("{}/api/{}/deathlink_probability", apx_api_root, lobby_room_id))
        .header("X-API-Key", apx_api_key)
        .send()
        .await?;

    let data: ProbabilityResponse = response.json().await?;
    Ok(Json(data))
}

#[rocket::post("/api/dashboard/<lobby_room_id>/deathlink_probability", data = "<request>")]
async fn set_deathlink_probability(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
        .post(format!("{}/api/{}/deathlink_probability", apx_api_root, lobby_room_id))
        .header("X-API-Key", apx_api_key)
        .json(&request.into_inner())
        .send()
        .await?;

    let data: ProbabilityResponse = response.json().await?;
    Ok(Json(data))
}

#[rocket::get("/api/dashboard/<lobby_room_id>/hint/<ty>/<slot_name>/<item_name>")]
async fn hint(
    _session: ModeratorSession,
    lobby_room_id: &str,
    ty: &str,
    slot_name: &str,
    item_name: &str,
    apx_room_info: ApxRoomInfo,
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

    ap_cmd(cmd, apx_room_info, config).await?;

    Ok(Redirect::to(format!("/dashboard/{}", lobby_room_id)))
}

#[rocket::get("/api/dashboard/<lobby_room_id>/give/<ty>/<slot_name>/<item_name>")]
async fn give(
    _session: ModeratorSession,
    lobby_room_id: &str,
    ty: &str,
    slot_name: &str,
    item_name: &str,
    apx_room_info: ApxRoomInfo,
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

    ap_cmd(cmd, apx_room_info, config).await?;

    Ok(Redirect::to(format!("/dashboard/{}", lobby_room_id)))
}

// Fix turn into apx route
async fn ap_cmd(cmd: String, apx_room_info: ApxRoomInfo, config: &State<Config>) -> crate::error::Result<()> {
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
    let ap_room_url = config
        .ap_api_root
        .join(&format!("/room/{}", apx_room_info.ap_room_id))?;
    let _ = client
        .post(ap_room_url)
        .multipart(form)
        .headers(headers)
        .send()
        .await?;

    Ok(())
}

#[rocket::get("/api/dashboard/<lobby_room_id>/release/<slot_name>")]
async fn release(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
        .join(&format!("/api/{}/release/", lobby_room_id))?;
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

    Ok(Redirect::to(format!("/dashboard/{}", lobby_room_id)))
}

#[allow(unused_variables)]
#[rocket::get("/api/dashboard/<lobby_room_id>/names/<type_name>/<game_name>")]
async fn get_game_names(
    _session: ModeratorSession,
    lobby_room_id: &str,
    type_name: &str,
    game_name: &str,
    config: &State<Config>,
) -> crate::error::Result<Json<Vec<String>>> {
    let apx_api_root = config
        .apx_api_root
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("APX API not configured"))?;
    let apx_api_key = config
        .apx_api_key
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("APX API key not configured"))?;

    let url = apx_api_root.join(&format!(
        "/api/{}/names/{}/{}",
        lobby_room_id, type_name, game_name
    ))?;

    let client = reqwest::Client::new();
    let result = client
        .get(url)
        .header("X-API-Key", apx_api_key)
        .send()
        .await?
        .error_for_status()
        .map_err(|e| anyhow!("APX returned error for game names: {}", e))?;

    let names: Vec<String> = result.json().await?;
    Ok(Json(names))
}

async fn notify_proxy_password_refresh(lobby_room_id: &str, config: &State<Config>) {
    let (Some(apx_root), Some(apx_key)) = (&config.apx_api_root, &config.apx_api_key) else {
        return;
    };

    let apx_url = match apx_root.join(&format!("/api/{}/refresh_passwords", lobby_room_id)) {
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

#[rocket::post("/api/dashboard/<lobby_room_id>/gen_all_passwords")]
async fn gen_all_passwords(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
        lobby_room_id
    ))?;

    let response = client
        .post(url)
        .headers(headers)
        .send()
        .await?;

    if !response.status().is_success() {
        Err(anyhow!("Failed to gen passwords: {}", response.status()))?;
    }

    notify_proxy_password_refresh(lobby_room_id, config).await;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(())
}

#[rocket::post("/api/dashboard/<lobby_room_id>/set_password/<yaml_id>", data = "<request>")]
async fn set_password(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
        lobby_room_id, yaml_id
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

    notify_proxy_password_refresh(lobby_room_id, config).await;

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

#[rocket::put("/api/dashboard/<lobby_room_id>/change_owner/<yaml_id>", data = "<request>")]
async fn change_yaml_owner(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
        lobby_room_id, yaml_id
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

    notify_proxy_password_refresh(lobby_room_id, config).await;

    // Invalidate tracker info cache
    *cache.0.lock().await = None;

    Ok(())
}

#[rocket::get("/dashboard/<lobby_room_id>/debug_slot/<slot_id>")]
async fn debug_slot_page(
    _session: ModeratorSession,
    lobby_room_id: &str,
    slot_id: i32,
) -> crate::error::Result<DebugSlotTpl> {
    Ok(DebugSlotTpl {
        room_id: lobby_room_id.to_string(),
        slot_id,
    })
}

#[rocket::get("/api/dashboard/<lobby_room_id>/debug/slot/<slot_id>")]
async fn debug_slot_tap(
    _session: ModeratorSession,
    lobby_room_id: &str,
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
        .join(&format!("/api/{}/debug/slot/{}", lobby_room_id, slot_id))?;

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

#[rocket::get("/")]
fn index() -> Redirect {
    Redirect::to("/rooms")
}

pub struct Config {
    pub lobby_root_url: Url,
    pub lobby_public_url: Option<String>,
    pub lobby_api_key: String,
    pub ap_api_root: Url,
    pub ap_room_host: String,
    pub ap_admin_api_key: String,
    pub apx_api_root: Option<Url>,
    pub apx_api_key: Option<String>,
}

pub struct TrackerInfoCache(pub Arc<tokio::sync::Mutex<Option<(Instant, Vec<MergedSlotInfo>)>>>);
pub struct ApRoomCache(pub Arc<Mutex<Option<(Instant, TrackerInfo)>>>);
pub struct RoomOwnerCache(pub Mutex<HashMap<Uuid, i64>>);
pub struct SlotMappingCache(pub Mutex<HashMap<String, BTreeMap<usize, String>>>);

const TRACKER_CACHE_TTL: Duration = Duration::from_secs(30);

#[rocket::main]
async fn main() -> crate::error::Result<()> {
    let _ = dotenvy::dotenv().ok();

    let lobby_root_url =
        std::env::var("LOBBY_ROOT_URL").expect("Provide a `LOBBY_ROOT_URL` env variable");
    let lobby_public_url = 
        std::env::var("LOBBY_PUBLIC_URL").ok();
    let lobby_api_key =
        std::env::var("LOBBY_API_KEY").expect("Provide a `LOBBY_API_KEY` env variable");
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

    let apx_api_root = std::env::var("APX_API_ROOT")
        .ok()
        .and_then(|s| s.parse().ok());
    let apx_api_key = std::env::var("APX_API_KEY").ok();

    let db_url = std::env::var("DATABASE_URL").expect("Provide a `DATABASE_URL` env variable");
    let db_pool = common::db::get_database_pool(&db_url, MIGRATIONS).await?;

    let mut config = Config {
        lobby_root_url: lobby_root_url.parse()?,
        lobby_public_url,
        lobby_api_key,
        ap_api_root,
        ap_room_host,
        ap_admin_api_key,
        apx_api_root,
        apx_api_key,
    };

    rocket::build()
        .mount(
            "/",
            routes![
                index,
                dashboard,
                deathlinks,
                proxy_add_exclusion,
                proxy_remove_exclusion,
                get_deathlink_probability,
                set_deathlink_probability,
                release,
                hint,
                get_game_names,
                give,
                get_password,
                set_password,
                gen_all_passwords,
                change_yaml_owner,
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
        .manage(RoomOwnerCache(Mutex::new(HashMap::new())))
        .manage(SlotMappingCache(Mutex::new(HashMap::new())))
        .attach(OAuth2::<Discord>::fairing("discord"))
        .launch()
        .await
        .unwrap();

    Ok(())
}
