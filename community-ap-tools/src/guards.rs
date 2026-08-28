use std::{collections::BTreeMap, fmt::Display, str::FromStr, time::{Duration, Instant}};

use reqwest::header::{HeaderName, HeaderValue};
use rocket::{
    Request, http::Status, outcome::{IntoOutcome, try_outcome}, request::{FromRequest, Outcome},
};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ApRoomCache, Config, SlotMappingCache};
use crate::datapackage::DataPackage;

#[derive(Deserialize, Debug)]
pub struct YamlInfo {
    pub id: Uuid,
    pub discord_handle: String,
    pub discord_id: i64,
    pub slot_number: usize,
    pub has_patch: bool,
}

#[derive(Deserialize, Debug)]
pub struct SlotPasswordInfo {
    pub password: Option<String>,
}

#[derive(Debug)]
pub struct SlotPasswords(pub Vec<SlotPasswordInfo>);

#[derive(Deserialize, Debug)]
pub struct LobbyRoom {
    pub id: Uuid,
    pub name: String,
    pub author_id: i64,
    pub yamls: Vec<YamlInfo>,
}

#[derive(Deserialize, Debug)]
pub struct RoomStatus {
    pub tracker: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct MergedSlotInfo {
    pub id: usize,
    pub name: String,
    pub game: String,
    pub checks: (u64, u64),
    pub status: SlotStatus,
    pub last_activity: Option<f64>,
    pub lobby_slot_id: Uuid,
    pub discord_handle: String,
    pub discord_id: String,
    pub has_patch: bool,
    pub incomplete_sphere1: bool,
    pub deathlinks_sent: i32,
    pub deathlink_excluded: bool,
    pub full_feed: bool,
}

pub struct ApRoom {
    pub tracker_info: TrackerInfo,
}

#[derive(Debug, Serialize)]
pub struct TrackerInfo {
    pub slots: Vec<SlotInfo>,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub enum SlotStatus {
    Disconnected,
    Connected,
    Ready,
    Playing,
    GoalCompleted,
    Unknown(String),
}

impl SlotStatus {
    fn sort_key(&self) -> u8 {
        match self {
            Self::Unknown(_) => 0,
            Self::Disconnected => 1,
            Self::Connected | Self::Ready | Self::Playing => 2,
            Self::GoalCompleted => 3,
        }
    }
}

impl PartialOrd for SlotStatus {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SlotStatus {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort_key().cmp(&other.sort_key())
    }
}

impl FromStr for SlotStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "Goal Completed" => Self::GoalCompleted,
            "Disconnected" => Self::Disconnected,
            "Connected" => Self::Connected,
            "Ready" => Self::Ready,
            "Playing" => Self::Playing,
            _ => Self::Unknown(s.to_string()),
        })
    }
}

impl Display for SlotStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GoalCompleted => f.write_str("Goal Completed"),
            Self::Disconnected => f.write_str("Disconnected"),
            Self::Connected => f.write_str("Connected"),
            Self::Ready => f.write_str("Ready"),
            Self::Playing => f.write_str("Playing"),
            Self::Unknown(s) => f.write_fmt(format_args!("Unknown ({s})")),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SlotInfo {
    pub id: usize,
    pub name: String,
    pub game: String,
    pub checks: (u64, u64),
    pub status: SlotStatus,
    pub last_activity: Option<f64>,
}

macro_rules! try_err_outcome {
    ($e: expr) => {
        try_outcome!(
            $e.map_err(|e| {
                eprintln!("[GUARD ERROR] {}: {:?}", stringify!($e), e);
                e.into()
            })
            .or_error(Status::InternalServerError)
        )
    };
}

pub struct LobbyRoomId(pub Uuid);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for LobbyRoomId {
    type Error = crate::error::Error;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let route = match request.route() {
            Some(route) => route,
            None => return Outcome::Error((
                Status::BadRequest,
                anyhow::anyhow!("Why is there no route?").into(),
            )),
        };

        // Find index of segment in request that'll contain this routes <lobby_room_id> value
        // This feels hacky but if it works it works
        let param_index = route
            .uri
            .unmounted_origin
            .path()
            .split('/')
            .filter(|s| !s.is_empty())
            .enumerate()
            .find_map(|(i, seg)| (seg == "<lobby_room_id>").then_some(i));

        match param_index.and_then(|i| request.param::<Uuid>(i)?.ok()) {
            Some(id) => Outcome::Success(LobbyRoomId(id)),
            None => Outcome::Error((
                Status::BadRequest,
                anyhow::anyhow!("Missing or invalid lobby_room_id in path").into(),
            )),
        }
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for LobbyRoom {
    type Error = crate::error::Error;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let LobbyRoomId(lobby_room_id) = try_outcome!(LobbyRoomId::from_request(request).await);
        let config = request.rocket().state::<Config>().unwrap();

        let url = try_err_outcome!(
            config
                .lobby_root_url
                .join(&format!("/api/room/{}", lobby_room_id))
        );
        let client = reqwest::Client::new();
        let result = try_err_outcome!(
            client
                .get(url)
                .header(
                    HeaderName::from_static("x-api-key"),
                    HeaderValue::from_str(&config.lobby_api_key).unwrap(),
                )
                .send()
                .await
        );
        let mut room: LobbyRoom = try_err_outcome!(result.json().await);
        room.yamls.sort_by_key(|yaml| yaml.slot_number);

        Outcome::Success(room)
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for SlotPasswords {
    type Error = crate::error::Error;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let LobbyRoomId(lobby_room_id) = try_outcome!(LobbyRoomId::from_request(request).await);
        let config = request.rocket().state::<Config>().unwrap();

        let url = try_err_outcome!(
            config
                .lobby_root_url
                .join(&format!("/api/room/{}/slots_passwords", lobby_room_id))
        );
        let client = reqwest::Client::new();
        let result = try_err_outcome!(
            client
                .get(url)
                .header(
                    HeaderName::from_static("x-api-key"),
                    HeaderValue::from_str(&config.lobby_api_key).unwrap(),
                )
                .send()
                .await
        );
        let passwords: Vec<SlotPasswordInfo> = try_err_outcome!(result.json().await);

        Outcome::Success(SlotPasswords(passwords))
    }
}

#[derive(Deserialize)]
pub struct ApMsg {
    #[serde(flatten)]
    data: DataPackage,
}
#[derive(Deserialize)]
pub struct DPackage(Vec<ApMsg>);

const AP_ROOM_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Deserialize, Debug)]
pub struct ApxRoomInfo {
    pub lobby_room_id: String,
    pub ap_room_id: String,
    pub normal_id: i64,
    pub reduced_id: i64,
    pub disabled: bool,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ApxRoomInfo {
    type Error = crate::error::Error;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let LobbyRoomId(lobby_room_id) = try_outcome!(LobbyRoomId::from_request(request).await);
        let config = request.rocket().state::<Config>().unwrap();

        let apx_api_root = try_err_outcome!(
            config
                .apx_api_root
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("APX API not configured"))
        );
        let apx_api_key = try_err_outcome!(
            config
                .apx_api_key
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("APX API key not configured"))
        );

        let apx_room_url = try_err_outcome!(
            apx_api_root.join(&format!("/api/room/{}", lobby_room_id))
        );
        eprintln!("[GUARD] Fetching APX room info from: {}", apx_room_url);

        let client = reqwest::Client::new();
        let result = try_err_outcome!(
            client
                .get(apx_room_url)
                .header("X-API-Key", apx_api_key)
                .send()
                .await
        );
        let apx_room_info: ApxRoomInfo = try_err_outcome!(result.json().await);

        Outcome::Success(apx_room_info)
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ApRoom {
    type Error = crate::error::Error;
    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let cache = request.rocket().state::<ApRoomCache>().unwrap();

        // Return cached value if still fresh
        {
            let cached = cache.0.lock().unwrap();
            if let Some((fetched_at, ref tracker_info)) = *cached {
                if fetched_at.elapsed() < AP_ROOM_CACHE_TTL {
                    return Outcome::Success(ApRoom {
                        tracker_info: TrackerInfo {
                            slots: tracker_info.slots.clone(),
                        },
                    });
                }
            }
        }

        let apx_room_info = try_outcome!(ApxRoomInfo::from_request(request).await);
        let config = request.rocket().state::<Config>().unwrap();
        let slot_mapping_cache = request.rocket().state::<SlotMappingCache>().unwrap();

        let room_status_url = try_err_outcome!(
            config
                .ap_api_root
                .join(&format!("/api/room_status/{}", apx_room_info.ap_room_id))
        );
        eprintln!("[GUARD] Fetching room status from: {}", room_status_url);
        let result = try_err_outcome!(reqwest::get(room_status_url).await);
        let room_status: RoomStatus = try_err_outcome!(result.json().await);

        // Fetch slot mapping if not cached for this ap room
        {
            let has_mapping = slot_mapping_cache
                .0
                .lock()
                .unwrap()
                .contains_key(&apx_room_info.ap_room_id);

            if !has_mapping {
                let ap_room_url = try_err_outcome!(
                    config
                        .ap_api_root
                        .join(&format!("/room/{}", apx_room_info.ap_room_id))
                );
                let response = try_err_outcome!(reqwest::get(ap_room_url).await);
                let body = try_err_outcome!(response.text().await);
                let slots = try_err_outcome!(parse_room(body));
                slot_mapping_cache
                    .0
                    .lock()
                    .unwrap()
                    .insert(apx_room_info.ap_room_id.clone(), slots);
            }
        }

        let tracker_url = try_err_outcome!(
            config
                .ap_api_root
                .join(&format!("/tracker/{}", room_status.tracker))
        );
        eprintln!("[GUARD] Fetching tracker from: {}", tracker_url);
        let tracker_page = try_err_outcome!(reqwest::get(tracker_url).await);
        let tracker_body = try_err_outcome!(tracker_page.text().await);

        let tracker_info = {
            let mapping_guard = slot_mapping_cache.0.lock().unwrap();
            let slot_map = mapping_guard.get(&apx_room_info.ap_room_id).unwrap();
            try_err_outcome!(parse_tracker(tracker_body, slot_map))
        };

        *cache.0.lock().unwrap() = Some((Instant::now(), TrackerInfo {
            slots: tracker_info.slots.clone(),
        }));

        Outcome::Success(ApRoom { tracker_info })
    }
}

fn parse_room(body: String) -> crate::error::Result<BTreeMap<usize, String>> {
    let mut slots = BTreeMap::new();
    let html = Html::parse_document(&body);
    let slot_lines_selector = Selector::parse("#slots-table > tbody > tr").unwrap();
    let slot_lines = html.select(&slot_lines_selector);
    let td_selector = Selector::parse("td").unwrap();
    let a_selector = Selector::parse("a").unwrap();

    for slot_line in slot_lines {
        let mut cells = slot_line.select(&td_selector);
        let slot_id = cells.next().unwrap().inner_html().trim().parse::<usize>()?;
        let slot_name = htmlize::unescape(
            cells
                .next()
                .unwrap()
                .select(&a_selector)
                .next()
                .unwrap()
                .inner_html()
                .trim()
                .to_string(),
        );

        slots.insert(slot_id, slot_name.to_string());
    }

    Ok(slots)
}

fn parse_tracker(body: String, slot_map: &BTreeMap<usize, String>) -> crate::error::Result<TrackerInfo> {
    let mut slots = Vec::new();
    let html = Html::parse_document(&body);
    let slot_lines_selector = Selector::parse("#checks-table > tbody > tr").unwrap();
    let td_selector = Selector::parse("td").unwrap();
    let a_selector = Selector::parse("a").unwrap();
    let slot_lines = html.select(&slot_lines_selector);

    for slot_line in slot_lines {
        let mut cells = slot_line.select(&td_selector);

        let slot_id = cells
            .next()
            .unwrap()
            .select(&a_selector)
            .next()
            .unwrap()
            .inner_html()
            .trim()
            .parse::<usize>()?;
        let _ = cells.next();
        let slot_name = slot_map.get(&slot_id).unwrap();
        let slot_game = htmlize::unescape(cells.next().unwrap().inner_html().trim().to_string());
        let status = cells.next().unwrap().inner_html().trim().to_string();
        let checks = cells
            .next()
            .unwrap()
            .inner_html()
            .trim()
            .to_string()
            .split_once('/')
            .map(|(v1, v2)| (v1.parse::<u64>().unwrap(), v2.parse::<u64>().unwrap()))
            .unwrap();
        let _percent = cells.next();
        let last_activity = cells
            .next()
            .unwrap()
            .inner_html()
            .trim()
            .to_string()
            .parse::<f64>()
            .ok();

        slots.push(SlotInfo {
            id: slot_id,
            name: slot_name.to_string(),
            game: slot_game.to_string(),
            status: status.parse().unwrap(),
            checks,
            last_activity,
        });
    }

    Ok(TrackerInfo { slots })
}
