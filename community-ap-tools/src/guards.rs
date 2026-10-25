use std::{fmt::Display, str::FromStr, time::{Duration, Instant}};

use reqwest::header::{HeaderName, HeaderValue};
use rocket::{
    Request, http::Status, outcome::{IntoOutcome, try_outcome}, request::{FromRequest, Outcome},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ApRoomCache, Config};

#[derive(Deserialize, Debug)]
pub struct YamlInfo {
    pub id: Uuid,
    pub discord_handle: String,
    pub discord_id: i64,
    pub slot_number: usize,
    pub has_patch: bool,
}

#[derive(Deserialize, Debug)]
pub struct LobbyRoom {
    pub id: Uuid,
    pub name: String,
    pub author_id: i64,
    /// The Archipelago version the room is on. A lobby from before rooms had one doesn't say.
    #[serde(default)]
    pub ap_version: Option<String>,
    pub yamls: Vec<YamlInfo>,
}

#[derive(Serialize, Clone, Debug)]
pub struct MergedSlotInfo {
    pub id: usize,
    pub name: String,
    pub game: String,
    pub locations: usize,
    pub checks: usize,
    pub status: SlotStatus,
    pub last_activity: i64,
    pub lobby_slot_id: Uuid,
    pub discord_handle: String,
    pub discord_id: String,
    pub has_patch: bool,
    pub incomplete_sphere1: bool,
    pub deathlinks_sent: i32,
    pub deathlink_excluded: bool,
    pub bounce_tag_exclusions: Vec<String>,
    pub slot_bounces_excluded: bool,
    pub full_feed: bool,
}

pub struct ApRoom {
    pub tracker_info: TrackerInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrackerInfo {
    pub slots: Vec<SlotInfo>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum SlotStatus {
    Disconnected,
    Connected,
    Ready,
    Playing,
    GoalCompleted,
    Unknown(i32),
}

impl serde::Serialize for SlotStatus {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let n = match self {
            Self::Disconnected => 0,
            Self::Connected => 5,
            Self::Ready => 10,
            Self::Playing => 20,
            Self::GoalCompleted => 30,
            Self::Unknown(n) => *n,
        };
        serializer.serialize_i32(n)
    }
}

impl<'de> serde::Deserialize<'de> for SlotStatus {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let n = i32::deserialize(deserializer)?;
        Ok(match n {
            0 => Self::Unknown(0),
            5 => Self::Connected,
            10 => Self::Ready,
            20 => Self::Playing,
            30 => Self::GoalCompleted,
            n => Self::Unknown(n),
        })
    }
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

impl Display for SlotStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GoalCompleted => f.write_str("Goal Completed"),
            Self::Disconnected => f.write_str("Disconnected"),
            Self::Connected => f.write_str("Connected"),
            Self::Ready => f.write_str("Ready"),
            Self::Playing => f.write_str("Playing"),
            Self::Unknown(n) => write!(f, "Unknown ({n})"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotInfo {
    pub team: usize,
    pub slot: usize,
    pub name: String,
    pub game: String,
    pub locations: usize,
    pub checks: usize,
    pub status: SlotStatus,
    pub last_activity: i64,
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
            .unmounted()
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

const AP_ROOM_CACHE_TTL: Duration = Duration::from_secs(15);

#[derive(Deserialize, Debug)]
pub struct ApxRoomInfo {
    pub lobby_room_id: String,
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

        if result.status() == reqwest::StatusCode::NOT_FOUND {
            return Outcome::Error((
                Status::NotFound,
                anyhow::anyhow!("Room {} not found", lobby_room_id).into(),
            ));
        }

        let apx_room_info: ApxRoomInfo = try_err_outcome!(result.json().await);

        Outcome::Success(apx_room_info)
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ApRoom {
    type Error = crate::error::Error;

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let LobbyRoomId(lobby_room_id) = try_outcome!(LobbyRoomId::from_request(request).await);
        let lobby_room_id_str = lobby_room_id.to_string();
        let cache = request.rocket().state::<ApRoomCache>().unwrap();

        {
            let cached = cache.0.lock().unwrap();
            if let Some((fetched_at, tracker_info)) = cached.get(&lobby_room_id_str) {
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

        let apx_api_root = try_err_outcome!(
            config
                .apx_api_root
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("APX API not configured"))
        );

        let tracker_overview_url = try_err_outcome!(
            apx_api_root.join(&format!("/api/tracker/{}/overview", apx_room_info.lobby_room_id))
        );
        eprintln!("[GUARD] Fetching tracker overview from: {}", tracker_overview_url);
        let result = try_err_outcome!(reqwest::get(tracker_overview_url).await);
        let tracker_info: TrackerInfo = try_err_outcome!(result.json().await);

        cache.0.lock().unwrap().insert(lobby_room_id_str, (Instant::now(), TrackerInfo {
            slots: tracker_info.slots.clone(),
        }));

        Outcome::Success(ApRoom { tracker_info })
    }
}
