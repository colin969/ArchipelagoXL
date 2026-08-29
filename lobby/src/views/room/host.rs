use askama::Template;
use askama_web::WebTemplate;
use rocket::{FromForm, State};
use rocket::form::Form;

use crate::{Context, LobbyConfig, TplContext, db::{self, Generation, GenerationStatus, Room, RoomId}, session::LoggedInSession};
use crate::error::Result;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ApxRoomInfo {
    pub lobby_room_id: String,
    pub ap_room_id: String,
    pub normal_id: u16,
    pub reduced_id: u16,
    pub disabled: bool,
    pub per_slot_passwords: bool,
    pub deathlink_disabled: bool,
    pub reduced_access: bool,
}

#[derive(Debug, Clone)]
pub struct ApxRoomInfoDisplay {
    pub lobby_room_id: String,
    pub ap_room_id: String,
    pub normal_addr: String,
    pub reduced_addr: String,
    pub disabled: bool,
    pub per_slot_passwords: bool,
    pub deathlink_disabled: bool,
    pub reduced_access: bool,
}

#[derive(Template, WebTemplate)]
#[template(path = "room/host.html")]
struct HostRoomTpl<'a> {
    base: TplContext<'a>,
    room: Room,
    current_gen: Option<Generation>,
    apx_room_info: Option<ApxRoomInfoDisplay>,
    ap_tools_public_url: String,
}


#[rocket::get("/room/<room_id>/host")]
#[tracing::instrument(skip(session, ctx))]
async fn host_room<'a>(
    room_id: RoomId,
    session: LoggedInSession,
    ctx: &'a State<Context>,
    lobby_config: &State<LobbyConfig>,
) -> Result<HostRoomTpl<'a>> {
    let mut conn = ctx.db_pool.get().await?;
    let room = db::get_room(room_id, &mut conn).await?;
    let is_my_room = session.0.is_admin || session.user_id() == room.settings.author_id;

    if !is_my_room {
        Err(anyhow::anyhow!(
            "Cannot access host page for a room that isn't yours"
        ))?
    }

    let current_gen = db::get_generation_for_room(room_id, &mut conn).await?;
    let apx_room_info = fetch_apx_room_info(
      &lobby_config.apx_root,
      &lobby_config.apx_api_key,
      &room.id.to_string(),
    )
    .await
    .unwrap_or(None)
    .map(|info| ApxRoomInfoDisplay {
        lobby_room_id: info.lobby_room_id,
        ap_room_id: info.ap_room_id,
        normal_addr: format!("ap{}.{}", info.normal_id, lobby_config.apx_ws_root),
        reduced_addr: format!("ap{}.{}", info.reduced_id, lobby_config.apx_ws_root),
        disabled: info.disabled,
        per_slot_passwords: info.per_slot_passwords,
        deathlink_disabled: info.deathlink_disabled,
        reduced_access: info.reduced_access,
    });


    Ok(HostRoomTpl {
        base: TplContext::from_session("room", session.0, ctx, lobby_config, Some(format!("{} - Host", room.settings.name))).await,
        room,
        current_gen,
        apx_room_info,
        ap_tools_public_url: lobby_config.ap_tools_public_url.clone(),
    })
}

#[derive(FromForm)]
pub struct HostStartForm {
    pub per_slot_passwords: Option<bool>,
    pub deathlink_disabled: Option<bool>,
    pub reduced_access: Option<bool>,
}

#[rocket::post("/room/<room_id>/host/start", data = "<form>")]
#[tracing::instrument(skip(session, ctx, lobby_config, generation_out_dir, form))]
async fn host_room_start(
    room_id: RoomId,
    session: LoggedInSession,
    ctx: &State<Context>,
    lobby_config: &State<LobbyConfig>,
    generation_out_dir: &State<crate::jobs::GenerationOutDir>,
    form: Form<HostStartForm>,
) -> Result<rocket::response::Redirect> {
    let mut conn = ctx.db_pool.get().await?;
    let room = db::get_room(room_id, &mut conn).await?;
    let is_my_room = session.0.is_admin || session.user_id() == room.settings.author_id;

    if !is_my_room {
        Err(anyhow::anyhow!("Cannot host a room that isn't yours"))?
    }

    let per_slot_passwords = form.per_slot_passwords.unwrap_or(false);
    let deathlink_disabled = form.deathlink_disabled.unwrap_or(false);
    let reduced_access = form.reduced_access.unwrap_or(false);

    let client = reqwest::Client::new();

    if fetch_apx_room_info(&lobby_config.apx_root, &lobby_config.apx_api_key, &room.id.to_string())
        .await
        .unwrap_or(None)
        .is_some() {
        let resp = client
            .post(format!("{}/api/room/{}/start", lobby_config.apx_root, room_id))
            .header("X-API-Key", &lobby_config.apx_api_key)
            .json(&serde_json::json!({
                "per_slot_passwords": per_slot_passwords,
                "deathlink_disabled": deathlink_disabled,
                "reduced_access": reduced_access,
            }))
            .send()
            .await?;

        if !resp.status().is_success() {
            let err: serde_json::Value = resp.json().await.unwrap_or_default();
            Err(anyhow::anyhow!("APX error: {}", err["error"].as_str().unwrap_or(&err.to_string())))?
        }

        return Ok(rocket::response::Redirect::to(format!("/room/{}/host", room_id)));
    }

    let gen = db::get_generation_for_room(room_id, &mut conn)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No generation found for room"))?;

    if gen.status != GenerationStatus::Done {
        Err(anyhow::anyhow!("Generation is not complete"))?
    }

    let generation_info = crate::generation::get_generation_info(gen.job_id, &generation_out_dir.inner().0)?;
    let output_path = generation_info
        .output_file
        .ok_or_else(|| anyhow::anyhow!("No output file for generation"))?;

    let complete_out_path = generation_out_dir
        .inner()
        .0
        .join(gen.job_id.to_string())
        .join(&output_path);

    let file_bytes = tokio::fs::read(&complete_out_path).await?;
    let filename = complete_out_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("output.zip")
        .to_string();

    let part = reqwest::multipart::Part::bytes(file_bytes)
        .file_name(filename)
        .mime_str("application/zip")?;
    let multipart = reqwest::multipart::Form::new()
        .part("file", part)
        .text("lobby_room_id", room.id.to_string())
        .text("per_slot_passwords", per_slot_passwords.to_string())
        .text("deathlink_disabled", deathlink_disabled.to_string())
        .text("reduced_access", reduced_access.to_string());

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/room", lobby_config.apx_root))
        .header("X-API-Key", &lobby_config.apx_api_key)
        .multipart(multipart)
        .send()
        .await?;

    if !resp.status().is_success() {
        let err: serde_json::Value = resp.json().await.unwrap_or_default();
        Err(anyhow::anyhow!("APX error: {}", err["error"].as_str().unwrap_or(&err.to_string())))?
    }

    Ok(rocket::response::Redirect::to(format!("/room/{}/host", room_id)))
}

#[rocket::get("/room/<room_id>/host/stop")]
#[tracing::instrument(skip(session, ctx, lobby_config))]
async fn host_room_stop(
    room_id: RoomId,
    session: LoggedInSession,
    ctx: &State<Context>,
    lobby_config: &State<LobbyConfig>,
) -> Result<rocket::response::Redirect> {
    let mut conn = ctx.db_pool.get().await?;
    let room = db::get_room(room_id, &mut conn).await?;
    let is_my_room = session.0.is_admin || session.user_id() == room.settings.author_id;

    if !is_my_room {
        Err(anyhow::anyhow!("Cannot stop a room that isn't yours"))?
    }

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/room/{}/stop", lobby_config.apx_root, room_id))
        .header("X-API-Key", &lobby_config.apx_api_key)
        .send()
        .await?;

    if !resp.status().is_success() {
        let err: serde_json::Value = resp.json().await.unwrap_or_default();
        Err(anyhow::anyhow!("APX error: {}", err["error"].as_str().unwrap_or(&err.to_string())))?
    }

    Ok(rocket::response::Redirect::to(format!("/room/{}/host", room_id)))
}

#[rocket::get("/room/<room_id>/host/delete")]
#[tracing::instrument(skip(session, ctx, lobby_config))]
async fn host_room_delete(
    room_id: RoomId,
    session: LoggedInSession,
    ctx: &State<Context>,
    lobby_config: &State<LobbyConfig>,
) -> Result<rocket::response::Redirect> {
    let mut conn = ctx.db_pool.get().await?;
    let room = db::get_room(room_id, &mut conn).await?;
    let is_my_room = session.0.is_admin || session.user_id() == room.settings.author_id;

    if !is_my_room {
        Err(anyhow::anyhow!("Cannot delete a room that isn't yours"))?
    }

    let client = reqwest::Client::new();
    let resp = client
        .delete(format!("{}/api/room/{}", lobby_config.apx_root, room_id))
        .header("X-API-Key", &lobby_config.apx_api_key)
        .send()
        .await?;

    if !resp.status().is_success() {
        let err: serde_json::Value = resp.json().await.unwrap_or_default();
        Err(anyhow::anyhow!("APX error: {}", err["error"].as_str().unwrap_or(&err.to_string())))?
    }

    Ok(rocket::response::Redirect::to(format!("/room/{}/host", room_id)))
}

async fn fetch_apx_room_info(
  apx_root: &str,
  apx_api_key: &str,
  lobby_room_id: &str,
) -> anyhow::Result<Option<ApxRoomInfo>> {
  let client = reqwest::Client::new();
  let resp = client
      .get(format!("{}/api/room/{}", apx_root, lobby_room_id))
      .header("X-API-Key", apx_api_key)
      .send()
      .await?;

  if resp.status() == reqwest::StatusCode::NOT_FOUND {
      return Ok(None);
  }

  Ok(Some(resp.error_for_status()?.json::<ApxRoomInfo>().await?))
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![
        host_room,
        host_room_start,
        host_room_stop,
        host_room_delete,
    ]
}
