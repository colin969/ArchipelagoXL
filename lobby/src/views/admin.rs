use crate::db::{self, DiscordUser};
use crate::error::Result;
use crate::session::LoggedInSession;
use crate::{Context, LobbyConfig, TplContext};
use askama::Template;
use askama_web::WebTemplate;
use rocket::{get, State};

// --- Template structs ---

#[derive(Template, WebTemplate)]
#[template(path = "admin/main.html")]
pub struct AdminTpl<'a> {
    base: TplContext<'a>,
}

#[derive(Template, WebTemplate)]
#[template(path = "admin/roomcreation.html")]
pub struct RoomCreationTpl<'a> {
    base: TplContext<'a>,
    allowed_users: Vec<DiscordUser>,
}

// --- Data types ---

#[get("/admin")]
#[tracing::instrument(skip(ctx, session, lobby_config))]
pub async fn admin<'a>(
    ctx: &State<Context>,
    session: LoggedInSession,
    lobby_config: &State<LobbyConfig>,
) -> Result<AdminTpl<'a>> {
    if !session.is_admin() {
        return Err(anyhow::anyhow!("Forbidden").into());
    }
    Ok(AdminTpl {
        base: TplContext::from_session("admin", session.0, ctx, lobby_config, Some("Admin".to_owned())).await,
    })
}

#[get("/admin/room_creation")]
#[tracing::instrument(skip(ctx, session, lobby_config))]
pub async fn room_creation<'a>(
    session: LoggedInSession,
    ctx: &State<Context>,
    lobby_config: &State<LobbyConfig>,
) -> Result<RoomCreationTpl<'a>> {
    if !session.is_admin() {
        return Err(anyhow::anyhow!("Forbidden").into());
    }
    let mut conn = ctx.db_pool.get().await?;
    let allowed_users = db::get_room_creation_allowed_users(&mut conn).await?;

    Ok(RoomCreationTpl {
        base: TplContext::from_session("admin", session.0, ctx, lobby_config, Some("Admin - Room Creation".to_owned())).await,
        allowed_users,
    })
}

// --- Route registration ---

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![admin, room_creation,]
}
