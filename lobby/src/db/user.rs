use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel::{Insertable, Queryable};
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::Result;
use crate::schema::discord_users;

#[derive(Insertable, Queryable)]
#[diesel(table_name=discord_users)]
pub struct DiscordUser {
    pub id: i64,
    pub username: String,
    pub room_creation_allowed: bool,
}

#[tracing::instrument(skip(conn))]
pub async fn get_username(user_id: i64, conn: &mut AsyncPgConnection) -> Result<Option<String>> {
    let username = discord_users::table
        .filter(discord_users::id.eq(user_id))
        .select(discord_users::username)
        .first::<String>(conn)
        .await
        .optional()?;

    Ok(username)
}

#[tracing::instrument(skip(conn))]
pub async fn ensure_user_exists(user_id: i64, conn: &mut AsyncPgConnection) -> Result<()> {
    let user = DiscordUser {
        id: user_id,
        username: "unknown".to_string(),
        room_creation_allowed: false,
    };

    diesel::insert_into(discord_users::table)
        .values(&user)
        .on_conflict(discord_users::id)
        .do_nothing()
        .execute(conn)
        .await?;

    Ok(())
}

#[tracing::instrument(skip(conn, discord_id), fields(%discord_id))]
pub async fn upsert_discord_user(
    discord_id: i64,
    username: &str,
    conn: &mut AsyncPgConnection,
) -> Result<()> {
    let discord_user = DiscordUser {
        id: discord_id,
        username: username.to_string(),
        room_creation_allowed: false,
    };

    diesel::insert_into(discord_users::table)
        .values(&discord_user)
        .on_conflict(discord_users::id)
        .do_update()
        .set(discord_users::username.eq(username))
        .execute(conn)
        .await?;

    Ok(())
}

#[tracing::instrument(skip(conn))]
pub async fn get_room_creation_allowed_users(
    conn: &mut AsyncPgConnection,
) -> Result<Vec<DiscordUser>> {
    let users = discord_users::table
        .filter(discord_users::room_creation_allowed.eq(true))
        .load::<DiscordUser>(conn)
        .await?;

    Ok(users)
}

#[tracing::instrument(skip(conn))]
pub async fn get_room_creation_allowed(user_id: i64, conn: &mut AsyncPgConnection) -> Result<bool> {
    let allowed = discord_users::table
        .filter(discord_users::id.eq(user_id))
        .select(discord_users::room_creation_allowed)
        .first::<bool>(conn)
        .await
        .optional()?
        .unwrap_or(false);

    Ok(allowed)
}

#[tracing::instrument(skip(conn))]
pub async fn set_room_creation_allowed(
    discord_id: i64,
    allowed: bool,
    conn: &mut AsyncPgConnection,
) -> Result<()> {
    let rows = diesel::update(discord_users::table.filter(discord_users::id.eq(discord_id)))
        .set(discord_users::room_creation_allowed.eq(allowed))
        .execute(conn)
        .await?;

    if rows == 0 {
        return Err(anyhow::anyhow!("User {} not found", discord_id).into());
    }

    Ok(())
}
