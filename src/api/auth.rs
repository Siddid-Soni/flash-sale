use actix_web::{get, post, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{actions, auth, auth::AuthUser, config::Config, db::DbPool, error::ApiError, models::NewUser};

#[derive(Debug, Deserialize)]
struct Credentials {
    username: String,
    password: String,
}

#[derive(Debug, Serialize)]
struct RegisterResponse {
    id: i32,
    username: String,
}

#[derive(Debug, Serialize)]
struct LoginResponse {
    token: String,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct SessionResponse {
    token: String,
    expires_at: DateTime<Utc>,
    user_id: i32,
}

const USERNAME_LEN: std::ops::RangeInclusive<usize> = 3..=50;
// The upper bound keeps a huge password from tying up a hashing thread
const PASSWORD_LEN: std::ops::RangeInclusive<usize> = 8..=128;

#[post("/auth/register")]
async fn register(pool: web::Data<DbPool>, body: web::Json<Credentials>) -> Result<HttpResponse, ApiError> {
    let body = body.into_inner();
    validate(&body)?;

    let user = web::block(move || {
        let password_hash =
            auth::hash_password(&body.password).map_err(|e| ApiError::Internal(format!("hash password: {e}")))?;
        let mut conn = pool.get()?;
        let new_user = NewUser { username: &body.username, password_hash: &password_hash };
        actions::users::create_user(&mut conn, &new_user).map_err(|err| match ApiError::from(err) {
            ApiError::Conflict(_) => ApiError::Conflict(format!("username '{}' is already taken", body.username)),
            other => other,
        })
    })
    .await??;

    Ok(HttpResponse::Created().json(RegisterResponse { id: user.id, username: user.username.unwrap_or_default() }))
}

#[post("/auth/login")]
async fn login(
    pool: web::Data<DbPool>,
    config: web::Data<Config>,
    body: web::Json<Credentials>,
) -> Result<HttpResponse, ApiError> {
    let body = body.into_inner();
    let invalid = || ApiError::Unauthorized("invalid username or password".into());
    if !PASSWORD_LEN.contains(&body.password.chars().count()) {
        return Err(invalid());
    }

    let user_id = web::block(move || {
        let mut conn = pool.get()?;
        let user = actions::users::find_by_username(&mut conn, &body.username)?;
        // Verified even when the user is missing, so both failures take equally long
        let valid = auth::verify_password(&body.password, user.as_ref().and_then(|u| u.password_hash.as_deref()));
        Ok::<_, ApiError>(user.filter(|_| valid).map(|u| u.id))
    })
    .await??
    .ok_or_else(invalid)?;

    let (token, expires_at) = auth::issue_token(user_id, &config.jwt_secret, config.jwt_ttl_secs)
        .map_err(|e| ApiError::Internal(format!("issue token: {e}")))?;

    Ok(HttpResponse::Ok().json(LoginResponse { token, expires_at }))
}

/// A guest session: a user row with no username or password, and a token for
/// it. Clients create it when the product page loads, before the sale opens, so
/// the 8 PM spike does not hit this insert; the gateway rate-limits it (Stage 11).
#[post("/session")]
async fn create_session(pool: web::Data<DbPool>, config: web::Data<Config>) -> Result<HttpResponse, ApiError> {
    let user = web::block(move || {
        let mut conn = pool.get()?;
        Ok::<_, ApiError>(actions::users::create_guest(&mut conn)?)
    })
    .await??;

    let (token, expires_at) = auth::issue_token(user.id, &config.jwt_secret, config.jwt_ttl_secs)
        .map_err(|e| ApiError::Internal(format!("issue token: {e}")))?;

    Ok(HttpResponse::Created().json(SessionResponse { token, expires_at, user_id: user.id }))
}

#[derive(Debug, Serialize)]
struct MeResponse {
    id: i32,
}

/// Returns the caller's user id; a client can use it to check its token.
#[get("/auth/me")]
async fn me(user: AuthUser) -> HttpResponse {
    HttpResponse::Ok().json(MeResponse { id: user.id })
}

fn validate(body: &Credentials) -> Result<(), ApiError> {
    let username = &body.username;
    if !USERNAME_LEN.contains(&username.chars().count())
        || !username.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(ApiError::BadRequest(
            "username must be 3-50 characters of letters, digits, '_', '-' or '.'".into(),
        ));
    }
    if !PASSWORD_LEN.contains(&body.password.chars().count()) {
        return Err(ApiError::BadRequest("password must be 8-128 characters".into()));
    }
    Ok(())
}

// POST /auth/register, POST /auth/login, POST /session, GET /auth/me
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(register).service(login).service(create_session).service(me);
}
