use actix_web::{post, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{actions, auth, config::Config, db::DbPool, error::ApiError};

#[derive(Debug, Serialize)]
struct SessionResponse {
    token: String,
    expires_at: DateTime<Utc>,
    user_id: i32,
}

/// A guest session: a user row with no username or email, and a token for it.
/// Clients create it when the sale page loads, before the sale opens, so the
/// spike at the start does not hit this insert; the gateway rate-limits it
/// per IP (Stage 7).
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

// POST /v1/session
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(create_session);
}
