//! JWT issuing/validation (HS256) and the `AuthUser` extractor. Every user is
//! a guest (`POST /v1/session`), so there are no passwords; the user always
//! comes from the token, never from a request body.

use std::future::{ready, Ready};

use actix_web::{dev::Payload, http::header, web, FromRequest, HttpRequest};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{decode, encode, errors::ErrorKind, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::{config::Config, error::ApiError};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    /// User id, as a string per the JWT spec
    pub sub: String,
    pub iat: i64,
    pub exp: i64,
}

/// Returns a signed token for `user_id` and its expiry time.
pub fn issue_token(
    user_id: i32,
    secret: &str,
    ttl_secs: u64,
) -> Result<(String, DateTime<Utc>), jsonwebtoken::errors::Error> {
    let now = Utc::now();
    let expires_at = now + Duration::seconds(ttl_secs as i64);
    let claims = Claims { sub: user_id.to_string(), iat: now.timestamp(), exp: expires_at.timestamp() };
    let token = encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(secret.as_bytes()))?;
    Ok((token, expires_at))
}

/// Validates the signature and expiry and returns the user id.
pub fn verify_token(token: &str, secret: &str) -> Result<i32, jsonwebtoken::errors::Error> {
    let mut validation = Validation::new(Algorithm::HS256);
    // Tokens are issued and checked by our own servers, so no clock-skew allowance
    validation.leeway = 0;
    let data = decode::<Claims>(token, &DecodingKey::from_secret(secret.as_bytes()), &validation)?;
    data.claims
        .sub
        .parse()
        .map_err(|_| jsonwebtoken::errors::ErrorKind::InvalidSubject.into())
}

/// The authenticated caller. Add `user: AuthUser` to a handler to require a
/// valid `Authorization: Bearer <jwt>` header; otherwise the request is
/// rejected with `401` before the handler runs. Checking the token is a
/// signature check only, with no database lookup.
#[derive(Debug, Clone, Copy)]
pub struct AuthUser {
    pub id: i32,
}

impl FromRequest for AuthUser {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        ready(authenticate(req))
    }
}

fn authenticate(req: &HttpRequest) -> Result<AuthUser, ApiError> {
    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| ApiError::Unauthorized("missing bearer token".into()))?;

    let config = req
        .app_data::<web::Data<Config>>()
        .ok_or_else(|| ApiError::Internal("Config is not registered as app data".into()))?;

    match verify_token(token, &config.jwt_secret) {
        Ok(id) => Ok(AuthUser { id }),
        Err(err) if matches!(err.kind(), ErrorKind::ExpiredSignature) => {
            Err(ApiError::Unauthorized("token expired".into()))
        }
        Err(_) => Err(ApiError::Unauthorized("invalid token".into())),
    }
}

/// Marks an operator route (`/v1/admin/*`): requires the `X-Admin-Token`
/// header to equal `ADMIN_TOKEN`, otherwise `403` before the handler runs. The
/// comparison takes the same time wherever the strings differ, so the token
/// cannot be guessed byte by byte (only its length is observable). NGINX also
/// refuses `/v1/admin/` from outside the internal network (Stage 7).
#[derive(Debug, Clone, Copy)]
pub struct AdminAuth;

impl FromRequest for AdminAuth {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        ready(check_admin(req))
    }
}

fn check_admin(req: &HttpRequest) -> Result<AdminAuth, ApiError> {
    let config = req
        .app_data::<web::Data<Config>>()
        .ok_or_else(|| ApiError::Internal("Config is not registered as app data".into()))?;
    let given = req.headers().get("X-Admin-Token").map(|value| value.as_bytes()).unwrap_or_default();
    if tokens_match(given, config.admin_token.as_bytes()) {
        Ok(AdminAuth)
    } else {
        Err(ApiError::Forbidden("a valid X-Admin-Token header is required".into()))
    }
}

/// Constant-time equality; an empty expected token never matches, so an unset
/// token cannot be satisfied by a missing header.
fn tokens_match(given: &[u8], expected: &[u8]) -> bool {
    !expected.is_empty() && bool::from(given.ct_eq(expected))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trip() {
        let (token, _) = issue_token(42, "secret", 60).unwrap();
        assert_eq!(verify_token(&token, "secret").unwrap(), 42);
        assert!(verify_token(&token, "other-secret").is_err());
        assert!(verify_token("not-a-token", "secret").is_err());
    }

    #[test]
    fn admin_token_comparison() {
        assert!(tokens_match(b"secret", b"secret"));
        assert!(!tokens_match(b"secreT", b"secret"));
        assert!(!tokens_match(b"secret2", b"secret"));
        assert!(!tokens_match(b"", b"secret"));
        assert!(!tokens_match(b"", b""));
    }
}
