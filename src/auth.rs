//! Password hashing (Argon2id), JWT issuing/validation (HS256), and the
//! `AuthUser` extractor.
//! Hashing and verifying take tens of milliseconds of CPU by design, so call
//! them inside `web::block`, never directly on an async worker.

use std::future::{ready, Ready};
use std::sync::OnceLock;

use actix_web::{dev::Payload, http::header, web, FromRequest, HttpRequest};
use argon2::{
    password_hash::{phc::PasswordHash, PasswordHasher, PasswordVerifier},
    Argon2,
};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{decode, encode, errors::ErrorKind, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use crate::{config::Config, error::ApiError};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    /// User id, as a string per the JWT spec
    pub sub: String,
    pub iat: i64,
    pub exp: i64,
}

pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    Ok(Argon2::default().hash_password(password.as_bytes())?.to_string())
}

/// Checks `password` against `stored_hash`. When the user does not exist,
/// pass `None`: a dummy hash is still verified, so the response takes as long
/// as for a real user and does not reveal which usernames exist.
pub fn verify_password(password: &str, stored_hash: Option<&str>) -> bool {
    static DUMMY_HASH: OnceLock<String> = OnceLock::new();
    let dummy = DUMMY_HASH.get_or_init(|| hash_password("dummy-password").expect("hashing a constant succeeds"));

    let hash = stored_hash.unwrap_or(dummy);
    let matches = PasswordHash::new(hash)
        .map(|parsed| Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok())
        .unwrap_or(false);
    matches && stored_hash.is_some()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_round_trip() {
        let hash = hash_password("correct horse").unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(verify_password("correct horse", Some(&hash)));
        assert!(!verify_password("wrong horse", Some(&hash)));
        assert!(!verify_password("dummy-password", None));
    }

    #[test]
    fn token_round_trip() {
        let (token, _) = issue_token(42, "secret", 60).unwrap();
        assert_eq!(verify_token(&token, "secret").unwrap(), 42);
        assert!(verify_token(&token, "other-secret").is_err());
        assert!(verify_token("not-a-token", "secret").is_err());
    }
}
