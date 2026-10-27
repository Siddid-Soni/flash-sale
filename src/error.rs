use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use serde::Serialize;

/// Error type for every handler. Returning `Err(ApiError::...)` produces a JSON
/// body `{"message": "..."}` with the matching status code.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Unauthorized(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Gone(String),
    #[error("{0}")]
    TooManyRequests(String),
    #[error("{0}")]
    ServiceUnavailable(String),
    #[error("internal error")]
    Internal(String),
}

#[derive(Serialize)]
pub struct ErrorBody {
    pub message: String,
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::Gone(_) => StatusCode::GONE,
            ApiError::TooManyRequests(_) => StatusCode::TOO_MANY_REQUESTS,
            ApiError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        // Internal details are logged, never sent to the client
        if let ApiError::Internal(detail) = self {
            log::error!("{detail}");
        }
        HttpResponse::build(self.status_code()).json(ErrorBody { message: self.to_string() })
    }
}

impl From<diesel::result::Error> for ApiError {
    fn from(err: diesel::result::Error) -> Self {
        use diesel::result::{DatabaseErrorKind, Error};
        match err {
            Error::NotFound => ApiError::NotFound("not found".into()),
            Error::DatabaseError(DatabaseErrorKind::UniqueViolation, info) => {
                ApiError::Conflict(info.message().to_string())
            }
            other => ApiError::Internal(format!("database: {other}")),
        }
    }
}

impl From<diesel::r2d2::PoolError> for ApiError {
    fn from(err: diesel::r2d2::PoolError) -> Self {
        ApiError::Internal(format!("database pool: {err}"))
    }
}

impl From<redis::RedisError> for ApiError {
    fn from(err: redis::RedisError) -> Self {
        ApiError::Internal(format!("redis: {err}"))
    }
}

impl From<actix_web::error::BlockingError> for ApiError {
    fn from(err: actix_web::error::BlockingError) -> Self {
        ApiError::Internal(format!("blocking task: {err}"))
    }
}

/// A failed publish is a temporary condition: the client retries with the same key.
impl From<crate::queue::QueueError> for ApiError {
    fn from(err: crate::queue::QueueError) -> Self {
        log::error!("{err}");
        ApiError::ServiceUnavailable("the checkout queue is unavailable, retry shortly".into())
    }
}

impl From<crate::payment::PaymentError> for ApiError {
    fn from(err: crate::payment::PaymentError) -> Self {
        ApiError::Internal(err.to_string())
    }
}
