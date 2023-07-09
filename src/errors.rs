use uuid::Uuid;

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    GenericError,
    NotImplemented,
    DatabaseError,
    UrlError,

    TaskNotFound,
    UserNotFound,
    UserLimitExceeded,
    OptimisticConcurrencyRetryTransaction,
    OptimisticConcurrencyTooManyRetryAttempts,
    NotEnoughTasks,
}
impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ErrorCode::GenericError => "GenericError",
                ErrorCode::NotImplemented => "NotImplemented",
                ErrorCode::DatabaseError => "DatabaseError",
                ErrorCode::UrlError => "UrlError",

                ErrorCode::TaskNotFound => "TaskNotFound",
                ErrorCode::UserNotFound => "UserNotFound",
                ErrorCode::UserLimitExceeded => "UserLimitExceeded",
                ErrorCode::OptimisticConcurrencyRetryTransaction => "OCRetryTransaction",
                ErrorCode::OptimisticConcurrencyTooManyRetryAttempts => "OCTooManyRetryAttempts",
                ErrorCode::NotEnoughTasks => "NotEnoughTasks",
            }
        )
    }
}

#[derive(Debug, Clone)]
pub struct Error {
    code: ErrorCode,
    msg: String,
}
impl Error {
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn generic(msg: &str) -> Self {
        Error {
            code: ErrorCode::GenericError,
            msg: msg.to_string(),
        }
    }

    pub fn not_implemented(msg: &str) -> Self {
        Error {
            code: ErrorCode::NotImplemented,
            msg: msg.to_string(),
        }
    }

    pub fn db_error(msg: &str) -> Self {
        Error {
            code: ErrorCode::DatabaseError,
            msg: msg.to_string(),
        }
    }

    pub fn url_error(msg: &str) -> Self {
        Error {
            code: ErrorCode::UrlError,
            msg: format!("Unable to parse url: {}", msg),
        }
    }

    pub fn task_not_found(t_id: &Uuid) -> Self {
        Error {
            code: ErrorCode::TaskNotFound,
            msg: format!("task {} not found", t_id),
        }
    }

    pub fn user_not_found(u_id: &str) -> Self {
        Error {
            code: ErrorCode::UserNotFound,
            msg: format!("user {} not found", u_id),
        }
    }

    pub fn user_limit_exceeded(u_id: &str) -> Self {
        Error {
            code: ErrorCode::UserLimitExceeded,
            msg: format!(
                "user {} has reached the maximum number of votes per week",
                u_id
            ),
        }
    }

    pub fn retry_transaction() -> Self {
        Error {
            code: ErrorCode::OptimisticConcurrencyRetryTransaction,
            msg: "optimistic concurrency: retry transaction with new offset".to_string(),
        }
    }

    pub fn too_many_retry_attempts() -> Self {
        Error {
            code: ErrorCode::OptimisticConcurrencyTooManyRetryAttempts,
            msg: "optimistic concurrency: too many retry attempts, giving up".to_string(),
        }
    }

    pub fn not_enough_tasks() -> Self {
        Error {
            code: ErrorCode::NotEnoughTasks,
            msg: "not enough tasks to ask a meaningful question".to_string(),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error <{}> {}", self.code, &self.msg)
    }
}

use rusqlite;
use url;

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Error {
        Error::db_error(&err.to_string())
    }
}
impl From<url::ParseError> for Error {
    fn from(err: url::ParseError) -> Error {
        Error::url_error(&err.to_string())
    }
}
