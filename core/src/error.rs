use async_backtrace::{backtrace, Location};
use diesel::result::{DatabaseErrorKind, Error};
use std::fmt::{Debug, Display};

/// DbError is a simplified representation of a diesel Error
/// It largely exists to make service code handling db errors
/// able to make business decisions without having to handle
/// the complexity of potentially every database failure in
/// interaction with the database.
#[derive(Derivative, thiserror::Error)]
#[derivative(Debug)]
pub enum DbError {
    #[error("db error: application error: {0}")]
    Application(anyhow::Error, #[derivative(Debug = "ignore")] Option<Box<[Location]>>),
    #[error("db error: database could not process request: {0}")]
    BadRequest(Error, #[derivative(Debug = "ignore")] Option<Box<[Location]>>),
    #[error("db error: bad request: {0}")]
    CustomBadRequest(anyhow::Error, #[derivative(Debug = "ignore")] Option<Box<[Location]>>),
    #[error("invalid db state")]
    InvalidDbState(anyhow::Error, #[derivative(Debug = "ignore")] Option<Box<[Location]>>),
    #[error("db error: network error while communiciating with database: {0}")]
    Network(Error, #[derivative(Debug = "ignore")] Option<Box<[Location]>>),
    #[error("db error: {0}")]
    Other(Error, #[derivative(Debug = "ignore")] Option<Box<[Location]>>),
    #[error("db error: application performed an invalid db operation: {0}")]
    Server(Error, #[derivative(Debug = "ignore")] Option<Box<[Location]>>),
    #[error("db error: could not commit transaction, another concurrent has locked affected rows")]
    Stale(#[derivative(Debug = "ignore")] Option<Box<[Location]>>),
    #[error("transaction cleanup error: {0}")]
    TxCleanup(#[from] TxCleanupError),
}

#[derive(Derivative, thiserror::Error)]
#[derivative(Debug)]
#[error("{source}")]
pub struct TxCleanupError {
    pub source: anyhow::Error,
    #[derivative(Debug = "ignore")]
    pub backtrace: Option<Box<[Location]>>,
}

impl DbError {
    #[framed]
    pub fn application<M: Debug + Display + Send + Sync + 'static>(msg: M) -> DbError {
        DbError::Application(anyhow::Error::msg(msg), backtrace())
    }
    #[framed]
    pub fn bad_request<M: Debug + Display + Send + Sync + 'static>(msg: M) -> DbError {
        DbError::CustomBadRequest(anyhow::Error::msg(msg), backtrace())
    }
    #[framed]
    pub fn invalid_db_state<M: Debug + Display + Send + Sync + 'static>(msg: M) -> DbError {
        DbError::InvalidDbState(anyhow::Error::msg(msg), backtrace())
    }
}

impl From<std::convert::Infallible> for DbError {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!()
    }
}

impl From<Error> for DbError {
    #[framed]
    fn from(error: Error) -> Self {
        match &error {
            Error::InvalidCString(_) => DbError::BadRequest(error, backtrace()),
            Error::DatabaseError(kind, _) => match kind {
                DatabaseErrorKind::UniqueViolation => DbError::BadRequest(error, backtrace()),
                DatabaseErrorKind::ForeignKeyViolation => DbError::BadRequest(error, backtrace()),
                DatabaseErrorKind::UnableToSendCommand => DbError::BadRequest(error, backtrace()),
                DatabaseErrorKind::ReadOnlyTransaction => DbError::Server(error, backtrace()),
                DatabaseErrorKind::NotNullViolation => DbError::BadRequest(error, backtrace()),
                DatabaseErrorKind::CheckViolation => DbError::BadRequest(error, backtrace()),
                DatabaseErrorKind::ClosedConnection => DbError::Network(error, backtrace()),
                // assume any other database errors are the result
                // of a raised exception implying a bad request
                _ => DbError::BadRequest(error, backtrace()),
            },
            Error::NotFound => DbError::BadRequest(error, backtrace()),
            Error::QueryBuilderError(_) => DbError::Server(error, backtrace()),
            Error::DeserializationError(_) => DbError::Server(error, backtrace()),
            Error::SerializationError(_) => DbError::Server(error, backtrace()),
            Error::AlreadyInTransaction => DbError::Server(error, backtrace()),
            Error::NotInTransaction => DbError::Server(error, backtrace()),
            _ => DbError::Other(error, backtrace()),
        }
    }
}

impl From<DbError> for Option<Error> {
    fn from(db_error: DbError) -> Self {
        match db_error {
            DbError::Application(_, _) => None,
            DbError::BadRequest(error, _) => Some(error),
            DbError::CustomBadRequest(_, _) => None,
            DbError::InvalidDbState(_, _) => None,
            DbError::Network(error, _) => Some(error),
            DbError::Other(error, _) => Some(error),
            DbError::Server(error, _) => Some(error),
            DbError::Stale(_) => None,
            DbError::TxCleanup(_) => None,
        }
    }
}

impl TxCleanupError {
    #[framed]
    pub fn new<S: Display + Debug + Send + Sync + 'static>(msg: S) -> Self {
        Self {
            source: anyhow::Error::msg(msg),
            backtrace: backtrace(),
        }
    }
}

impl From<anyhow::Error> for TxCleanupError {
    #[framed]
    fn from(source: anyhow::Error) -> Self {
        Self {
            source,
            backtrace: backtrace(),
        }
    }
}

impl From<TxCleanupError> for Error {
    fn from(value: TxCleanupError) -> Self {
        error!("transaction cleanup error occurred within a transaction whose error type is diesel::result::Error, converting error to diesel::result::Error::RollbackTransaction");
        error!("original error: {value}");
        Self::RollbackTransaction
    }
}
