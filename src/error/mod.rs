use std::backtrace::Backtrace;
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug)]
pub enum ErrorKind {
    NotReadyError = 1,
    OutOfBoundsError = 2,
    UnsupportedNamedParameters = 3,
    MixedParametersTypes = 4,
    TypeMismatch = 5,

    PostgresTypeMismatch = 1001,

    UnknownError = -1,
}

pub struct Error {
    kind: ErrorKind,
    inner: Box<dyn std::error::Error + Send + Sync>,
    backtrace: Backtrace,
}

pub struct StdError(Error);

impl Display for StdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0.to_string(), f)
    }
}

impl Debug for StdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0.to_string(), f)
    }
}

impl std::error::Error for StdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.inner.as_ref())
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        Some(&self.0.backtrace)
    }
}

impl From<Error> for StdError {
    fn from(e: Error) -> Self {
        StdError(e)
    }
}

impl Error {
    pub fn new<E>(kind: ErrorKind, error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Error {
            kind,
            inner: error.into(),
            backtrace: Backtrace::capture(),
        }
    }

    pub fn not_ready() -> Self {
        Self::new(ErrorKind::NotReadyError, "Statement not ready")
    }
    pub fn type_mismatch() -> Self {
        Self::new(ErrorKind::TypeMismatch, "Type mismatch")
    }
    pub fn unsupported_named_parameters() -> Self {
        Self::new(
            ErrorKind::UnsupportedNamedParameters,
            "This driver does not support named parameters",
        )
    }
    pub fn mixed_parameters_types() -> Self {
        Self::new(
            ErrorKind::MixedParametersTypes,
            "Cannot mix named and positional parameters",
        )
    }

    pub fn out_of_bounds<T>(index: T) -> Self
    where
        T: ToString,
    {
        Self::new(
            ErrorKind::OutOfBoundsError,
            format!("Unable to read {} index", index.to_string()),
        )
    }

    pub fn postgres_type_mismatch() -> Self {
        Self::new(
            ErrorKind::PostgresTypeMismatch,
            "Type mismatch when converting parameters to postgres values.",
        )
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\nBacktrace:\n{}",
            self.inner,
            self.backtrace
        )
    }
}

impl<T> From<T> for Error
where
    T: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn from(err: T) -> Self {
        crate::error::Error::new(ErrorKind::UnknownError, err)
    }
}
