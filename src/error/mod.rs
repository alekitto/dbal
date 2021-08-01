use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum ErrorKind {
    NotReadyError = 1,
    OutOfBoundsError = 2,

    UnknownError = -1,
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    inner: Box<dyn std::error::Error + Send + Sync>,
}

impl Error {
    pub fn new<E>(kind: ErrorKind, error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Error {
            kind,
            inner: error.into(),
        }
    }

    pub fn not_ready() -> Self {
        Self::new(ErrorKind::NotReadyError, "Statement not ready")
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
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.to_string())
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
