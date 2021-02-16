use std::ffi::CString;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Error {
    pub(crate) fn new<T: Into<String>>(message: T) -> Self {
        Error {
            message: message.into(),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl<T: std::error::Error> From<T> for Error {
    fn from(err: T) -> Self {
        crate::error::Error {
            message: err.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct InvalidPathError {
    pub path: CString,
}

impl InvalidPathError {
    pub fn new(path: CString) -> Self {
        InvalidPathError { path }
    }
}

impl std::error::Error for InvalidPathError {}
impl Display for InvalidPathError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let safe_path = self.path.to_str().unwrap_or("invalid string");
        write!(f, "Invalid path \"{}\"", safe_path)
    }
}

#[derive(Debug)]
pub struct NotReadyError {}
impl<'a> std::error::Error for NotReadyError {}
impl<'a> Display for NotReadyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Statement not ready")
    }
}

#[derive(Debug)]
pub struct OutOfBoundsError {
    pub index: String,
}

impl<'a> std::error::Error for OutOfBoundsError {}
impl<'a> Display for OutOfBoundsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unable to read {} index", self.index)
    }
}

impl<'a> From<usize> for OutOfBoundsError {
    fn from(value: usize) -> Self {
        crate::error::OutOfBoundsError {
            index: value.to_string().clone(),
        }
    }
}

impl<'a> From<String> for OutOfBoundsError {
    fn from(value: String) -> Self {
        crate::error::OutOfBoundsError { index: value }
    }
}
