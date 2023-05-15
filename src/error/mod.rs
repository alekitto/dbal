use crate::schema::IntoIdentifier;
use crate::schema::{Asset, Identifier};
use crate::Value;
use std::any::TypeId;
use std::backtrace::Backtrace;
use std::fmt::{Debug, Display, Formatter};
use std::num::TryFromIntError;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    UnknownDriver = 0,
    PlatformNotCompiled = 1,
    NotReadyError = 2,
    OutOfBoundsError = 3,
    UnsupportedNamedParameters = 4,
    MixedParametersTypes = 5,
    TypeMismatch = 6,
    UnknownType = 7,
    ConversionFailed = 8,
    UnknownDatabaseType = 9,

    PostgresTypeMismatch = 1001,
    PlatformFeatureUnsupported = 2000,
    NoColumnsForTable = 2001,
    ForeignKeyDefinitionInvalid = 2002,
    IndexDefinitionInvalid = 2003,
    ColumnDoesNotExist = 2004,
    NotConnected = 5000,
    DatabaseRequired = 5001,

    UnknownError = -1,
}

pub struct Error {
    kind: ErrorKind,
    inner: Box<dyn std::error::Error + Send + Sync>,
    backtrace: Backtrace,
}

pub struct StdError(Error);

impl StdError {
    pub fn backtrace(&self) -> Option<&Backtrace> {
        Some(&self.0.backtrace)
    }
}

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
        Self {
            kind,
            inner: error.into(),
            backtrace: Backtrace::capture(),
        }
    }

    pub fn backtrace(&self) -> Option<&Backtrace> {
        Some(&self.backtrace)
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn unknown_driver(scheme: &str) -> Self {
        Self::new(ErrorKind::UnknownDriver, format!("Unknown driver protocol \"{}\". Use Driver::create_with_connection to use a custom driver connection", scheme))
    }

    pub fn platform_not_compiled(platform: &str) -> Self {
        Self::new(
            ErrorKind::PlatformNotCompiled,
            format!(
                "Invalid platform \"{}\": support for this platform has not beed compiled",
                platform
            ),
        )
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

    pub fn unknown_type(r#type: TypeId) -> Self {
        Self::new(
            ErrorKind::UnknownType,
            format!("You have requested a non-existent type {:?}. Please register it in the type manager before trying to use it", r#type)
        )
    }

    pub fn unknown_database_type(r#type: &str, platform_name: &str) -> Self {
        Self::new(
            ErrorKind::UnknownDatabaseType,
            format!(
                "Unknown database type {} requested, {} may not support it.",
                r#type, platform_name
            ),
        )
    }

    pub fn postgres_type_mismatch() -> Self {
        Self::new(
            ErrorKind::PostgresTypeMismatch,
            "Type mismatch when converting parameters to postgres values.",
        )
    }

    pub fn platform_feature_unsupported<T>(err: T) -> Self
    where
        T: ToString,
    {
        Self::new(ErrorKind::PlatformFeatureUnsupported, err.to_string())
    }

    pub fn no_columns_specified_for_table(table_name: &Identifier) -> Self {
        Self::new(
            ErrorKind::NoColumnsForTable,
            format!("No columns specified for table {}", table_name.get_name()),
        )
    }

    pub fn foreign_key_definition_invalid(invalid_component: &str) -> Self {
        Self::new(
            ErrorKind::ForeignKeyDefinitionInvalid,
            format!("Incomplete definition. '{}' required.", invalid_component),
        )
    }

    pub fn index_definition_invalid(invalid_component: &str) -> Self {
        Self::new(
            ErrorKind::IndexDefinitionInvalid,
            format!("Incomplete definition. '{}' required.", invalid_component),
        )
    }

    pub fn column_does_not_exist(invalid_column: &dyn IntoIdentifier, table_name: &str) -> Self {
        Self::new(
            ErrorKind::ColumnDoesNotExist,
            format!(
                "Column '{}' does not exist on table '{}'.",
                invalid_column, table_name
            ),
        )
    }

    pub fn not_connected() -> Self {
        Self::new(ErrorKind::NotConnected, "Not connected")
    }

    pub fn database_required(method_name: &str) -> Self {
        Self::new(
            ErrorKind::DatabaseRequired,
            format!("A database is required for the method: {}.", method_name),
        )
    }

    pub fn conversion_failed_invalid_type(
        value: &Value,
        to_type: &str,
        possible_types: &[&str],
    ) -> Self {
        Self::new(
            ErrorKind::ConversionFailed,
            format!(
                "Could not convert value {:#?} to type {}. Expected one of the following types: {}",
                value,
                to_type,
                possible_types.join(", ")
            ),
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
        write!(f, "{}\nBacktrace:\n{}", self.inner, self.backtrace)
    }
}

impl Default for Error {
    fn default() -> Self {
        Error::new(ErrorKind::UnknownError, "unknown error")
    }
}

impl<T> From<T> for Error
where
    T: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
{
    fn from(err: T) -> Self {
        let mut kind = ErrorKind::UnknownError;
        if TypeId::of::<TryFromIntError>() == TypeId::of::<T>() {
            kind = ErrorKind::TypeMismatch;
        }

        Error::new(kind, err)
    }
}
