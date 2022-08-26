use crate::platform::DatabasePlatform;
use crate::Result;
use std::sync::Arc;

pub(crate) type PlatformBox = Arc<Box<(dyn DatabasePlatform + Sync + Send)>>;

pub trait ToSqlStatementList: Send + Sync {
    fn to_statement_list(self) -> Result<Vec<String>>;
}

impl ToSqlStatementList for String {
    fn to_statement_list(self) -> Result<Vec<String>> {
        Ok(vec![self])
    }
}

impl ToSqlStatementList for &str {
    fn to_statement_list(self) -> Result<Vec<String>> {
        Ok(vec![self.to_string()])
    }
}

impl ToSqlStatementList for Vec<String> {
    fn to_statement_list(self) -> Result<Vec<String>> {
        Ok(self)
    }
}

impl ToSqlStatementList for Result<String> {
    fn to_statement_list(self) -> Result<Vec<String>> {
        Ok(vec![self?])
    }
}

impl ToSqlStatementList for Result<&str> {
    fn to_statement_list(self) -> Result<Vec<String>> {
        Ok(vec![self?.to_string()])
    }
}

impl ToSqlStatementList for Result<Vec<String>> {
    fn to_statement_list(self) -> Result<Vec<String>> {
        self
    }
}
