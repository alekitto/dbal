use crate::platform::DatabasePlatform;
use crate::schema::Asset;
use crate::{Connection, Result};
use itertools::Itertools;
use regex::{Captures, Regex};
use std::collections::HashMap;
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

pub fn strtr<const N: usize>(string: &str, replace_pairs: [(String, String); N]) -> String {
    if replace_pairs.is_empty() {
        string.to_string()
    } else {
        let map = HashMap::from(replace_pairs);
        let search_patterns = map.keys().map(|f| regex::escape(f)).join("|");
        let search_re = Regex::new(&search_patterns).unwrap();

        search_re
            .replace_all(string, |cap: &Captures| {
                let mat = cap.get(0).unwrap().as_str();
                map.get(mat).unwrap().clone()
            })
            .to_string()
    }
}

pub macro function_name() {{
    // Okay, this is ugly, I get it. However, this is the best we can get on a stable rust.
    fn f() {}
    fn type_name_of<T>(_: T) -> &'static str {
        std::any::type_name::<T>()
    }
    let name = type_name_of(f);
    // `3` is the length of the `::f`.
    &name[..name.len() - 3]
}}

/// Filters asset names if they are configured to return only a subset of all
/// the found elements.
pub fn filter_asset_names<A: Asset + Clone>(connection: &Connection, assets: Vec<A>) -> Vec<A> {
    let configuration = connection.get_configuration();
    let filter = configuration.get_schema_assets_filter();

    assets
        .iter()
        .filter(|a| filter(&a.get_name()))
        .cloned()
        .collect()
}

pub macro const_expr_count {
    () => (0),
    ($e:expr) => (1),
    ($e:expr; $($other_e:expr);*) => ({
        1 $(+ $crate::const_expr_count!($other_e) )*
    }),
    ($e:expr; $($other_e:expr);* ; ) => (
        $crate::const_expr_count! { $e; $($other_e);* }
    ),
}
