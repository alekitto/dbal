use crate::schema::asset::{Asset, impl_asset};
use crate::schema::{Identifier, IntoIdentifier};

#[derive(Clone, IntoIdentifier)]
pub struct View {
    name: Identifier,
    sql: String,
}

impl View {
    pub fn new<I: IntoIdentifier>(name: I, sql: &str) -> Self {
        Self {
            name: name.into_identifier(),
            sql: sql.to_string(),
        }
    }

    pub fn get_sql(&self) -> String {
        self.sql.clone()
    }
}

impl_asset!(View, name);
