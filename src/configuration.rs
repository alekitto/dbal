use std::fmt::{Debug, Formatter};

type SchemaAssetFilterType = dyn (Fn(&str) -> bool) + Sync + Send;

pub struct Configuration {
    schema_assets_filter: Box<SchemaAssetFilterType>,
}

impl Configuration {
    pub fn new() -> Self {
        Self {
            schema_assets_filter: Box::new(|_| true),
        }
    }

    pub fn set_schema_assets_filter(mut self, filter: Box<SchemaAssetFilterType>) -> Self {
        self.schema_assets_filter = filter;
        self
    }

    pub fn get_schema_assets_filter(&self) -> &SchemaAssetFilterType {
        &self.schema_assets_filter
    }
}

impl Debug for Configuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Configuration").finish()
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration::new()
    }
}
