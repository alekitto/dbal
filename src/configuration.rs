use std::fmt::{Debug, Formatter};

pub struct Configuration {
    schema_assets_filter: Box<dyn (Fn(&str) -> bool) + Sync + Send>,
}

impl Configuration {
    pub fn new() -> Self {
        Self {
            schema_assets_filter: Box::new(|_| true),
        }
    }

    pub fn get_schema_assets_filter(&self) -> &(dyn (Fn(&str) -> bool) + Sync + Send) {
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
