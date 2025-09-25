use crate::Value;
use crate::platform::DatabasePlatform;
use crate::schema::asset::{AbstractAsset, Asset, impl_asset};
use crate::schema::{Identifier, IntoIdentifier};
use std::collections::HashMap;

#[derive(Clone, Debug, IntoIdentifier)]
pub struct UniqueConstraint {
    asset: AbstractAsset,
    columns: HashMap<String, Identifier>,
    flags: Vec<String>,
    options: HashMap<String, Value>,
}

impl UniqueConstraint {
    pub fn new<S: AsRef<str>, C: IntoIdentifier>(
        name: S,
        columns: &[C],
        flags: &[String],
        options: HashMap<String, Value>,
    ) -> Self {
        let mut asset = AbstractAsset::default();
        asset.set_name(name.as_ref());

        let mut this = Self {
            asset,
            columns: HashMap::new(),
            flags: vec![],
            options,
        };

        for column in columns {
            this.add_column(column);
        }

        for flag in flags {
            this.add_flag(flag);
        }

        this
    }

    /// Adds flag for a unique constraint that translates to platform specific handling.
    pub fn add_flag(&mut self, flag: &str) {
        if !self.has_flag(flag) {
            self.flags.push(flag.to_string());
        }
    }

    /// Does this unique constraint have a specific flag?
    pub fn has_flag(&self, flag: &str) -> bool {
        self.flags.iter().any(|f| f.eq(flag))
    }

    pub fn remove_flag(&mut self, flag: &str) {
        for i in 0..self.flags.len() {
            if self.flags.get(i).unwrap().eq(flag) {
                self.flags.remove(i);
            }
        }
    }

    pub fn get_columns(&self) -> Vec<String> {
        self.columns.keys().cloned().collect()
    }

    pub fn get_quoted_columns(&self, platform: &dyn DatabasePlatform) -> Vec<String> {
        self.columns
            .values()
            .map(|c| c.get_quoted_name(platform))
            .collect()
    }

    pub fn get_flags(&self) -> &Vec<String> {
        &self.flags
    }

    pub fn get_unquoted_columns(&self) -> Vec<String> {
        self.get_columns()
            .iter()
            .map(|col| self.trim_quotes(col))
            .collect()
    }

    /// Does this unique constraint have a specific option?
    pub fn has_option(&self, name: &str) -> bool {
        if let Some(opt) = self.options.get(name) {
            opt != &Value::NULL
        } else {
            false
        }
    }

    pub fn get_option(&self, name: &str) -> Option<&Value> {
        self.options.get(name)
    }

    pub fn get_options(&self) -> Vec<Value> {
        self.options.values().cloned().collect()
    }

    /// Adds a new column to the unique constraint.
    fn add_column<I: IntoIdentifier>(&mut self, column: &I) {
        let _ = self
            .columns
            .insert(column.to_string(), column.into_identifier());
    }
}

impl_asset!(UniqueConstraint, asset);
