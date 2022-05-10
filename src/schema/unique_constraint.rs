use crate::platform::DatabasePlatform;
use crate::schema::asset::{AbstractAsset, Asset};
use crate::schema::Identifier;
use crate::Value;
use std::collections::HashMap;

#[derive(Clone)]
pub struct UniqueConstraint {
    asset: AbstractAsset,
    columns: HashMap<String, Identifier>,
    flags: Vec<String>,
    options: HashMap<String, Value>,
}

impl UniqueConstraint {
    fn new(
        name: String,
        columns: Vec<String>,
        flags: Vec<String>,
        options: HashMap<String, Value>,
    ) -> Self {
        let mut asset = AbstractAsset::default();
        asset.set_name(name);

        let mut this = Self {
            asset,
            columns: HashMap::new(),
            flags: vec![],
            options,
        };

        for column in columns {
            this.add_column(&column);
        }

        for flag in flags {
            this.add_flag(&flag);
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

    pub fn get_quoted_columns<T: DatabasePlatform + ?Sized>(&self, platform: &T) -> Vec<String> {
        self.columns
            .iter()
            .map(|(_, c)| c.get_quoted_name(platform))
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
        self.options.iter().map(|(_, v)| v).cloned().collect()
    }

    /// Adds a new column to the unique constraint.
    fn add_column(&mut self, column: &str) {
        let _ = self
            .columns
            .insert(column.to_string(), Identifier::new(column, false));
    }
}

impl Asset for UniqueConstraint {
    fn get_name(&self) -> String {
        self.asset.get_name()
    }

    fn set_name(&mut self, name: String) {
        self.asset.set_name(name)
    }

    fn get_namespace_name(&self) -> Option<String> {
        self.asset.get_namespace_name()
    }

    fn get_shortest_name(&self, default_namespace_name: &str) -> String {
        self.asset.get_shortest_name(default_namespace_name)
    }

    fn is_quoted(&self) -> bool {
        self.asset.is_quoted()
    }
}
