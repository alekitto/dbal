use crate::platform::DatabasePlatform;
use crate::schema::asset::{impl_asset, AbstractAsset, Asset};
use crate::schema::{Identifier, IntoIdentifier};
use crate::Value;
use std::collections::HashMap;

#[derive(Clone, Debug, IntoIdentifier)]
pub struct Index {
    asset: AbstractAsset,
    columns: Vec<Identifier>,
    flags: Vec<String>,
    options: HashMap<String, Value>,
    is_unique: bool,
    is_primary: bool,
    pub r#where: Option<String>,
}

impl Index {
    pub fn new<S: AsRef<str>, N: Into<Option<S>>, C: IntoIdentifier>(
        name: N,
        columns: &[C],
        is_unique: bool,
        is_primary: bool,
        flags: &[String],
        options: HashMap<String, Value>,
    ) -> Self {
        let mut asset = AbstractAsset::default();
        let name = if let Some(name) = name.into() {
            name.as_ref().to_string()
        } else {
            "".to_string()
        };

        asset.set_name(&name);
        let mut this = Self {
            asset,
            columns: vec![],
            flags: vec![],
            options,
            is_unique,
            is_primary,
            r#where: None,
        };

        for column in columns {
            this.add_column(column);
        }

        for flag in flags {
            this.add_flag(flag.as_ref());
        }

        this
    }

    /// Adds flag for an index that translates to platform specific handling.
    pub fn add_flag(&mut self, flag: &str) {
        if !self.has_flag(flag) {
            self.flags.push(flag.to_string());
        }
    }

    /// Does this index have a specific flag?
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
        self.columns
            .iter()
            .map(|c| c.get_name().into_owned())
            .collect()
    }

    pub fn get_quoted_columns(&self, platform: &dyn DatabasePlatform) -> Vec<String> {
        self.columns
            .iter()
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

    /// Does this index have a specific option?
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

    pub fn get_options(&self) -> &HashMap<String, Value> {
        &self.options
    }

    /// Adds a new column to the index.
    fn add_column<I: IntoIdentifier>(&mut self, column: &I) {
        let identifier = column.into_identifier();
        if self.columns.iter().all(|i| i != &identifier) {
            self.columns.push(identifier);
        }
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub fn is_unique(&self) -> bool {
        self.is_unique
    }

    /// Checks if this index exactly spans the given column names in the correct order.
    pub(crate) fn spans_columns(&self, column_names: &[String]) -> bool {
        self.columns.iter().enumerate().all(|(index, column)| {
            column_names.get(index).is_some_and(|column_name| {
                self.trim_quotes(&column.get_name().to_lowercase())
                    == self.trim_quotes(&column_name.to_lowercase())
            })
        })
    }

    pub fn is_fulfilled_by(&self, other: &Index) -> bool {
        // allow the other index to be equally large only. It being larger is an option
        // but it creates a problem with scenarios of the kind PRIMARY KEY(foo,bar) UNIQUE(foo)
        if other.get_columns().len() != self.get_columns().len() {
            return false;
        }

        // Check if columns are the same, and even in the same order
        if self.spans_columns(&other.get_columns()) {
            if self.same_partial_index(other) || !self.has_same_column_lengths(other) {
                return false;
            }

            if !self.is_unique() && self.is_primary() {
                // this is a special case: If the current key is neither primary or unique, any unique or
                // primary key will always have the same effect for the index and there cannot be any constraint
                // overlaps. This means a primary or unique index can always fulfill the requirements of just an
                // index that has no constraints.
                return true;
            }

            other.is_primary() == self.is_primary() && other.is_unique() == self.is_unique()
        } else {
            false
        }
    }

    /// Return whether the two indexes have the same partial index
    fn same_partial_index(&self, other: &Index) -> bool {
        self.get_option("where") == other.get_option("where")
    }

    /// Returns whether the index has the same column lengths as the other
    fn has_same_column_lengths(&self, other: &Index) -> bool {
        self.options.get("lengths") == other.options.get("lengths")
    }
}

impl_asset!(Index, asset);

pub struct IndexOptions {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub primary: bool,
    pub flags: Vec<String>,
    pub options_lengths: Vec<Option<usize>>,
    pub options_where: Option<String>,
}

impl IndexOptions {
    pub fn new_index(self) -> Index {
        let lengths = Value::Array(
            self.options_lengths
                .iter()
                .map(|v| match v {
                    Some(s) => Value::from(s),
                    None => Value::NULL,
                })
                .collect(),
        );

        let mut options = HashMap::new();
        options.insert("lengths".to_string(), lengths);
        options.insert(
            "where".to_string(),
            match self.options_where {
                Some(s) => Value::String(s),
                None => Value::NULL,
            },
        );

        Index::new(
            self.name,
            &self.columns,
            self.unique,
            self.primary,
            &self.flags,
            options,
        )
    }
}
