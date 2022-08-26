use super::asset::Asset;
use crate::platform::DatabasePlatform;
use crate::schema::asset::{impl_asset, AbstractAsset};
use crate::schema::{Identifier, Index, IntoIdentifier};
use crate::Value;
use std::borrow::Borrow;
use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyReferentialAction {
    Cascade,
    SetNull,
    NoAction,
    Restrict,
    SetDefault,
}

#[derive(Clone, IntoIdentifier)]
pub struct ForeignKeyConstraint {
    asset: AbstractAsset,
    local_columns: Vec<Identifier>,
    foreign_columns: Vec<Identifier>,
    foreign_table: Identifier,
    options: HashMap<String, Value>,
    pub on_update: Option<ForeignKeyReferentialAction>,
    pub on_delete: Option<ForeignKeyReferentialAction>,
}

impl ForeignKeyConstraint {
    pub fn new<LC, FC, FT>(
        local_columns: Vec<LC>,
        foreign_columns: Vec<FC>,
        foreign_table: FT,
        options: HashMap<String, Value>,
        on_update: Option<ForeignKeyReferentialAction>,
        on_delete: Option<ForeignKeyReferentialAction>,
    ) -> Self
    where
        LC: Into<String> + Clone,
        FC: Into<String> + Clone,
        FT: Into<String>,
    {
        let local_columns = local_columns
            .iter()
            .cloned()
            .map(|c| Identifier::new(c.into(), false))
            .collect();
        let foreign_columns = foreign_columns
            .iter()
            .cloned()
            .map(|c| Identifier::new(c.into(), false))
            .collect();
        let foreign_table = Identifier::new(foreign_table, false);

        Self {
            asset: AbstractAsset::default(),
            local_columns,
            foreign_columns,
            foreign_table,
            options,
            on_update,
            on_delete,
        }
    }

    pub fn get_local_columns(&self) -> &Vec<Identifier> {
        &self.local_columns
    }

    pub fn get_quoted_local_columns<T: DatabasePlatform + ?Sized>(
        &self,
        platform: &T,
    ) -> Vec<String> {
        self.local_columns
            .iter()
            .map(|c| c.get_quoted_name(platform))
            .collect()
    }

    pub fn get_unquoted_local_columns(&self) -> Vec<String> {
        self.local_columns.iter().map(|c| c.get_name()).collect()
    }

    pub fn get_foreign_columns(&self) -> &Vec<Identifier> {
        &self.foreign_columns
    }

    pub fn get_quoted_foreign_columns<T: DatabasePlatform + ?Sized>(
        &self,
        platform: &T,
    ) -> Vec<String> {
        self.foreign_columns
            .iter()
            .map(|c| c.get_quoted_name(platform))
            .collect()
    }

    pub fn get_unquoted_foreign_columns(&self) -> Vec<String> {
        self.foreign_columns.iter().map(|c| c.get_name()).collect()
    }

    pub fn get_foreign_table(&self) -> &Identifier {
        &self.foreign_table
    }

    pub fn get_quoted_foreign_table_name<T: DatabasePlatform + ?Sized>(
        &self,
        platform: &T,
    ) -> String {
        self.foreign_table.get_quoted_name(platform)
    }

    pub fn get_options(&self) -> HashMap<String, Value> {
        self.options.clone()
    }

    pub fn get_option(&self, option: &str) -> Option<&Value> {
        self.options.get(option)
    }

    /// Returns the non-schema qualified foreign table name.
    pub fn get_unqualified_foreign_table_name(&self) -> String {
        let name = self.foreign_table.get_name().to_lowercase();
        if let Some(pos) = name.rfind('.') {
            name.split_at(pos + 1).1.to_string()
        } else {
            name
        }
    }

    /// Checks whether this foreign key constraint intersects the given index columns.
    /// Returns `true` if at least one of this foreign key's local columns
    /// matches one of the given index's columns, `false` otherwise.
    pub fn intersects_index_columns(&self, index: &Index) -> bool {
        for index_column in index.get_columns() {
            for local_column in self.local_columns.iter().map(|c| c.get_name()) {
                if local_column.to_lowercase() == index_column.to_lowercase() {
                    return true;
                }
            }
        }

        false
    }
}

fn lowercase_vec<T: Borrow<str>>(v: Vec<T>) -> Vec<String> {
    v.iter().map(|c| c.borrow().to_lowercase()).collect()
}

impl PartialEq for ForeignKeyConstraint {
    fn eq(&self, other: &Self) -> bool {
        lowercase_vec(self.get_unquoted_local_columns())
            == lowercase_vec(other.get_unquoted_local_columns())
            && lowercase_vec(self.get_unquoted_foreign_columns())
                == lowercase_vec(other.get_unquoted_foreign_columns())
            && self.get_unqualified_foreign_table_name()
                == other.get_unqualified_foreign_table_name()
            && self.on_update == other.on_update
            && self.on_delete == other.on_delete
    }
}

impl_asset!(ForeignKeyConstraint, asset);
