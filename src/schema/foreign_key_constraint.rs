use super::asset::Asset;
use crate::platform::DatabasePlatform;
use crate::schema::asset::AbstractAsset;
use crate::schema::{Identifier, Index};
use crate::Value;
use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq)]
pub enum ForeignKeyReferentialAction {
    Cascade,
    SetNull,
    NoAction,
    Restrict,
    SetDefault,
}

#[derive(Clone, PartialEq)]
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

    pub fn get_foreign_table(&self) -> &Identifier {
        &self.foreign_table
    }

    pub fn get_quoted_foreign_table_name<T: DatabasePlatform + ?Sized>(
        &self,
        platform: &T,
    ) -> String {
        self.foreign_table.get_quoted_name(platform)
    }

    pub fn get_option(&self, option: &str) -> Option<&Value> {
        self.options.get(option)
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

impl Asset for ForeignKeyConstraint {
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
