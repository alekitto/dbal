mod asset;
mod check_constraint;
mod column;
mod column_diff;
mod comparator;
mod foreign_key_constraint;
mod identifier;
mod index;
mod schema_config;
mod schema_diff;
mod schema_manager;
mod sequence;
mod table;
mod table_diff;
mod unique_constraint;
mod view;

pub(crate) use asset::Asset;
pub(crate) use column::ColumnData;
use std::borrow::Cow;
pub(crate) use table::TableOptions;

pub use check_constraint::CheckConstraint;
pub use column::{Column, ColumnList};
pub use column_diff::{ChangedProperty, ColumnDiff};
pub use comparator::{diff_column, Comparator, GenericComparator};
pub use foreign_key_constraint::{
    FKConstraintList, ForeignKeyConstraint, ForeignKeyReferentialAction,
};
pub use identifier::{Identifier, IntoIdentifier};
pub use index::{Index, IndexList, IndexOptions};
pub use schema_diff::SchemaDiff;
pub use schema_manager::{extract_type_from_comment, remove_type_from_comment, SchemaManager};
pub(crate) use schema_manager::{get_database, string_from_value};
pub use sequence::Sequence;
pub use table::{Table, TableList};
pub use table_diff::TableDiff;
pub use unique_constraint::UniqueConstraint;
pub use view::View;

pub use ::creed_macros::IntoIdentifier;

use crate::platform::DatabasePlatform;
use crate::schema::asset::{impl_asset, AbstractAsset};
use crate::schema::schema_config::SchemaConfig;
use crate::Result;
use itertools::Itertools;

pub trait NamedListIndex {
    fn is_usize(&self) -> bool;
    fn as_usize(&self) -> usize;
    fn as_str(&self) -> Cow<str>;
}

impl NamedListIndex for usize {
    fn is_usize(&self) -> bool {
        true
    }

    fn as_usize(&self) -> usize {
        *self
    }

    fn as_str(&self) -> Cow<str> {
        "".into()
    }
}

impl NamedListIndex for &str {
    fn is_usize(&self) -> bool {
        false
    }

    fn as_usize(&self) -> usize {
        0
    }

    fn as_str(&self) -> Cow<str> {
        self.to_string().into()
    }
}

impl NamedListIndex for dyn AsRef<str> {
    fn is_usize(&self) -> bool {
        false
    }

    fn as_usize(&self) -> usize {
        0
    }

    fn as_str(&self) -> Cow<str> {
        self.as_ref().into()
    }
}

impl NamedListIndex for Identifier {
    fn is_usize(&self) -> bool {
        false
    }

    fn as_usize(&self) -> usize {
        0
    }

    fn as_str(&self) -> Cow<str> {
        self.get_name()
    }
}

impl NamedListIndex for String {
    fn is_usize(&self) -> bool {
        false
    }

    fn as_usize(&self) -> usize {
        0
    }

    fn as_str(&self) -> Cow<str> {
        self.into()
    }
}

#[derive(Clone, Default, IntoIdentifier)]
pub struct Schema {
    asset: AbstractAsset,
    tables: Vec<Table>,
    sequences: Vec<Sequence>,
    views: Vec<View>,
    schema_names: Vec<Identifier>,
    schema_config: SchemaConfig,
}

impl Schema {
    pub fn new(
        tables: Vec<Table>,
        views: Vec<View>,
        sequences: Vec<Sequence>,
        schema_names: Vec<Identifier>,
        schema_config: SchemaConfig,
    ) -> Self {
        Self {
            asset: AbstractAsset::default(),
            tables,
            sequences,
            views,
            schema_names,
            schema_config,
        }
    }

    pub fn get_schema_names(&self) -> &Vec<Identifier> {
        &self.schema_names
    }

    pub fn has_schema_name<T: IntoIdentifier>(&self, name: T) -> bool {
        let name = name.into_identifier();
        let name = name.get_name();
        self.schema_names.iter().any(|i| i.get_name() == name)
    }

    pub fn get_tables(&self) -> &Vec<Table> {
        &self.tables
    }

    pub fn get_table<T: IntoIdentifier>(&self, name: T) -> Option<&Table> {
        let name = name.into_identifier();
        let name = name.get_name();
        self.tables.iter().find(|i| i.get_name() == name)
    }

    pub fn get_table_mut<T: IntoIdentifier>(&mut self, name: T) -> Option<&mut Table> {
        let name = name.into_identifier();
        let name = name.get_name();
        self.tables.iter_mut().find(|i| i.get_name() == name)
    }

    pub fn get_views(&self) -> &Vec<View> {
        &self.views
    }

    /// Gets the first table matching name and unwraps the value.
    ///
    /// # Safety
    ///
    /// Calling this method without checking if table exists will _panic_.
    pub unsafe fn get_table_unchecked<T: IntoIdentifier>(&self, name: T) -> &Table {
        let name = name.into_identifier();
        let name = name.get_name();
        self.tables.iter().find(|i| i.get_name() == name).unwrap()
    }

    pub fn has_table<T: IntoIdentifier>(&self, name: T) -> bool {
        let name = name.into_identifier();
        let name = name.get_name();
        self.tables.iter().any(|i| i.get_name() == name)
    }

    pub fn has_view<T: IntoIdentifier>(&self, name: T) -> bool {
        let name = name.into_identifier();
        let name = name.get_name();
        self.views.iter().any(|i| i.get_name() == name)
    }

    pub fn create_table<T: IntoIdentifier>(&mut self, table: T) -> Result<&mut Table> {
        let name = table.into_identifier();
        let name = name.get_name();
        if self.has_table(&name) {
            Err(format!(r#"Table "{}" already exists."#, name).into())
        } else {
            let mut table = Table::new(&name);
            table.set_schema_config(self.schema_config.clone());

            self.tables.push(table);
            Ok(self.get_table_mut(name).unwrap())
        }
    }

    pub fn drop_table<T: IntoIdentifier>(&mut self, name: T) {
        let name = name.into_identifier();
        let name = name.get_name();
        if let Some(pos) = self
            .tables
            .iter()
            .position(|table| table.get_name() == name)
        {
            self.tables.remove(pos);
        }
    }

    pub fn drop_view<T: IntoIdentifier>(&mut self, name: T) {
        let name = name.into_identifier();
        let name = name.get_name();
        if let Some(pos) = self.views.iter().position(|view| view.get_name() == name) {
            self.views.remove(pos);
        }
    }

    pub fn get_sequences(&self) -> &Vec<Sequence> {
        &self.sequences
    }

    pub fn get_sequence<T: IntoIdentifier>(&self, name: T) -> Option<&Sequence> {
        let name = name.into_identifier();
        let name = name.get_name();
        self.sequences.iter().find(|i| i.get_name() == name)
    }

    /// Gets the first sequence matching name and unwraps the value.
    ///
    /// # Safety
    ///
    /// Calling this method without checking if sequence exists will _panic_.
    pub unsafe fn get_sequence_unchecked<T: IntoIdentifier>(&self, name: T) -> &Sequence {
        let name = name.into_identifier();
        let name = name.get_name();
        self.sequences
            .iter()
            .find(|i| i.get_name() == name)
            .unwrap()
    }

    pub fn has_sequence<T: IntoIdentifier>(&self, name: T) -> bool {
        let name = name.into_identifier();
        let name = name.get_name();
        self.sequences.iter().any(|i| i.get_name() == name)
    }

    /// Returns an array of necessary SQL queries to create the schema on the given platform.
    pub fn to_sql(&self, schema_manager: &dyn SchemaManager) -> Result<Vec<String>> {
        let builder = CreateSchemaObjectsSQLBuilder::new(schema_manager);
        builder.build_sql(self)
    }

    /// Return an array of necessary SQL queries to drop the schema on the given platform.
    pub fn to_drop_sql(&self, schema_manager: &dyn SchemaManager) -> Result<Vec<String>> {
        let builder = DropSchemaObjectsSQLBuilder::new(schema_manager);
        builder.build_sql(self)
    }
}

impl_asset!(Schema, asset);

struct CreateSchemaObjectsSQLBuilder<'a> {
    schema_manager: &'a dyn SchemaManager,
}

impl<'a> CreateSchemaObjectsSQLBuilder<'a> {
    fn new(schema_manager: &'a dyn SchemaManager) -> Self {
        Self { schema_manager }
    }

    pub fn build_sql(&self, schema: &Schema) -> Result<Vec<String>> {
        let mut sql = vec![];
        sql.extend(self.build_namespace_statements(schema.get_schema_names())?);
        sql.extend(self.build_sequence_statements(schema.get_sequences())?);
        sql.extend(self.build_table_statements(schema.get_tables())?);
        sql.extend(self.build_view_statements(schema.get_views())?);

        Ok(sql)
    }

    fn build_namespace_statements(&self, namespaces: &[Identifier]) -> Result<Vec<String>> {
        let platform = self.schema_manager.get_platform()?;
        Ok(if platform.supports_schemas() {
            namespaces
                .iter()
                .map(|ns| self.schema_manager.get_create_schema_sql(ns))
                .try_collect()?
        } else {
            vec![]
        })
    }

    fn build_table_statements(&self, tables: &[Table]) -> Result<Vec<String>> {
        self.schema_manager.get_create_tables_sql(tables)
    }

    fn build_sequence_statements(&self, sequences: &[Sequence]) -> Result<Vec<String>> {
        sequences
            .iter()
            .map(|s| self.schema_manager.get_create_sequence_sql(s))
            .try_collect()
    }

    fn build_view_statements(&self, views: &[View]) -> Result<Vec<String>> {
        views
            .iter()
            .map(|v| self.schema_manager.get_create_view_sql(v))
            .try_collect()
    }
}

struct DropSchemaObjectsSQLBuilder<'a> {
    schema_manager: &'a dyn SchemaManager,
}

impl<'a> DropSchemaObjectsSQLBuilder<'a> {
    fn new(schema_manager: &'a dyn SchemaManager) -> Self {
        Self { schema_manager }
    }

    pub fn build_sql(&self, schema: &Schema) -> Result<Vec<String>> {
        let mut sql = vec![];
        sql.extend(self.build_sequence_statements(schema.get_sequences())?);
        sql.extend(self.build_table_statements(schema.get_tables())?);
        sql.extend(self.build_view_statements(schema.get_views())?);

        Ok(sql)
    }

    fn build_table_statements(&self, tables: &[Table]) -> Result<Vec<String>> {
        self.schema_manager.get_drop_tables_sql(tables)
    }

    fn build_sequence_statements(&self, sequences: &[Sequence]) -> Result<Vec<String>> {
        sequences
            .iter()
            .map(|s| self.schema_manager.get_drop_sequence_sql(s))
            .try_collect()
    }

    fn build_view_statements(&self, views: &[View]) -> Result<Vec<String>> {
        views
            .iter()
            .map(|v| self.schema_manager.get_drop_view_sql(v))
            .try_collect()
    }
}
