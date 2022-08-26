use crate::schema::asset::Asset;
use crate::schema::{Column, ForeignKeyConstraint, Identifier, Index, UniqueConstraint};
use std::collections::HashMap;

#[derive(Clone, Default)]
pub struct TableOptions {
    pub unique_constraints: HashMap<String, UniqueConstraint>,
    pub indexes: HashMap<String, Index>,
    pub primary: Option<(Vec<String>, Index)>,
    pub foreign_keys: Vec<ForeignKeyConstraint>,
    pub temporary: bool,
    pub charset: Option<String>,
    pub collation: Option<String>,
    pub engine: Option<String>,
    pub auto_increment: Option<String>,
    pub comment: Option<String>,
    pub row_format: Option<String>,
    pub table_options: Option<String>,
    pub partition_options: Option<String>,
}

#[derive(Clone)]
pub struct Table {
    name: Identifier,
    columns: Vec<Column>,
    indices: Vec<Index>,
    unique_constraints: Vec<UniqueConstraint>,
    foreign_keys: Vec<ForeignKeyConstraint>,
    temporary: bool,
    charset: Option<String>,
    collation: Option<String>,
    engine: Option<String>,
    auto_increment: Option<String>,
    comment: Option<String>,
    row_format: Option<String>,
    table_options: Option<String>,
    partition_options: Option<String>,
}

impl Table {
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn get_table_name(&self) -> &Identifier {
        &self.name
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_column(&self, name: &Identifier) -> Option<&Column> {
        let name = name.get_name();
        self.columns
            .iter()
            .find(|&column| column.get_name() == name)
    }

    pub fn get_indices(&self) -> &Vec<Index> {
        &self.indices
    }

    pub fn get_unique_constraints(&self) -> &Vec<UniqueConstraint> {
        &self.unique_constraints
    }

    pub fn get_foreign_keys(&self) -> &Vec<ForeignKeyConstraint> {
        &self.foreign_keys
    }

    pub fn get_comment(&self) -> &Option<String> {
        &self.comment
    }

    pub fn get_primary_key(&self) -> Option<&Index> {
        self.indices.iter().find(|&index| index.is_primary())
    }

    pub fn get_engine(&self) -> Option<String> {
        self.engine.clone()
    }
}

impl Asset for Table {
    fn get_name(&self) -> String {
        self.name.get_name()
    }

    fn set_name(&mut self, name: String) {
        self.name.set_name(name)
    }

    fn get_namespace_name(&self) -> Option<String> {
        self.name.get_namespace_name()
    }

    fn get_shortest_name(&self, default_namespace_name: &str) -> String {
        self.name.get_shortest_name(default_namespace_name)
    }

    fn is_quoted(&self) -> bool {
        self.name.is_quoted()
    }
}
