use crate::schema::asset::{impl_asset, Asset};
use crate::schema::{
    Column, ForeignKeyConstraint, Identifier, Index, IntoIdentifier, UniqueConstraint,
};
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
    pub alter: bool,
}

#[derive(Clone, IntoIdentifier)]
pub struct Table {
    name: Identifier,
    columns: Vec<Column>,
    indices: Vec<Index>,
    primary_key_name: Option<String>,
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
    alter: bool,
}

impl Table {
    pub fn new(name: Identifier) -> Self {
        Self {
            name,
            columns: vec![],
            indices: vec![],
            primary_key_name: None,
            unique_constraints: vec![],
            foreign_keys: vec![],
            temporary: false,
            charset: None,
            collation: None,
            engine: None,
            auto_increment: None,
            comment: None,
            row_format: None,
            table_options: None,
            partition_options: None,
            alter: false,
        }
    }

    pub fn template(&self) -> Self {
        Self {
            name: self.name.clone(),
            columns: vec![],
            indices: vec![],
            primary_key_name: None,
            unique_constraints: vec![],
            foreign_keys: vec![],
            temporary: self.temporary,
            charset: self.charset.clone(),
            collation: self.collation.clone(),
            engine: self.engine.clone(),
            auto_increment: self.auto_increment.clone(),
            comment: self.comment.clone(),
            row_format: self.row_format.clone(),
            table_options: self.table_options.clone(),
            partition_options: self.partition_options.clone(),
            alter: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn get_table_name(&self) -> &Identifier {
        &self.name
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn add_column(&mut self, column: Column) {
        self.columns.push(column)
    }

    pub fn add_columns<T: Iterator<Item = Column>>(&mut self, columns: T) {
        for column in columns {
            self.add_column(column)
        }
    }

    pub fn has_column(&self, name: &dyn IntoIdentifier) -> bool {
        let name = name.into_identifier().get_name();
        self.columns.iter().any(|column| column.get_name() == name)
    }

    pub fn get_column(&self, name: &dyn IntoIdentifier) -> Option<&Column> {
        let name = name.into_identifier().get_name();
        self.columns
            .iter()
            .find(|&column| column.get_name() == name)
    }

    pub fn add_index(&mut self, index: Index) {
        self.indices.push(index);
    }

    pub fn add_indices<T: Iterator<Item = Index>>(&mut self, indices: T) {
        for index in indices {
            self.add_index(index)
        }
    }

    pub fn has_primary_key(&self) -> bool {
        self.primary_key_name.is_some()
    }

    pub fn get_indices(&self) -> &Vec<Index> {
        &self.indices
    }

    pub fn has_index(&self, index_name: &dyn IntoIdentifier) -> bool {
        let name = index_name.into_identifier().get_name();
        self.indices.iter().any(|i| i.get_name() == name)
    }

    pub fn get_index(&self, index_name: &dyn IntoIdentifier) -> Option<&Index> {
        let name = index_name.into_identifier().get_name();
        self.indices.iter().find(|i| i.get_name() == name)
    }

    pub fn get_unique_constraints(&self) -> &Vec<UniqueConstraint> {
        &self.unique_constraints
    }

    pub fn get_foreign_keys(&self) -> &Vec<ForeignKeyConstraint> {
        &self.foreign_keys
    }

    pub fn add_foreign_key(&mut self, constraint: ForeignKeyConstraint) {
        self.foreign_keys.push(constraint)
    }

    pub fn add_foreign_keys<T: Iterator<Item = ForeignKeyConstraint>>(&mut self, constraints: T) {
        for constraint in constraints {
            self.add_foreign_key(constraint)
        }
    }

    pub fn get_comment(&self) -> &Option<String> {
        &self.comment
    }

    pub fn set_comment(&mut self, comment: Option<String>) {
        self.comment = comment;
    }

    pub fn get_primary_key(&self) -> Option<&Index> {
        self.indices.iter().find(|&index| index.is_primary())
    }

    pub fn get_engine(&self) -> Option<String> {
        self.engine.clone()
    }

    pub fn set_engine(&mut self, engine: Option<String>) {
        self.engine = engine;
    }

    pub fn is_temporary(&self) -> bool {
        self.temporary
    }

    pub fn set_temporary(&mut self, temporary: bool) {
        self.temporary = temporary;
    }

    pub fn get_charset(&self) -> Option<String> {
        self.charset.clone()
    }

    pub fn set_charset(&mut self, charset: Option<String>) {
        self.charset = charset;
    }

    pub fn get_collation(&self) -> Option<String> {
        self.collation.clone()
    }

    pub fn set_collation(&mut self, collation: Option<String>) {
        self.collation = collation;
    }

    pub fn get_auto_increment(&self) -> Option<String> {
        self.auto_increment.clone()
    }

    pub fn set_auto_increment(&mut self, auto_increment: Option<String>) {
        self.auto_increment = auto_increment;
    }

    pub fn get_row_format(&self) -> Option<String> {
        self.row_format.clone()
    }

    pub fn set_row_format(&mut self, row_format: Option<String>) {
        self.row_format = row_format;
    }

    pub fn get_table_options(&self) -> Option<String> {
        self.table_options.clone()
    }

    pub fn set_table_options(&mut self, table_options: Option<String>) {
        self.table_options = table_options;
    }

    pub fn get_partition_options(&self) -> Option<String> {
        self.partition_options.clone()
    }

    pub fn set_partition_options(&mut self, partition_options: Option<String>) {
        self.partition_options = partition_options;
    }

    pub fn get_alter(&self) -> bool {
        self.alter
    }

    pub fn set_alter(&mut self, alter: bool) {
        self.alter = alter;
    }
}

impl_asset!(Table, name);
