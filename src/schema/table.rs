use crate::schema::asset::{generate_identifier_name, impl_asset, Asset};
use crate::schema::{
    Column, ForeignKeyConstraint, ForeignKeyReferentialAction, Identifier, Index, IntoIdentifier,
    UniqueConstraint,
};
use crate::{Error, Result, Value};
use regex::Regex;
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

#[derive(Clone, Debug, IntoIdentifier)]
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
    alter: bool,
}

impl Table {
    pub fn new<I: IntoIdentifier>(name: I) -> Self {
        Self {
            name: name.into_identifier(),
            columns: vec![],
            indices: vec![],
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

    pub fn add_column<IC: Into<Column>>(&mut self, column: IC) {
        self.columns.push(column.into())
    }

    pub fn add_columns<T: Iterator<Item = Column>>(&mut self, columns: T) {
        for column in columns {
            self.add_column(column)
        }
    }

    pub fn has_column(&self, name: &dyn IntoIdentifier) -> bool {
        let name = name.into_identifier();
        let name = name.get_name();
        self.columns.iter().any(|column| column.get_name() == name)
    }

    pub fn get_column(&self, name: &dyn IntoIdentifier) -> Option<&Column> {
        let name = name.into_identifier();
        let name = name.get_name();
        self.columns
            .iter()
            .find(|&column| column.get_name() == name)
    }

    fn get_column_mut(&mut self, name: &dyn IntoIdentifier) -> Option<&mut Column> {
        let name = name.into_identifier();
        let name = name.get_name();
        self.columns
            .iter_mut()
            .find(|column| column.get_name() == name)
    }

    pub fn add_index(&mut self, mut index: Index) {
        if index.get_name().is_empty() {
            let mut columns = vec![self.get_name().into_owned()];
            columns.extend(index.get_columns());
            index.set_name(&generate_identifier_name(
                &columns,
                "idx",
                self.get_max_identifier_length(),
            ))
        }

        self.indices.push(index);
    }

    pub fn add_unique_index<S: AsRef<str> + IntoIdentifier + Clone>(
        &mut self,
        column_names: &[S],
        index_name: Option<&str>,
        options: HashMap<String, Value>,
    ) -> Result<()> {
        let index_name = if let Some(index_name) = index_name {
            index_name.to_string()
        } else {
            let mut names = vec![self.get_name().into_owned()];
            names.extend(column_names.iter().map(|c| c.as_ref().to_string()));
            generate_identifier_name(&names, "uniq", self.get_max_identifier_length())
        };

        self.add_index(self.create_index(
            column_names,
            &index_name,
            true,
            false,
            vec![],
            options,
        )?);

        Ok(())
    }

    /// Checks if an index begins in the order of the given columns.
    pub fn columns_are_indexed<S: IntoIdentifier + Clone>(&self, column_names: &[S]) -> bool {
        let column_names = column_names
            .iter()
            .map(|cn| cn.into_identifier().to_string())
            .collect::<Vec<_>>();
        for index in &self.indices {
            if index.spans_columns(column_names.as_slice()) {
                return true;
            }
        }

        false
    }

    fn get_max_identifier_length(&self) -> usize {
        // TODO
        65
    }

    pub fn add_indices<T: Iterator<Item = Index>>(&mut self, indices: T) {
        for index in indices {
            self.add_index(index)
        }
    }

    pub fn has_primary_key(&self) -> bool {
        self.indices.iter().any(|idx| idx.is_primary())
    }

    pub fn get_indices(&self) -> &Vec<Index> {
        &self.indices
    }

    pub fn has_index(&self, index_name: &dyn IntoIdentifier) -> bool {
        let name = index_name.into_identifier();
        let name = name.get_name();
        self.indices.iter().any(|i| i.get_name() == name)
    }

    pub fn get_index(&self, index_name: &dyn IntoIdentifier) -> Option<&Index> {
        let name = index_name.into_identifier();
        let name = name.get_name();
        self.indices.iter().find(|i| i.get_name() == name)
    }

    /// Sets the Primary Key.
    pub fn set_primary_key<S: IntoIdentifier + Clone>(
        &mut self,
        column_names: &[S],
        index_name: Option<&str>,
    ) -> Result<()> {
        let index_name = index_name.unwrap_or("primary");
        self.add_index(self.create_index(
            column_names,
            index_name,
            true,
            true,
            vec![],
            HashMap::default(),
        )?);

        for column_name in column_names {
            if let Some(column) = self.get_column_mut(column_name) {
                column.set_notnull(true);
            } else {
                return Err(Error::column_does_not_exist(column_name, &self.get_name()));
            }
        }

        Ok(())
    }

    pub fn get_unique_constraints(&self) -> &Vec<UniqueConstraint> {
        &self.unique_constraints
    }

    pub fn get_foreign_keys(&self) -> &Vec<ForeignKeyConstraint> {
        &self.foreign_keys
    }

    pub fn add_foreign_key_constraint<LC, FC, FT, N>(
        &mut self,
        local_columns: &[LC],
        foreign_columns: &[FC],
        foreign_table: FT,
        options: HashMap<String, Value>,
        on_update: Option<ForeignKeyReferentialAction>,
        on_delete: Option<ForeignKeyReferentialAction>,
        name: Option<N>,
    ) -> Result<()>
    where
        LC: IntoIdentifier,
        FC: IntoIdentifier,
        FT: IntoIdentifier,
        N: IntoIdentifier,
    {
        let name = name.map(|n| n.into_identifier()).unwrap_or_else(|| {
            let mut names = vec![self.get_name().into_owned()];
            for local_column in local_columns {
                names.push(local_column.to_string());
            }

            generate_identifier_name(&names, "fk", self.get_max_identifier_length())
                .into_identifier()
        });

        for local_column in local_columns {
            if !self.has_column(&local_column.to_string()) {
                return Err(Error::column_does_not_exist(local_column, &self.get_name()));
            }
        }

        self.create_foreign_key_constraint(
            local_columns,
            foreign_columns,
            foreign_table,
            options,
            on_update,
            on_delete,
            name,
        )
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

    pub fn get_primary_key_columns(&self) -> Option<Vec<&Column>> {
        self.indices
            .iter()
            .find(|&index| index.is_primary())
            .map(|i| i.get_columns())
            .map(|cols| {
                self.columns
                    .iter()
                    .filter(|c| cols.contains(&c.get_name().into_owned()))
                    .collect()
            })
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

    pub fn set_charset<S: AsRef<str>, I: Into<Option<S>>>(&mut self, charset: I) {
        self.charset = charset.into().map(|s| s.as_ref().to_string());
    }

    pub fn get_collation(&self) -> Option<String> {
        self.collation.clone()
    }

    pub fn set_collation<S: AsRef<str>, I: Into<Option<S>>>(&mut self, collation: I) {
        self.collation = collation.into().map(|s| s.as_ref().to_string());
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

    /// Normalizes a given identifier.
    /// Trims quotes and lowercases the given identifier.
    fn normalize_identifier(&self, identifier: &str) -> String {
        if identifier.is_empty() {
            "".to_string()
        } else {
            self.trim_quotes(identifier)
        }
    }

    fn create_index<S: IntoIdentifier + Clone>(
        &self,
        column_names: &[S],
        index_name: &str,
        is_unique: bool,
        is_primary: bool,
        flags: Vec<String>,
        options: HashMap<String, Value>,
    ) -> Result<Index> {
        let regex = Regex::new("[^a-zA-Z0-9_]+").unwrap();
        if regex.is_match(&self.normalize_identifier(index_name)) {
            Err(Error::index_definition_invalid("name"))
        } else {
            for column_name in column_names {
                if !self.has_column(column_name) {
                    return Err(Error::column_does_not_exist(
                        &column_name.into_identifier().get_name(),
                        &self.get_name(),
                    ));
                }
            }

            Ok(Index::new(
                index_name,
                column_names,
                is_unique,
                is_primary,
                &flags,
                options,
            ))
        }
    }

    fn create_foreign_key_constraint<LC, FC, FT>(
        &mut self,
        local_columns: &[LC],
        foreign_columns: &[FC],
        foreign_table: FT,
        options: HashMap<String, Value>,
        on_update: Option<ForeignKeyReferentialAction>,
        on_delete: Option<ForeignKeyReferentialAction>,
        name: Identifier,
    ) -> Result<()>
    where
        LC: IntoIdentifier,
        FC: IntoIdentifier,
        FT: IntoIdentifier,
    {
        let mut constraint = ForeignKeyConstraint::new(
            local_columns,
            foreign_columns,
            foreign_table,
            options,
            on_update,
            on_delete,
        );

        let mut names = vec![self.get_name().into_owned()];
        for local_column in local_columns {
            names.push(local_column.to_string());
        }

        constraint.set_name(&if name.is_empty() {
            generate_identifier_name(&names, "fk", self.get_max_identifier_length())
        } else {
            name.get_name().into_owned()
        });

        /* Add an implicit index (defined by the DBAL) on the foreign key
        columns. If there is already a user-defined index that fulfills these
        requirements drop the request. In the case of "new" calling
        this method during hydration from schema-details, all the explicitly
        added indexes lead to duplicates. This creates computation overhead in
        this case, however no duplicate indexes are ever added (based on
        columns). */
        let index_name = generate_identifier_name(&names, "idx", self.get_max_identifier_length());
        let index_candidate = self.create_index(
            constraint.get_local_columns(),
            &index_name,
            false,
            false,
            vec![],
            HashMap::default(),
        )?;
        self.add_foreign_key(constraint);

        for index in &self.indices {
            if index_candidate.is_fulfilled_by(index) {
                return Ok(());
            }
        }

        self.add_index(index_candidate);
        Ok(())
    }
}

impl_asset!(Table, name);
