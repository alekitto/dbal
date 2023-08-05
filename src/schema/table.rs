use crate::schema::asset::{generate_identifier_name, impl_asset, Asset};
use crate::schema::schema_config::SchemaConfig;
use crate::schema::{
    Column, ColumnList, FKConstraintList, ForeignKeyConstraint, ForeignKeyReferentialAction,
    Identifier, Index, IndexList, IntoIdentifier, NamedListIndex, UniqueConstraint,
};
use crate::{Error, Result, Value};
use itertools::Itertools;
use regex::Regex;
use std::collections::HashMap;
use std::slice::Iter;
use std::vec::IntoIter;

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

#[derive(Clone, Debug, Default)]
pub struct TableList {
    inner: Vec<Table>,
}

impl TableList {
    pub fn push(&mut self, table: Table) {
        self.inner.push(table)
    }

    pub fn remove<T: NamedListIndex>(&mut self, index: T) {
        let pos = if index.is_usize() {
            index.as_usize()
        } else {
            let idx = index.as_str();
            let Some((p, _)) = self.inner.iter().find_position(|p| p.get_name() == idx) else {
                return;
            };

            p
        };

        self.inner.remove(pos);
    }

    pub fn has<T: NamedListIndex>(&self, index: T) -> bool {
        self.get(index).is_some()
    }

    pub fn filter<P>(&self, predicate: P) -> impl Iterator<Item = &Table>
    where
        Self: Sized,
        P: FnMut(&&Table) -> bool,
    {
        self.inner.iter().filter(predicate)
    }

    pub fn get<T: NamedListIndex>(&self, index: T) -> Option<&Table> {
        if index.is_usize() {
            self.inner.get(index.as_usize())
        } else {
            let name = index.as_str().to_lowercase();
            self.inner
                .iter()
                .find(|c| c.get_name().to_lowercase() == name)
        }
    }

    pub fn get_mut<T: NamedListIndex>(&mut self, index: T) -> Option<&mut Table> {
        if index.is_usize() {
            self.inner.get_mut(index.as_usize())
        } else {
            let name = index.as_str().to_lowercase();
            self.inner
                .iter_mut()
                .find(|c| c.get_name().to_lowercase() == name)
        }
    }

    pub fn get_position<T: NamedListIndex>(&self, index: T) -> Option<(usize, &Table)> {
        if index.is_usize() {
            let idx = index.as_usize();
            self.inner.get(idx).map(|i| (idx, i))
        } else {
            let name = index.as_str().to_lowercase();
            self.inner
                .iter()
                .find_position(|c| c.get_name().to_lowercase() == name)
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = String> + '_ {
        self.inner.iter().map(|c| c.get_name().into_owned())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> Iter<Table> {
        self.into_iter()
    }
}

impl IntoIterator for TableList {
    type Item = Table;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a TableList {
    type Item = &'a Table;
    type IntoIter = Iter<'a, Table>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl From<Vec<Table>> for TableList {
    fn from(value: Vec<Table>) -> Self {
        Self { inner: value }
    }
}

impl From<TableList> for Vec<Table> {
    fn from(value: TableList) -> Self {
        value.inner
    }
}

#[derive(Clone, Debug, IntoIdentifier)]
pub struct Table {
    name: Identifier,
    columns: ColumnList,
    indices: IndexList,
    unique_constraints: Vec<UniqueConstraint>,
    foreign_keys: FKConstraintList,
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
    schema_config: SchemaConfig,
}

impl Table {
    pub fn new<I: IntoIdentifier>(name: I) -> Self {
        Self {
            name: name.into_identifier(),
            columns: ColumnList::default(),
            indices: IndexList::default(),
            unique_constraints: vec![],
            foreign_keys: FKConstraintList::default(),
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
            schema_config: SchemaConfig::default(),
        }
    }

    pub fn template(&self) -> Self {
        Self {
            name: self.name.clone(),
            columns: ColumnList::default(),
            indices: IndexList::default(),
            unique_constraints: vec![],
            foreign_keys: FKConstraintList::default(),
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
            schema_config: self.schema_config.clone(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn get_table_name(&self) -> &Identifier {
        &self.name
    }

    pub fn columns(&self) -> &ColumnList {
        &self.columns
    }

    pub fn columns_mut(&mut self) -> &mut ColumnList {
        &mut self.columns
    }

    pub fn add_column<IC: Into<Column>>(&mut self, column: IC) {
        self.columns.push(column.into())
    }

    pub fn add_columns<T: Iterator<Item = Column>>(&mut self, columns: T) {
        for column in columns {
            self.add_column(column)
        }
    }

    pub fn has_column<T: IntoIdentifier>(&self, name: T) -> bool {
        self.columns.has(name.into_identifier())
    }

    pub fn drop_column<T: IntoIdentifier>(&mut self, name: T) {
        self.columns.remove(name.into_identifier());
    }

    pub fn get_column<T: IntoIdentifier>(&self, name: T) -> Option<&Column> {
        self.columns.get(name.into_identifier())
    }

    pub fn get_column_mut<T: IntoIdentifier>(&mut self, name: T) -> Option<&mut Column> {
        self.columns.get_mut(name.into_identifier())
    }

    pub fn add_index<I: Into<Index>>(&mut self, index: I) {
        let mut index = index.into();
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

    /// Renames an index.
    pub fn rename_index<S, N>(&mut self, old_name: S, new_name: Option<N>) -> Result<()>
    where
        S: AsRef<str> + IntoIdentifier + Clone,
        N: AsRef<str> + IntoIdentifier + Clone,
    {
        let old_name = self.normalize_identifier(old_name.as_ref());
        let normalized_new_name = new_name
            .clone()
            .map(|n| n.as_ref().to_string())
            .unwrap_or_default();

        if old_name == normalized_new_name {
            return Ok(());
        }

        let Some(old_index) = self
            .indices
            .iter()
            .find(|idx| idx.get_name() == old_name)
            .cloned()
        else {
            return Err(Error::index_does_not_exist(old_name, &self.name));
        };

        let new_index_name = new_name.clone().map(|n| n.to_string());
        if old_index.is_primary() {
            self.drop_index(old_index.into_identifier());
            self.set_primary_key(old_index.get_columns().as_slice(), new_index_name)?;

            return Ok(());
        }

        if self.has_index(&normalized_new_name) {
            return Err(Error::index_already_exists(normalized_new_name, &self.name));
        }

        self.drop_index(old_index.into_identifier());
        self.add_index(Index::new(
            new_name.map(|n| n.as_ref().to_string()).unwrap_or_default(),
            old_index.get_columns().as_slice(),
            old_index.is_unique(),
            false,
            old_index.get_flags().as_slice(),
            old_index.get_options().clone(),
        ));

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

    pub fn add_indices<T: Iterator<Item = Index>>(&mut self, indices: T) {
        for index in indices {
            self.add_index(index)
        }
    }

    pub fn drop_index<T: IntoIdentifier>(&mut self, name: T) {
        let name = name.into_identifier();
        let name = name.get_name();
        if let Some(pos) = self
            .indices
            .iter()
            .position(|index| index.get_name() == name)
        {
            self.indices.remove(pos);
        }
    }

    pub fn has_primary_key(&self) -> bool {
        self.indices.iter().any(|idx| idx.is_primary())
    }

    pub fn indices(&self) -> &IndexList {
        &self.indices
    }

    pub fn has_index<T: IntoIdentifier>(&self, index_name: T) -> bool {
        let name = index_name.into_identifier();
        let name = name.get_name();
        self.indices.iter().any(|i| i.get_name() == name)
    }

    pub fn get_index<T: IntoIdentifier>(&self, index_name: T) -> Option<&Index> {
        let name = index_name.into_identifier();
        let name = name.get_name();
        self.indices.iter().find(|i| i.get_name() == name)
    }

    /// Sets the Primary Key.
    pub fn set_primary_key<C: IntoIdentifier + Clone>(
        &mut self,
        column_names: &[C],
        index_name: Option<String>,
    ) -> Result<()> {
        let index_name = index_name.unwrap_or("primary".to_string());
        self.add_index(self.create_index(
            column_names,
            index_name.as_ref(),
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

    fn get_max_identifier_length(&self) -> usize {
        self.schema_config.max_identifier_length
    }

    pub fn get_unique_constraints(&self) -> &Vec<UniqueConstraint> {
        &self.unique_constraints
    }

    pub fn get_foreign_keys(&self) -> &FKConstraintList {
        &self.foreign_keys
    }

    pub fn has_foreign_key<T: IntoIdentifier>(&self, fk_name: T) -> bool {
        let name = fk_name.into_identifier();
        let name = name.get_name();
        self.foreign_keys.iter().any(|i| i.get_name() == name)
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

    pub fn add_foreign_key<T: Into<ForeignKeyConstraint>>(&mut self, constraint: T) -> Result<()> {
        let constraint = constraint.into();
        let local_columns = constraint.get_local_columns().clone();
        self.foreign_keys.push(constraint);

        let mut names = vec![self.get_name().into_owned()];
        for local_column in &local_columns {
            names.push(local_column.to_string());
        }

        // Add an implicit index (creed-defined) on the foreign key
        // columns. If there is already a user-defined index that fulfills these
        // requirements drop the request. In the case of "new" calling
        // this method during hydration from schema-details, all the explicitly
        // added indexes lead to duplicates. This creates computation overhead in
        // this case, however no duplicate indexes are ever added (based on
        // columns).
        let index_name = generate_identifier_name(&names, "idx", self.get_max_identifier_length());
        let index_candidate = self.create_index(
            &local_columns,
            &index_name,
            false,
            false,
            vec![],
            HashMap::default(),
        )?;

        if !self
            .indices
            .iter()
            .any(|i| index_candidate.is_fulfilled_by(i))
        {
            self.add_index(index_candidate);
        }

        Ok(())
    }

    pub fn add_foreign_keys<I: Into<ForeignKeyConstraint>, T: Iterator<Item = I>>(
        &mut self,
        constraints: T,
    ) -> Result<()> {
        for constraint in constraints {
            self.add_foreign_key(constraint)?;
        }

        Ok(())
    }

    pub fn add_foreign_keys_raw<T: Iterator<Item = ForeignKeyConstraint>>(
        &mut self,
        constraints: T,
    ) {
        for constraint in constraints {
            self.foreign_keys.push(constraint);
        }
    }

    pub fn get_comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn remove_comment(&mut self) {
        self.comment = None;
    }

    pub fn set_comment<S: AsRef<str>>(&mut self, comment: S) {
        let comment = comment.as_ref();
        self.comment = if comment.is_empty() {
            None
        } else {
            Some(comment.to_string())
        };
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

    pub fn set_schema_config(&mut self, schema_config: SchemaConfig) {
        self.schema_config = schema_config;
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
        let mut names = vec![self.get_name().into_owned()];
        for local_column in local_columns {
            names.push(local_column.to_string());
        }

        let mut constraint = ForeignKeyConstraint::new(
            local_columns,
            foreign_columns,
            foreign_table,
            options,
            on_update,
            on_delete,
        );

        constraint.set_name(&if name.is_empty() {
            generate_identifier_name(&names, "fk", self.get_max_identifier_length())
        } else {
            name.get_name().into_owned()
        });

        self.add_foreign_key(constraint)?;

        Ok(())
    }
}

impl_asset!(Table, name);
