use crate::driver::statement::Statement;
use crate::driver::statement_result::StatementResult;
use crate::platform::{default, CreateFlags, DatabasePlatform};
use crate::r#type;
use crate::r#type::{IntoType, TypeManager, TypePtr};
use crate::schema::{
    Asset, Column, ColumnData, ColumnDiff, Comparator, ForeignKeyConstraint,
    ForeignKeyReferentialAction, Identifier, Index, IntoIdentifier, Schema, SchemaDiff, Sequence,
    Table, TableDiff, TableOptions, UniqueConstraint, View,
};
use crate::util::{function_name, ToSqlStatementList};
use crate::{
    params, AsyncResult, Connection, Error, Result, Row, SchemaColumnDefinitionEvent, Value,
};
use regex::Regex;
use std::collections::HashMap;
use std::ops::Index as _;
use std::sync::Arc;

pub(crate) async fn get_database(conn: &Connection, method_name: &str) -> Result<String> {
    if let Some(database) = conn.get_database().await {
        Ok(database)
    } else {
        Err(Error::database_required(method_name))
    }
}

pub(crate) fn string_from_value(conn: &Connection, value: Result<&Value>) -> Result<String> {
    Ok(match conn.convert_value(value?, r#type::STRING)? {
        Value::NULL => "".to_string(),
        Value::String(n) => n,
        _ => unreachable!(),
    })
}

fn _exec_sql<S: ToSqlStatementList>(connection: &Connection, sql: S) -> AsyncResult<()> {
    let sql = sql.to_statement_list();
    Box::pin(async move {
        for stmt in sql? {
            connection.prepare(stmt)?.execute(params!()).await?;
        }

        Ok(())
    })
}

/// Given a table comment this method tries to extract a typehint for Doctrine Type, or returns
/// the type given as default.
///
/// # Internal
/// This method should be only used from within the AbstractSchemaManager class hierarchy.
pub fn extract_type_from_comment<I: IntoType>(
    comment: Option<String>,
    current_type: I,
) -> Result<TypePtr> {
    let type_regex = Regex::new("\\(CRType:([^)]+)\\)").unwrap();
    let current_type = current_type.into_type();
    comment
        .and_then(|comment| {
            type_regex
                .captures(&comment)
                .map(|cap| cap.index(1).to_owned())
        })
        .map(|name| TypeManager::get_instance().get_type_by_name(&name))
        .unwrap_or(current_type)
}

/// # Internal
/// This method should be only used from within the AbstractSchemaManager class hierarchy.
pub fn remove_type_from_comment<I: IntoType>(
    comment: Option<String>,
    current_type: I,
) -> Option<String> {
    comment.map(|comment| {
        let current_type = current_type.into_type().expect("Invalid type provided");
        comment.replace(&format!("(CRType:{})", current_type.get_name()), "")
    })
}

pub trait SchemaManager: Sync {
    /// Gets the database connection.
    fn get_connection(&self) -> &Connection;

    /// As &dyn SchemaManager
    fn as_dyn(&self) -> &dyn SchemaManager;

    /// Gets the database platform instance.
    ///
    /// # Errors
    ///
    /// The function returns an error if the connection is not active.
    fn get_platform(&self) -> Result<Arc<Box<dyn DatabasePlatform + Send + Sync>>> {
        self.get_connection().get_platform()
    }

    /// Returns the SQL statement(s) to create a table with the specified name, columns and constraints
    /// on this platform.
    fn get_create_table_sql(
        &self,
        table: &Table,
        create_flags: Option<CreateFlags>,
    ) -> Result<Vec<String>> {
        default::get_create_table_sql(
            self.as_dyn(),
            table,
            create_flags.or(Some(
                CreateFlags::CREATE_INDEXES | CreateFlags::CREATE_FOREIGN_KEYS,
            )),
        )
    }

    fn get_create_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>> {
        default::get_create_tables_sql(self.as_dyn(), tables)
    }

    /// Returns the SQL used to create a table.
    ///
    /// # Internal
    /// The method should be only used from within the Schema trait.
    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>> {
        default::_get_create_table_sql(self.as_dyn(), name, columns, options)
    }

    fn get_create_temporary_table_snippet_sql(&self) -> Result<String> {
        default::get_create_temporary_table_snippet_sql()
    }

    /// Returns the SQL to create a sequence on this platform.
    #[allow(unused_variables)]
    fn get_create_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sequences are not supported by this platform",
        ))
    }

    /// Returns the SQL to create an index on a table on this platform.
    fn get_create_index_sql(&self, index: &Index, table: &dyn IntoIdentifier) -> Result<String> {
        default::get_create_index_sql(self.as_dyn(), index, table)
    }

    /// Adds additional flags for index generation.
    fn get_create_index_sql_flags(&self, index: &Index) -> String {
        default::get_create_index_sql_flags(index)
    }

    /// Returns the SQL to create an unnamed primary key constraint.
    fn get_create_primary_key_sql(
        &self,
        index: &Index,
        table: &dyn IntoIdentifier,
    ) -> Result<String> {
        default::get_create_primary_key_sql(self.as_dyn(), index, table)
    }

    /// Returns the SQL to create a named schema.
    fn get_create_schema_sql(&self, schema_name: &dyn IntoIdentifier) -> Result<String> {
        default::get_create_schema_sql(self.get_platform()?.as_dyn(), schema_name)
    }

    /// Returns the SQL to create a unique constraint on a table on this platform.
    fn get_create_unique_constraint_sql(
        &self,
        constraint: &UniqueConstraint,
        table_name: &dyn IntoIdentifier,
    ) -> Result<String> {
        default::get_create_unique_constraint_sql(
            self.get_platform()?.as_dyn(),
            constraint,
            table_name,
        )
    }

    /// Returns the SQL to create a new foreign key.
    fn get_create_foreign_key_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
        table: &dyn IntoIdentifier,
    ) -> Result<String> {
        default::get_create_foreign_key_sql(self.as_dyn(), foreign_key, table)
    }

    fn get_create_view_sql(&self, view: &View) -> Result<String> {
        default::get_create_view_sql(self.get_platform()?.as_dyn(), view)
    }

    /// Returns the SQL to create a new database.
    fn get_create_database_sql(&self, name: &Identifier) -> Result<String> {
        default::get_create_database_sql(self.get_platform()?.as_dyn(), name)
    }

    /// Obtains DBMS specific SQL code portion needed to set the CHARACTER SET
    /// of a column declaration to be used in statements like CREATE TABLE.
    /// # Internal
    #[allow(unused_variables)]
    fn get_column_charset_declaration_sql(&self, charset: &str) -> String {
        default::get_column_charset_declaration_sql()
    }

    /// Gets the SQL query to retrieve the databases list.
    ///
    /// # Errors
    ///
    /// Returns an error if the feature is not supported by the current platform.
    fn get_list_databases_sql(&self) -> Result<String> {
        Err(Error::platform_feature_unsupported("list databases"))
    }

    /// Gets the SQL query to retrieve all the tables in the current database.
    ///
    /// # Errors
    ///
    /// Returns an error if the feature is not supported by the current platform.
    fn get_list_tables_sql(&self) -> Result<String> {
        Err(Error::platform_feature_unsupported("list tables"))
    }

    /// Gets the SQL query to retrieve all the sequences in the given database.
    ///
    /// # Errors
    ///
    /// Returns an error if the feature is not supported by the current platform.
    #[allow(unused_variables)]
    fn get_list_sequences_sql(&self, database: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported("list sequences"))
    }

    /// Gets the SQL query to list all the columns in a given table.
    ///
    /// # Errors
    ///
    /// Returns an error if the feature is not supported by the current platform.
    #[allow(unused_variables)]
    fn get_list_table_columns_sql(&self, table: &str, database: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported("list table columns"))
    }

    /// Returns the list of indexes for the current database.
    /// The current database parameter is optional but will always be passed
    /// when using the SchemaManager API and is the database the given table is in.
    ///
    /// Attention: Some platforms only support currentDatabase when they
    /// re connected with that database. Cross-database information schema
    /// requests may be impossible.
    ///
    /// # Errors
    ///
    /// Returns an error if the feature is not supported by the current platform.
    #[allow(unused_variables)]
    fn get_list_table_indexes_sql(&self, table: &str, database: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported("list table indexes"))
    }

    #[allow(unused_variables)]
    fn get_list_table_foreign_keys_sql(&self, table: &str, database: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "list table foreign keys",
        ))
    }

    #[allow(unused_variables)]
    fn get_list_table_constraints_sql(&self, table: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "list table constraints",
        ))
    }

    fn get_comment_on_table_sql(&self, table_name: &Identifier, comment: &str) -> Result<String> {
        default::get_comment_on_table_sql(self.get_platform()?.as_dyn(), table_name, comment)
    }

    fn get_comment_on_column_sql(
        &self,
        table_name: &dyn IntoIdentifier,
        column: &dyn IntoIdentifier,
        comment: &str,
    ) -> Result<String> {
        default::get_comment_on_column_sql(
            self.get_platform()?.as_dyn(),
            table_name,
            column,
            comment,
        )
    }

    /// Returns the SQL to create inline comment on a column.
    fn get_inline_column_comment_sql(&self, comment: &str) -> Result<String> {
        default::get_inline_column_comment_sql(self.get_platform()?.as_dyn(), comment)
    }

    /// Generates SQL statements that can be used to apply the diff.
    fn get_alter_schema_sql(&self, diff: SchemaDiff) -> Result<Vec<String>> {
        diff.to_sql(self)
    }

    /// Gets the SQL statements for altering an existing table.
    /// This method returns an array of SQL statements, since some platforms need several statements.
    #[allow(unused_variables)]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>> {
        Err(Error::platform_feature_unsupported("alter table"))
    }

    /// Returns the SQL to change a sequence on this platform.
    #[allow(unused_variables)]
    fn get_alter_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sequences are not supported by this platform",
        ))
    }

    /// Generates a Truncate Table SQL statement for a given table.
    ///
    /// Cascade is not supported on many platforms but would optionally cascade the truncate by
    /// following the foreign keys.
    #[allow(unused_variables)]
    fn get_truncate_table_sql(
        &self,
        table_name: &dyn IntoIdentifier,
        cascade: bool,
    ) -> Result<String> {
        default::get_truncate_table_sql(self.as_dyn(), table_name)
    }

    /// Returns the SQL snippet to drop an existing database.
    fn get_drop_database_sql(&self, name: &str) -> Result<String> {
        default::get_drop_database_sql(self.as_dyn(), name)
    }

    /// Returns the SQL snippet to drop a schema.
    fn get_drop_schema_sql(&self, schema_name: &str) -> Result<String> {
        default::get_drop_schema_sql(self.as_dyn(), schema_name)
    }

    /// Returns the SQL snippet to drop an existing table.
    fn get_drop_table_sql(&self, table_name: &dyn IntoIdentifier) -> Result<String> {
        default::get_drop_table_sql(self.as_dyn(), table_name)
    }

    fn get_drop_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>> {
        default::get_drop_tables_sql(self.as_dyn(), tables)
    }

    /// Returns the SQL to safely drop a temporary table WITHOUT implicitly committing an open transaction.
    fn get_drop_temporary_table_sql(&self, table: &Identifier) -> Result<String> {
        default::get_drop_temporary_table_sql(self.as_dyn(), table)
    }

    /// Returns the SQL to drop an index from a table.
    #[allow(unused_variables)]
    fn get_drop_index_sql(&self, index: &Identifier, table: &Identifier) -> Result<String> {
        default::get_drop_index_sql(self.get_platform()?.as_dyn(), index)
    }

    /// Returns the SQL to drop a unique constraint.
    fn get_drop_unique_constraint_sql(
        &self,
        name: &Identifier,
        table_name: &Identifier,
    ) -> Result<String> {
        default::get_drop_unique_constraint_sql(self.as_dyn(), name, table_name)
    }

    /// Returns the SQL to drop a constraint.
    ///
    /// # Internal
    /// The method should be only used from within the Platform trait.
    fn get_drop_constraint_sql(
        &self,
        constraint: &Identifier,
        table_name: &Identifier,
    ) -> Result<String> {
        default::get_drop_constraint_sql(self.get_platform()?.as_dyn(), constraint, table_name)
    }

    /// Returns the SQL to drop a foreign key.
    fn get_drop_foreign_key_sql(
        &self,
        foreign_key: &dyn IntoIdentifier,
        table_name: &dyn IntoIdentifier,
    ) -> Result<String> {
        default::get_drop_foreign_key_sql(self.get_platform()?.as_dyn(), foreign_key, table_name)
    }

    /// Returns the SQL snippet to drop an existing sequence.
    fn get_drop_sequence_sql(&self, sequence: &dyn IntoIdentifier) -> Result<String> {
        default::get_drop_sequence_sql(self.get_platform()?.as_dyn(), sequence)
    }

    fn get_drop_view_sql(&self, name: &Identifier) -> Result<String> {
        default::get_drop_view_sql(self.get_platform()?.as_dyn(), name)
    }

    /// Lists the available databases for this connection.
    fn list_databases(&self) -> AsyncResult<Vec<Identifier>> {
        default::list_databases(self.as_dyn())
    }

    /// Returns a list of the names of all schemata in the current database.
    fn list_schema_names(&self) -> AsyncResult<Vec<Identifier>> {
        Box::pin(async move { Err(Error::platform_feature_unsupported("list schema names")) })
    }

    /// Lists the available sequences for this connection.
    fn list_sequences(&self) -> AsyncResult<Vec<Sequence>> {
        default::list_sequences(self.as_dyn())
    }

    /// Lists the columns for a given table.
    fn list_table_columns(&self, table: &str, database: Option<&str>) -> AsyncResult<Vec<Column>> {
        let table = table.to_string();
        let database = database.map(ToString::to_string);

        Box::pin(async move { default::list_table_columns(self.as_dyn(), table, database).await })
    }

    /// Lists the indexes for a given table returning an array of Index instances.
    /// Keys of the portable indexes list are all lower-cased.
    fn list_table_indexes(&self, table: &str) -> AsyncResult<Vec<Index>> {
        let table = table.to_string();

        Box::pin(async move { default::list_table_indexes(self.as_dyn(), table).await })
    }

    /// Whether all the given tables exist.
    fn tables_exist(&self, names: &[&str]) -> AsyncResult<bool> {
        let names = names.iter().map(|s| s.to_lowercase()).collect::<Vec<_>>();

        Box::pin(async move { default::tables_exist(self.as_dyn(), names).await })
    }

    /// Returns a list of all tables in the current database.
    fn list_table_names(&self) -> AsyncResult<Vec<String>> {
        Box::pin(async move { default::list_table_names(self.as_dyn()).await })
    }

    /// Lists the tables for this connection.
    fn list_tables(&self) -> AsyncResult<Vec<Table>> {
        Box::pin(async move { default::list_tables(self.as_dyn()).await })
    }

    fn list_table_details(&self, name: &str) -> AsyncResult<Table> {
        let name = name.to_string();

        Box::pin(async move { default::list_table_details(self.as_dyn(), name).await })
    }

    /// An extension point for those platforms where case sensitivity of the object
    /// name depends on whether it's quoted.
    ///
    /// Such platforms should convert a possibly quoted name into a value of the corresponding case.
    fn normalize_name(&self, name: &str) -> String {
        Identifier::new(name, false).get_name().into_owned()
    }

    /// Selects names of tables in the specified database.
    /// # Abstract
    #[allow(unused_variables)]
    fn select_table_names(&self, database_name: &str) -> AsyncResult<StatementResult> {
        Box::pin(async move { Err(Error::platform_feature_unsupported(function_name!())) })
    }

    /// Selects definitions of table columns in the specified database.
    /// If the table name is specified, narrows down the selection to this table.
    /// # Abstract
    #[allow(unused_variables)]
    fn select_table_columns(
        &self,
        database_name: &str,
        table_name: Option<&str>,
    ) -> AsyncResult<StatementResult> {
        Box::pin(async move { Err(Error::platform_feature_unsupported(function_name!())) })
    }

    /// Selects definitions of index columns in the specified database.
    /// If the table name is specified, narrows down the selection to this table.
    #[allow(unused_variables)]
    fn select_index_columns(
        &self,
        database_name: &str,
        table_name: Option<&str>,
    ) -> AsyncResult<StatementResult> {
        Box::pin(async move { Err(Error::platform_feature_unsupported(function_name!())) })
    }

    /// Selects definitions of foreign key columns in the specified database.
    /// If the table name is specified, narrows down the selection to this table.
    #[allow(unused_variables)]
    fn select_foreign_key_columns(
        &self,
        database_name: &str,
        table_name: Option<&str>,
    ) -> AsyncResult<StatementResult> {
        Box::pin(async move { Err(Error::platform_feature_unsupported(function_name!())) })
    }

    fn quote_string_literal(&self, str: &str) -> String {
        self.get_platform()
            .map(|platform| platform.quote_string_literal(str))
            .unwrap_or_else(|_| {
                let c = "'";
                format!("{}{}{}", c, str.replace(c, &c.repeat(2)), c)
            })
    }

    /// Fetches definitions of table columns in the specified database and returns them grouped by table name.
    /// # Protected
    fn fetch_table_columns_by_table(
        &self,
        database_name: &str,
    ) -> AsyncResult<HashMap<String, Vec<Row>>> {
        let database_name = database_name.to_string();
        Box::pin(async move {
            default::fetch_table_columns_by_table(self.as_dyn(), database_name).await
        })
    }

    /// Fetches definitions of index columns in the specified database and returns them grouped by table name.
    /// # Protected
    fn fetch_index_columns_by_table(
        &self,
        database_name: &str,
    ) -> AsyncResult<HashMap<String, Vec<Row>>> {
        let database_name = database_name.to_string();
        Box::pin(async move {
            default::fetch_index_columns_by_table(self.as_dyn(), database_name).await
        })
    }

    /// Fetches definitions of foreign key columns in the specified database and returns them grouped by table name.
    /// # Protected
    fn fetch_foreign_key_columns_by_table(
        &self,
        database_name: &str,
    ) -> AsyncResult<HashMap<String, Vec<Row>>> {
        let database_name = database_name.to_string();
        Box::pin(async move {
            default::fetch_foreign_key_columns_by_table(self.as_dyn(), database_name).await
        })
    }

    /// Fetches table options for the tables in the specified database and returns them grouped by table name.
    /// If the table name is specified, narrows down the selection to this table.
    /// # Protected
    #[allow(unused_variables)]
    fn fetch_table_options_by_table(
        &self,
        database_name: &str,
        table_name: Option<&str>,
    ) -> AsyncResult<HashMap<String, Vec<Row>>> {
        Box::pin(async move { Err(Error::platform_feature_unsupported(function_name!())) })
    }

    /// Returns the SQL to list all views of a database or user.
    #[allow(unused_variables)]
    fn get_list_views_sql(&self, database: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported("list views"))
    }

    /// Introspects the table with the given name.
    fn introspect_table(&self, name: &str) -> AsyncResult<Table> {
        let name = name.to_string();
        Box::pin(async move { default::introspect_table(self.as_dyn(), name).await })
    }

    /// Lists the views this connection has.
    fn list_views(&self) -> AsyncResult<Vec<View>> {
        Box::pin(async move { default::list_views(self.as_dyn()).await })
    }

    /// Lists the foreign keys for the given table.
    fn list_table_foreign_keys(&self, table: &str) -> AsyncResult<Vec<ForeignKeyConstraint>> {
        let table = table.to_string();
        Box::pin(async move { default::list_table_foreign_keys(self.as_dyn(), table).await })
    }

    /// Obtains DBMS specific SQL code portion needed to declare a generic type
    /// column to be used in statements like CREATE TABLE.
    fn get_column_declaration_sql(&self, name: &str, column: &ColumnData) -> Result<String> {
        default::get_column_declaration_sql(self.as_dyn(), name, column)
    }

    /// Adds condition for partial index.
    fn get_partial_index_sql(&self, index: &Index) -> Result<String> {
        default::get_partial_index_sql(self.get_platform()?.as_dyn(), index)
    }

    /// Gets the comment of a passed column modified by potential doctrine type comment hints.
    fn get_column_comment(&self, column: &Column) -> Result<String> {
        default::get_column_comment(self.get_platform()?.as_dyn(), column)
    }

    /// Drops a database.
    ///
    /// # Note
    /// You cannot drop the database this SchemaManager is currently connected to.
    fn drop_database(&self, database: &str) -> AsyncResult<()> {
        let database = database.to_string();
        _exec_sql(self.get_connection(), self.get_drop_database_sql(&database))
    }

    /// Drops a schema.
    fn drop_schema(&self, schema_name: &str) -> AsyncResult<()> {
        let schema_name = schema_name.to_string();
        _exec_sql(
            self.get_connection(),
            self.get_drop_schema_sql(&schema_name),
        )
    }

    /// Drops the given table.
    fn drop_table(&self, name: &dyn IntoIdentifier) -> AsyncResult<()> {
        let name = name.into_identifier();
        _exec_sql(self.get_connection(), self.get_drop_table_sql(&name))
    }

    /// Drops the index from the given table.
    fn drop_index(
        &self,
        index: &dyn IntoIdentifier,
        table: &dyn IntoIdentifier,
    ) -> AsyncResult<()> {
        let index = index.into_identifier();
        let table = table.into_identifier();

        _exec_sql(
            self.get_connection(),
            self.get_drop_index_sql(&index, &table),
        )
    }

    /// Drops a foreign key from a table.
    fn drop_foreign_key(
        &self,
        foreign_key: &dyn IntoIdentifier,
        table: &dyn IntoIdentifier,
    ) -> AsyncResult<()> {
        let foreign_key = foreign_key.into_identifier();
        let table = table.into_identifier();
        _exec_sql(
            self.get_connection(),
            self.get_drop_foreign_key_sql(&foreign_key, &table),
        )
    }

    /// Drops a sequence with a given name.
    fn drop_sequence(&self, name: &dyn IntoIdentifier) -> AsyncResult<()> {
        let name = name.into_identifier();
        _exec_sql(self.get_connection(), self.get_drop_sequence_sql(&name))
    }

    /// Drops the unique constraint from the given table.
    fn drop_unique_constraint(
        &self,
        name: &dyn IntoIdentifier,
        table_name: &dyn IntoIdentifier,
    ) -> AsyncResult<()> {
        let name = name.into_identifier();
        let table_name = table_name.into_identifier();
        _exec_sql(
            self.get_connection(),
            self.get_drop_unique_constraint_sql(&name, &table_name),
        )
    }

    /// Drops a view.
    fn drop_view(&self, name: &dyn IntoIdentifier) -> AsyncResult<()> {
        let name = name.into_identifier();
        _exec_sql(self.get_connection(), self.get_drop_view_sql(&name))
    }

    // fn create_schema_objects(&self, schema: &Schema) -> AsyncResult<()>
    // {
    //     Box::pin(async move {
    //         self._exec_sql(schema.to_sql(self)?).await
    //     })
    // }

    /// Creates a new database.
    fn create_database(&self, database: &dyn IntoIdentifier) -> AsyncResult<()> {
        let database = database.into_identifier();

        Box::pin(async move {
            _exec_sql(
                self.get_connection(),
                self.get_create_database_sql(&database)?,
            )
            .await
        })
    }

    /// Creates a new table.
    fn create_table(&self, table: &Table) -> AsyncResult<()> {
        let create_flags = CreateFlags::CREATE_INDEXES | CreateFlags::CREATE_FOREIGN_KEYS;
        _exec_sql(
            self.get_connection(),
            self.get_create_table_sql(table, Some(create_flags)),
        )
    }

    /// Creates a new sequence.
    fn create_sequence(&self, sequence: &Sequence) -> AsyncResult<()> {
        _exec_sql(
            self.get_connection(),
            self.get_create_sequence_sql(sequence),
        )
    }

    /// Creates a new index on a table.
    fn create_index(&self, index: &Index, table: &dyn IntoIdentifier) -> AsyncResult<()> {
        _exec_sql(
            self.get_connection(),
            self.get_create_index_sql(index, &table.into_identifier()),
        )
    }

    /// Creates a new foreign key.
    fn create_foreign_key(
        &self,
        foreign_key: &ForeignKeyConstraint,
        table: &dyn IntoIdentifier,
    ) -> AsyncResult<()> {
        let table = table.into_identifier();
        let table_name = table.get_name().to_string();
        let foreign_key = foreign_key.clone();

        Box::pin(async move {
            default::create_foreign_key(self.as_dyn(), foreign_key, &table_name).await
        })
    }

    /// Creates a unique constraint on a table.
    fn create_unique_constraint(
        &self,
        unique_constraint: &UniqueConstraint,
        table: &dyn IntoIdentifier,
    ) -> AsyncResult<()> {
        _exec_sql(
            self.get_connection(),
            self.get_create_unique_constraint_sql(unique_constraint, &table.into_identifier()),
        )
    }

    /// Creates a new view.
    fn create_view(&self, view: &View) -> AsyncResult<()> {
        _exec_sql(self.get_connection(), self.get_create_view_sql(view))
    }

    fn drop_schema_objects(&self, schema: &Schema) -> AsyncResult<()> {
        let sql = self
            .get_platform()
            .and_then(|platform| schema.to_drop_sql(&platform));

        _exec_sql(self.get_connection(), sql)
    }

    /// Alters an existing schema.
    fn alter_schema(&self, schema_diff: SchemaDiff) -> AsyncResult<()> {
        _exec_sql(self.get_connection(), schema_diff.to_sql(self))
    }

    /// Migrates an existing schema to a new schema.
    fn migrate_schema(&self, to_schema: Schema) -> AsyncResult<()> {
        Box::pin(async move {
            let comparator = self.create_comparator();
            let from_schema = self.introspect_schema().await?;
            let schema_diff = comparator.compare_schemas(&from_schema, &to_schema)?;

            self.alter_schema(schema_diff).await
        })
    }

    /// Alters an existing tables schema.
    fn alter_table(&self, mut table_diff: TableDiff) -> AsyncResult<()> {
        _exec_sql(
            self.get_connection(),
            self.get_alter_table_sql(&mut table_diff),
        )
    }

    /// Renames a given table to another name.
    fn rename_table(
        &self,
        name: &dyn IntoIdentifier,
        new_name: &dyn IntoIdentifier,
    ) -> AsyncResult<()> {
        let mut table_diff = TableDiff::new(name.into_identifier().get_name(), None);
        table_diff.new_name = Some(new_name.into_identifier().get_name().into_owned());

        self.alter_table(table_diff)
    }

    /// # Protected
    fn get_pre_alter_table_index_foreign_key_sql(
        &self,
        diff: &mut TableDiff,
    ) -> Result<Vec<String>> {
        default::get_pre_alter_table_index_foreign_key_sql(self.as_dyn(), diff)
    }

    /// # Protected
    fn get_post_alter_table_index_foreign_key_sql(&self, diff: &TableDiff) -> Result<Vec<String>> {
        default::get_post_alter_table_index_foreign_key_sql(self.as_dyn(), diff)
    }

    /// Obtains DBMS specific SQL code portion needed to set a CHECK constraint
    /// declaration to be used in statements like CREATE TABLE.
    fn get_check_declaration_sql(&self, definition: &[ColumnData]) -> Result<String> {
        default::get_check_declaration_sql(self.as_dyn(), definition)
    }

    fn get_check_field_declaration_sql(&self, definition: &ColumnData) -> Result<String> {
        default::get_check_field_declaration_sql(self.as_dyn(), definition)
    }

    /// Obtains DBMS specific SQL code portion needed to set an index
    /// declaration to be used in statements like CREATE TABLE.
    fn get_index_field_declaration_list_sql(&self, index: &Index) -> Result<String> {
        default::get_index_field_declaration_list_sql(self.get_platform()?.as_dyn(), index)
    }

    #[allow(unused_variables)]
    fn get_sequence_next_val_sql(&self, sequence: &str) -> Result<String> {
        Err(Error::platform_feature_unsupported("sequence next val"))
    }

    /// Returns the SQL for renaming an index on a table.
    ///
    /// # Arguments
    ///
    /// * `old_index_name` - The name of the index to rename from.
    /// * `index` - The definition of the index to rename to.
    /// * `tableName` - The table to rename the given index on.
    ///
    /// # Protected
    fn get_rename_index_sql(
        &self,
        old_index_name: &Identifier,
        index: &Index,
        table_name: &Identifier,
    ) -> Result<Vec<String>> {
        default::get_rename_index_sql(self.as_dyn(), old_index_name, index, table_name)
    }

    /// Compares the definitions of the given columns in the context of this platform.
    fn columns_equal(&self, column1: &Column, column2: &Column) -> Result<bool> {
        default::columns_equal(self.as_dyn(), column1, column2)
    }

    /// Gets declaration of a number of columns in bulk.
    fn get_column_declaration_list_sql(&self, columns: &[ColumnData]) -> Result<String> {
        default::get_column_declaration_list_sql(self.as_dyn(), columns)
    }

    /// Obtains DBMS specific SQL code portion needed to set a unique
    /// constraint declaration to be used in statements like CREATE TABLE.
    fn get_unique_constraint_declaration_sql(
        &self,
        name: &str,
        constraint: &UniqueConstraint,
    ) -> Result<String> {
        default::get_unique_constraint_declaration_sql(self.as_dyn(), name, constraint)
    }

    /// Obtains DBMS specific SQL code portion needed to set an index
    /// declaration to be used in statements like CREATE TABLE.
    fn get_index_declaration_sql(&self, name: &str, index: &Index) -> Result<String> {
        default::get_index_declaration_sql(self.as_dyn(), name, index)
    }

    /// Obtains DBMS specific SQL code portion needed to set the COLLATION
    /// of a column declaration to be used in statements like CREATE TABLE.
    fn get_column_collation_declaration_sql(&self, collation: &str) -> Result<String> {
        default::get_column_collation_declaration_sql(self.get_platform()?.as_dyn(), collation)
    }

    /// Obtain DBMS specific SQL code portion needed to set the FOREIGN KEY constraint
    /// of a column declaration to be used in statements like CREATE TABLE.
    fn get_foreign_key_declaration_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        default::get_foreign_key_declaration_sql(self.as_dyn(), foreign_key)
    }

    /// Returns the FOREIGN KEY query section dealing with non-standard options
    /// as MATCH, INITIALLY DEFERRED, ON UPDATE, ...
    fn get_advanced_foreign_key_options_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        default::get_advanced_foreign_key_options_sql(self.as_dyn(), foreign_key)
    }

    /// Returns the given referential action in uppercase if valid, otherwise throws an exception.
    fn get_foreign_key_referential_action_sql(
        &self,
        action: &ForeignKeyReferentialAction,
    ) -> Result<String> {
        default::get_foreign_key_referential_action_sql(action)
    }

    /// Obtains DBMS specific SQL code portion needed to set the FOREIGN KEY constraint
    /// of a column declaration to be used in statements like CREATE TABLE.
    fn get_foreign_key_base_declaration_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        default::get_foreign_key_base_declaration_sql(self.get_platform()?.as_dyn(), foreign_key)
    }

    /// Obtains DBMS specific SQL code portion needed to set an index
    /// declaration to be used in statements like CREATE TABLE.
    fn get_columns_field_declaration_list_sql(&self, columns: &[String]) -> Result<String> {
        default::get_columns_field_declaration_list_sql(columns)
    }

    /// # Protected
    fn on_schema_alter_table_add_column(
        &self,
        column: &Column,
        diff: &TableDiff,
        column_sql: Vec<String>,
    ) -> Result<(bool, Vec<String>)> {
        default::on_schema_alter_table_add_column(self.as_dyn(), column, diff, column_sql)
    }

    /// # Protected
    fn on_schema_alter_table_remove_column(
        &self,
        column: &Column,
        diff: &TableDiff,
        column_sql: Vec<String>,
    ) -> Result<(bool, Vec<String>)> {
        default::on_schema_alter_table_remove_column(self.as_dyn(), column, diff, column_sql)
    }

    /// # Protected
    fn on_schema_alter_table_change_column(
        &self,
        column_diff: &ColumnDiff,
        diff: &TableDiff,
        column_sql: Vec<String>,
    ) -> Result<(bool, Vec<String>)> {
        default::on_schema_alter_table_change_column(self.as_dyn(), column_diff, diff, column_sql)
    }

    /// # Protected
    fn on_schema_alter_table_rename_column(
        &self,
        old_column_name: &str,
        column: &Column,
        diff: &TableDiff,
        column_sql: Vec<String>,
    ) -> Result<(bool, Vec<String>)> {
        default::on_schema_alter_table_rename_column(
            self.as_dyn(),
            old_column_name,
            column,
            diff,
            column_sql,
        )
    }

    /// # Protected
    fn on_schema_alter_table(
        &self,
        diff: &TableDiff,
        sql: Vec<String>,
    ) -> Result<(bool, Vec<String>)> {
        default::on_schema_alter_table(self.as_dyn(), diff, sql)
    }

    fn get_portable_databases_list(&self, databases: Vec<Row>) -> Result<Vec<Identifier>> {
        let mut list = vec![];
        for value in &databases {
            list.push(self.get_portable_database_definition(value)?)
        }

        Ok(list)
    }

    fn get_portable_database_definition(&self, row: &Row) -> Result<Identifier> {
        let name = string_from_value(self.get_connection(), row.get(0))?;
        Ok(Identifier::new(name, false))
    }

    fn get_portable_sequences_list(&self, sequences: Vec<Row>) -> Result<Vec<Sequence>> {
        let mut list = vec![];
        for row in &sequences {
            list.push(self.get_portable_sequence_definition(row)?)
        }

        Ok(list)
    }

    fn get_portable_sequence_definition(&self, _: &Row) -> Result<Sequence> {
        Err(Error::platform_feature_unsupported("sequences"))
    }

    /// Independent of the database the keys of the column list result are lowercased.
    /// The name of the created column instance however is kept in its case.
    ///
    /// # Protected
    fn get_portable_table_column_list(
        &self,
        table: &str,
        database: &str,
        table_columns: Vec<Row>,
    ) -> Result<Vec<Column>> {
        let table = table.to_string();
        let database = database.to_string();

        let platform = self.get_platform()?;
        let event_manager = platform.get_event_manager();
        let mut list = vec![];

        for table_column in table_columns {
            let event = event_manager.dispatch_sync(SchemaColumnDefinitionEvent::new(
                &table_column,
                &table,
                &database,
                platform.clone(),
            ))?;

            let column = if event.is_default_prevented() {
                event.column()
            } else {
                Some(self.get_portable_table_column_definition(&table_column)?)
            };

            if column.is_none() {
                continue;
            }

            let column = column.unwrap();
            list.push(column);
        }

        Ok(list)
    }

    /// Gets Table Column Definition.
    fn get_portable_table_column_definition(&self, table_column: &Row) -> Result<Column>;

    /// Aggregates and groups the index results according to the required data result.
    fn get_portable_table_indexes_list(
        &self,
        table_indexes: Vec<Row>,
        table_name: &str,
    ) -> AsyncResult<Vec<Index>> {
        let table_name = table_name.to_string();
        Box::pin(async move {
            default::get_portable_table_indexes_list(self.as_dyn(), table_indexes, table_name)
        })
    }

    fn get_portable_tables_list(&self, tables: Vec<Row>) -> AsyncResult<Vec<Identifier>> {
        Box::pin(async move {
            let mut list = vec![];
            for table in &tables {
                list.push(self.get_portable_table_definition(table).await?);
            }

            Ok(list)
        })
    }

    fn get_portable_table_definition(&self, table: &Row) -> AsyncResult<Identifier> {
        let name = string_from_value(self.get_connection(), table.get(0));
        Box::pin(async move { Ok(Identifier::new(name?, false)) })
    }

    fn get_portable_views_list(&self, rows: Vec<Row>) -> Result<Vec<View>> {
        let platform = self.get_platform()?;
        let mut list = HashMap::new();
        for view in rows {
            if let Some(view) = self.get_portable_view_definition(&view)? {
                let view_name = view.get_quoted_name(platform.as_dyn());
                list.insert(view_name.to_lowercase(), view);
            }
        }

        Ok(list.into_values().collect())
    }

    fn get_portable_view_definition(&self, view: &Row) -> Result<Option<View>> {
        let name = string_from_value(self.get_connection(), view.get(0))?;
        let sql = string_from_value(self.get_connection(), view.get(1))?;

        Ok(Some(View::new(name, &sql)))
    }

    fn get_portable_table_foreign_keys_list(
        &self,
        table_foreign_keys: Vec<Row>,
    ) -> Result<Vec<ForeignKeyConstraint>> {
        default::get_portable_table_foreign_keys_list(self.as_dyn(), table_foreign_keys)
    }

    fn get_portable_table_foreign_key_definition(
        &self,
        foreign_key: &Row,
    ) -> Result<ForeignKeyConstraint> {
        let connection = self.get_connection();
        let local_columns = string_from_value(connection, foreign_key.get("local_columns"))?;
        let foreign_columns = string_from_value(connection, foreign_key.get("foreign_columns"))?;

        let options = HashMap::new();

        let mut constraint = ForeignKeyConstraint::new(
            &local_columns.split(',').collect::<Vec<_>>(),
            &foreign_columns.split(',').collect::<Vec<_>>(),
            string_from_value(connection, foreign_key.get("foreign_table"))?,
            options,
            None,
            None,
        );

        if let Ok(constraint_name) = foreign_key.get("constraint_name") {
            let constraint_name = constraint_name.to_string();
            if !constraint_name.is_empty() {
                constraint.set_name(&constraint_name);
            }
        }

        Ok(constraint)
    }

    /// Creates a schema instance for the current database.
    fn introspect_schema(&self) -> AsyncResult<Schema> {
        Box::pin(async move {
            let platform = self.get_platform()?;
            let schema_names = if platform.supports_schemas() {
                self.list_schema_names().await?
            } else {
                vec![]
            };

            let sequences = if platform.supports_sequences() {
                self.list_sequences().await?
            } else {
                vec![]
            };

            let views = self.list_views().await?;
            let tables = self.list_tables().await?;

            Ok(Schema::new(tables, views, sequences, schema_names))
        })
    }

    fn create_comparator(&self) -> Box<dyn Comparator + Send + '_>;
}

impl<T: SchemaManager + ?Sized> SchemaManager for &mut T {
    delegate::delegate! {
        to(**self) {
            fn get_connection(&self) -> &Connection;
            fn get_platform(&self) -> Result<Arc<Box<dyn DatabasePlatform + Send + Sync>>>;
            fn get_create_table_sql(&self, table: &Table, create_flags: Option<CreateFlags>) -> Result<Vec<String>>;
            fn get_create_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>>;
            fn _get_create_table_sql(&self, name: &Identifier, columns: &[ColumnData], options: &TableOptions) -> Result<Vec<String>>;
            fn get_create_temporary_table_snippet_sql(&self) -> Result<String>;
            fn get_create_sequence_sql(&self, sequence: &Sequence) -> Result<String>;
            fn get_create_index_sql(&self, index: &Index, table: &dyn IntoIdentifier) -> Result<String>;
            fn get_create_index_sql_flags(&self, index: &Index) -> String;
            fn get_create_primary_key_sql(&self, index: &Index, table: &dyn IntoIdentifier) -> Result<String>;
            fn get_create_schema_sql(&self, schema_name: &dyn IntoIdentifier) -> Result<String>;
            fn get_create_unique_constraint_sql(&self, constraint: &UniqueConstraint, table_name: &dyn IntoIdentifier) -> Result<String>;
            fn get_create_foreign_key_sql(&self, foreign_key: &ForeignKeyConstraint, table: &dyn IntoIdentifier) -> Result<String>;
            fn get_create_view_sql(&self, view: &View) -> Result<String>;
            fn get_create_database_sql(&self, name: &Identifier) -> Result<String>;
            fn get_column_charset_declaration_sql(&self, charset: &str) -> String;
            fn get_list_databases_sql(&self) -> Result<String>;
            fn get_list_tables_sql(&self) -> Result<String>;
            fn get_list_sequences_sql(&self, database: &str) -> Result<String>;
            fn get_list_table_columns_sql(&self, table: &str, database: &str) -> Result<String>;
            fn get_list_table_indexes_sql(&self, table: &str, database: &str) -> Result<String>;
            fn get_list_table_foreign_keys_sql(&self, table: &str, database: &str) -> Result<String>;
            fn get_list_table_constraints_sql(&self, table: &str) -> Result<String>;
            fn get_comment_on_table_sql(&self, table_name: &Identifier, comment: &str) -> Result<String>;
            fn get_comment_on_column_sql(&self, table_name: &dyn IntoIdentifier, column: &dyn IntoIdentifier, comment: &str) -> Result<String>;
            fn get_inline_column_comment_sql(&self, comment: &str) -> Result<String>;
            fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>;
            fn get_alter_sequence_sql(&self, sequence: &Sequence) -> Result<String>;
            fn get_truncate_table_sql(&self, table_name: &dyn IntoIdentifier, cascade: bool) -> Result<String>;
            fn get_drop_database_sql(&self, name: &str) -> Result<String>;
            fn get_drop_schema_sql(&self, schema_name: &str) -> Result<String>;
            fn get_drop_table_sql(&self, table_name: &dyn IntoIdentifier) -> Result<String>;
            fn get_drop_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>>;
            fn get_drop_temporary_table_sql(&self, table: &Identifier) -> Result<String>;
            fn get_drop_index_sql(&self, index: &Identifier, table: &Identifier) -> Result<String>;
            fn get_drop_unique_constraint_sql(&self, name: &Identifier, table_name: &Identifier) -> Result<String>;
            fn get_drop_constraint_sql(&self, constraint: &Identifier, table_name: &Identifier) -> Result<String>;
            fn get_drop_foreign_key_sql(&self, foreign_key: &dyn IntoIdentifier, table_name: &dyn IntoIdentifier) -> Result<String>;
            fn get_drop_sequence_sql(&self, sequence: &dyn IntoIdentifier) -> Result<String>;
            fn get_drop_view_sql(&self, name: &Identifier) -> Result<String>;
            fn list_databases(&self) -> AsyncResult<Vec<Identifier>>;
            fn list_schema_names(&self) -> AsyncResult<Vec<Identifier>>;
            fn list_sequences(&self) -> AsyncResult<Vec<Sequence>>;
            fn list_table_columns(&self, table: &str, database: Option<&str>) -> AsyncResult<Vec<Column>>;
            fn list_table_indexes(&self, table: &str) -> AsyncResult<Vec<Index>>;
            fn tables_exist(&self, names: &[&str]) -> AsyncResult<bool>;
            fn list_table_names(&self) -> AsyncResult<Vec<String>>;
            fn list_tables(&self) -> AsyncResult<Vec<Table>>;
            fn list_table_details(&self, name: &str) -> AsyncResult<Table>;
            fn normalize_name(&self, name: &str) -> String;
            fn select_table_names(&self, database_name: &str) -> AsyncResult<StatementResult>;
            fn select_table_columns(&self, database_name: &str, table_name: Option<&str>) -> AsyncResult<StatementResult>;
            fn select_index_columns(&self, database_name: &str, table_name: Option<&str>) -> AsyncResult<StatementResult>;
            fn select_foreign_key_columns(&self, database_name: &str, table_name: Option<&str>) -> AsyncResult<StatementResult>;
            fn quote_string_literal(&self, str: &str) -> String;
            fn fetch_table_columns_by_table(&self, database_name: &str) -> AsyncResult<HashMap<String, Vec<Row>>>;
            fn fetch_index_columns_by_table(&self, database_name: &str) -> AsyncResult<HashMap<String, Vec<Row>>>;
            fn fetch_foreign_key_columns_by_table(&self, database_name: &str) -> AsyncResult<HashMap<String, Vec<Row>>>;
            fn fetch_table_options_by_table(&self, database_name: &str, table_name: Option<&str>) -> AsyncResult<HashMap<String, Vec<Row>>>;
            fn get_list_views_sql(&self, database: &str) -> Result<String>;
            fn list_views(&self) -> AsyncResult<Vec<View>>;
            fn list_table_foreign_keys(&self, table: &str) -> AsyncResult<Vec<ForeignKeyConstraint>>;
            fn get_column_declaration_sql(&self, name: &str, column: &ColumnData) -> Result<String>;
            fn get_partial_index_sql(&self, index: &Index) -> Result<String>;
            fn get_column_comment(&self, column: &Column) -> Result<String>;
            fn drop_database(&self, database: &str) -> AsyncResult<()>;
            fn drop_schema(&self, schema_name: &str) -> AsyncResult<()>;
            fn drop_table(&self, name: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn drop_index(&self, index: &dyn IntoIdentifier, table: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn drop_foreign_key(&self, foreign_key: &dyn IntoIdentifier, table: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn drop_sequence(&self, name: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn drop_unique_constraint(&self, name: &dyn IntoIdentifier, table_name: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn drop_view(&self, name: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn create_database(&self, database: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn create_table(&self, table: &Table) -> AsyncResult<()>;
            fn create_sequence(&self, sequence: &Sequence) -> AsyncResult<()>;
            fn create_index(&self, index: &Index, table: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn create_foreign_key(&self, foreign_key: &ForeignKeyConstraint, table: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn create_unique_constraint(&self, unique_constraint: &UniqueConstraint, table: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn create_view(&self, view: &View) -> AsyncResult<()>;
            fn drop_schema_objects(&self, schema: &Schema) -> AsyncResult<()>;
            fn alter_schema(&self, schema_diff: SchemaDiff) -> AsyncResult<()>;
            fn migrate_schema(&self, to_schema: Schema) -> AsyncResult<()>;
            fn alter_table(&self, table_diff: TableDiff) -> AsyncResult<()>;
            fn rename_table(&self, name: &dyn IntoIdentifier, new_name: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn get_pre_alter_table_index_foreign_key_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>;
            fn get_post_alter_table_index_foreign_key_sql(&self, diff: &TableDiff) -> Result<Vec<String>>;
            fn get_check_declaration_sql(&self, definition: &[ColumnData]) -> Result<String>;
            fn get_check_field_declaration_sql(&self, definition: &ColumnData) -> Result<String>;
            fn get_index_field_declaration_list_sql(&self, index: &Index) -> Result<String>;
            fn get_sequence_next_val_sql(&self, sequence: &str) -> Result<String>;
            fn get_rename_index_sql(&self, old_index_name: &Identifier, index: &Index, table_name: &Identifier) -> Result<Vec<String>>;
            fn columns_equal(&self, column1: &Column, column2: &Column) -> Result<bool>;
            fn get_column_declaration_list_sql(&self, columns: &[ColumnData]) -> Result<String>;
            fn get_unique_constraint_declaration_sql(&self, name: &str, constraint: &UniqueConstraint) -> Result<String>;
            fn get_index_declaration_sql(&self, name: &str, index: &Index) -> Result<String>;
            fn get_column_collation_declaration_sql(&self, collation: &str) -> Result<String>;
            fn get_foreign_key_declaration_sql(&self, foreign_key: &ForeignKeyConstraint) -> Result<String>;
            fn get_advanced_foreign_key_options_sql(&self, foreign_key: &ForeignKeyConstraint) -> Result<String>;
            fn get_foreign_key_referential_action_sql(&self, action: &ForeignKeyReferentialAction) -> Result<String>;
            fn get_foreign_key_base_declaration_sql(&self, foreign_key: &ForeignKeyConstraint) -> Result<String>;
            fn get_columns_field_declaration_list_sql(&self, columns: &[String]) -> Result<String>;
            fn on_schema_alter_table_add_column(&self, column: &Column, diff: &TableDiff, column_sql: Vec<String>) -> Result<(bool, Vec<String>)>;
            fn on_schema_alter_table_remove_column(&self, column: &Column, diff: &TableDiff, column_sql: Vec<String>) -> Result<(bool, Vec<String>)>;
            fn on_schema_alter_table_change_column(&self, column_diff: &ColumnDiff, diff: &TableDiff, column_sql: Vec<String>) -> Result<(bool, Vec<String>)>;
            fn on_schema_alter_table_rename_column(&self, old_column_name: &str, column: &Column, diff: &TableDiff, column_sql: Vec<String>) -> Result<(bool, Vec<String>)>;
            fn on_schema_alter_table(&self, diff: &TableDiff, sql: Vec<String>) -> Result<(bool, Vec<String>)>;
            fn get_portable_databases_list(&self, databases: Vec<Row>) -> Result<Vec<Identifier>>;
            fn get_portable_database_definition(&self, row: &Row) -> Result<Identifier>;
            fn get_portable_sequences_list(&self, sequences: Vec<Row>) -> Result<Vec<Sequence>>;
            fn get_portable_sequence_definition(&self, row: &Row) -> Result<Sequence>;
            fn get_portable_table_column_list(&self, table: &str, database: &str, table_columns: Vec<Row>) -> Result<Vec<Column>>;
            fn get_portable_table_column_definition(&self, table_column: &Row) -> Result<Column>;
            fn get_portable_table_indexes_list(&self, table_indexes: Vec<Row>, table_name: &str) -> AsyncResult<Vec<Index>>;
            fn get_portable_tables_list(&self, tables: Vec<Row>) -> AsyncResult<Vec<Identifier>>;
            fn get_portable_table_definition(&self, table: &Row) -> AsyncResult<Identifier>;
            fn get_portable_views_list(&self, rows: Vec<Row>) -> Result<Vec<View>>;
            fn get_portable_view_definition(&self, view: &Row) -> Result<Option<View>>;
            fn get_portable_table_foreign_keys_list(&self, table_foreign_keys: Vec<Row>) -> Result<Vec<ForeignKeyConstraint>>;
            fn get_portable_table_foreign_key_definition(&self, foreign_key: &Row) -> Result<ForeignKeyConstraint>;
            fn introspect_schema(&self) -> AsyncResult<Schema>;
            fn create_comparator(&self) -> Box<dyn Comparator + Send + '_>;
        }
    }

    fn as_dyn(&self) -> &dyn SchemaManager {
        self
    }
}

impl<T: SchemaManager + ?Sized> SchemaManager for Box<T> {
    delegate::delegate! {
        to(**self) {
            fn get_connection(&self) -> &Connection;
            fn get_platform(&self) -> Result<Arc<Box<dyn DatabasePlatform + Send + Sync>>>;
            fn get_create_table_sql(&self, table: &Table, create_flags: Option<CreateFlags>) -> Result<Vec<String>>;
            fn get_create_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>>;
            fn _get_create_table_sql(&self, name: &Identifier, columns: &[ColumnData], options: &TableOptions) -> Result<Vec<String>>;
            fn get_create_temporary_table_snippet_sql(&self) -> Result<String>;
            fn get_create_sequence_sql(&self, sequence: &Sequence) -> Result<String>;
            fn get_create_index_sql(&self, index: &Index, table: &dyn IntoIdentifier) -> Result<String>;
            fn get_create_index_sql_flags(&self, index: &Index) -> String;
            fn get_create_primary_key_sql(&self, index: &Index, table: &dyn IntoIdentifier) -> Result<String>;
            fn get_create_schema_sql(&self, schema_name: &dyn IntoIdentifier) -> Result<String>;
            fn get_create_unique_constraint_sql(&self, constraint: &UniqueConstraint, table_name: &dyn IntoIdentifier) -> Result<String>;
            fn get_create_foreign_key_sql(&self, foreign_key: &ForeignKeyConstraint, table: &dyn IntoIdentifier) -> Result<String>;
            fn get_create_view_sql(&self, view: &View) -> Result<String>;
            fn get_create_database_sql(&self, name: &Identifier) -> Result<String>;
            fn get_column_charset_declaration_sql(&self, charset: &str) -> String;
            fn get_list_databases_sql(&self) -> Result<String>;
            fn get_list_tables_sql(&self) -> Result<String>;
            fn get_list_sequences_sql(&self, database: &str) -> Result<String>;
            fn get_list_table_columns_sql(&self, table: &str, database: &str) -> Result<String>;
            fn get_list_table_indexes_sql(&self, table: &str, database: &str) -> Result<String>;
            fn get_list_table_foreign_keys_sql(&self, table: &str, database: &str) -> Result<String>;
            fn get_list_table_constraints_sql(&self, table: &str) -> Result<String>;
            fn get_comment_on_table_sql(&self, table_name: &Identifier, comment: &str) -> Result<String>;
            fn get_comment_on_column_sql(&self, table_name: &dyn IntoIdentifier, column: &dyn IntoIdentifier, comment: &str) -> Result<String>;
            fn get_inline_column_comment_sql(&self, comment: &str) -> Result<String>;
            fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>;
            fn get_alter_sequence_sql(&self, sequence: &Sequence) -> Result<String>;
            fn get_truncate_table_sql(&self, table_name: &dyn IntoIdentifier, cascade: bool) -> Result<String>;
            fn get_drop_database_sql(&self, name: &str) -> Result<String>;
            fn get_drop_schema_sql(&self, schema_name: &str) -> Result<String>;
            fn get_drop_table_sql(&self, table_name: &dyn IntoIdentifier) -> Result<String>;
            fn get_drop_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>>;
            fn get_drop_temporary_table_sql(&self, table: &Identifier) -> Result<String>;
            fn get_drop_index_sql(&self, index: &Identifier, table: &Identifier) -> Result<String>;
            fn get_drop_unique_constraint_sql(&self, name: &Identifier, table_name: &Identifier) -> Result<String>;
            fn get_drop_constraint_sql(&self, constraint: &Identifier, table_name: &Identifier) -> Result<String>;
            fn get_drop_foreign_key_sql(&self, foreign_key: &dyn IntoIdentifier, table_name: &dyn IntoIdentifier) -> Result<String>;
            fn get_drop_sequence_sql(&self, sequence: &dyn IntoIdentifier) -> Result<String>;
            fn get_drop_view_sql(&self, name: &Identifier) -> Result<String>;
            fn list_databases(&self) -> AsyncResult<Vec<Identifier>>;
            fn list_schema_names(&self) -> AsyncResult<Vec<Identifier>>;
            fn list_sequences(&self) -> AsyncResult<Vec<Sequence>>;
            fn list_table_columns(&self, table: &str, database: Option<&str>) -> AsyncResult<Vec<Column>>;
            fn list_table_indexes(&self, table: &str) -> AsyncResult<Vec<Index>>;
            fn tables_exist(&self, names: &[&str]) -> AsyncResult<bool>;
            fn list_table_names(&self) -> AsyncResult<Vec<String>>;
            fn list_tables(&self) -> AsyncResult<Vec<Table>>;
            fn list_table_details(&self, name: &str) -> AsyncResult<Table>;
            fn normalize_name(&self, name: &str) -> String;
            fn select_table_names(&self, database_name: &str) -> AsyncResult<StatementResult>;
            fn select_table_columns(&self, database_name: &str, table_name: Option<&str>) -> AsyncResult<StatementResult>;
            fn select_index_columns(&self, database_name: &str, table_name: Option<&str>) -> AsyncResult<StatementResult>;
            fn select_foreign_key_columns(&self, database_name: &str, table_name: Option<&str>) -> AsyncResult<StatementResult>;
            fn quote_string_literal(&self, str: &str) -> String;
            fn fetch_table_columns_by_table(&self, database_name: &str) -> AsyncResult<HashMap<String, Vec<Row>>>;
            fn fetch_index_columns_by_table(&self, database_name: &str) -> AsyncResult<HashMap<String, Vec<Row>>>;
            fn fetch_foreign_key_columns_by_table(&self, database_name: &str) -> AsyncResult<HashMap<String, Vec<Row>>>;
            fn fetch_table_options_by_table(&self, database_name: &str, table_name: Option<&str>) -> AsyncResult<HashMap<String, Vec<Row>>>;
            fn get_list_views_sql(&self, database: &str) -> Result<String>;
            fn list_views(&self) -> AsyncResult<Vec<View>>;
            fn list_table_foreign_keys(&self, table: &str) -> AsyncResult<Vec<ForeignKeyConstraint>>;
            fn get_column_declaration_sql(&self, name: &str, column: &ColumnData) -> Result<String>;
            fn get_partial_index_sql(&self, index: &Index) -> Result<String>;
            fn get_column_comment(&self, column: &Column) -> Result<String>;
            fn drop_database(&self, database: &str) -> AsyncResult<()>;
            fn drop_schema(&self, schema_name: &str) -> AsyncResult<()>;
            fn drop_table(&self, name: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn drop_index(&self, index: &dyn IntoIdentifier, table: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn drop_foreign_key(&self, foreign_key: &dyn IntoIdentifier, table: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn drop_sequence(&self, name: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn drop_unique_constraint(&self, name: &dyn IntoIdentifier, table_name: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn drop_view(&self, name: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn create_database(&self, database: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn create_table(&self, table: &Table) -> AsyncResult<()>;
            fn create_sequence(&self, sequence: &Sequence) -> AsyncResult<()>;
            fn create_index(&self, index: &Index, table: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn create_foreign_key(&self, foreign_key: &ForeignKeyConstraint, table: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn create_unique_constraint(&self, unique_constraint: &UniqueConstraint, table: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn create_view(&self, view: &View) -> AsyncResult<()>;
            fn drop_schema_objects(&self, schema: &Schema) -> AsyncResult<()>;
            fn alter_schema(&self, schema_diff: SchemaDiff) -> AsyncResult<()>;
            fn migrate_schema(&self, to_schema: Schema) -> AsyncResult<()>;
            fn alter_table(&self, table_diff: TableDiff) -> AsyncResult<()>;
            fn rename_table(&self, name: &dyn IntoIdentifier, new_name: &dyn IntoIdentifier) -> AsyncResult<()>;
            fn get_pre_alter_table_index_foreign_key_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>;
            fn get_post_alter_table_index_foreign_key_sql(&self, diff: &TableDiff) -> Result<Vec<String>>;
            fn get_check_declaration_sql(&self, definition: &[ColumnData]) -> Result<String>;
            fn get_check_field_declaration_sql(&self, definition: &ColumnData) -> Result<String>;
            fn get_index_field_declaration_list_sql(&self, index: &Index) -> Result<String>;
            fn get_sequence_next_val_sql(&self, sequence: &str) -> Result<String>;
            fn get_rename_index_sql(&self, old_index_name: &Identifier, index: &Index, table_name: &Identifier) -> Result<Vec<String>>;
            fn columns_equal(&self, column1: &Column, column2: &Column) -> Result<bool>;
            fn get_column_declaration_list_sql(&self, columns: &[ColumnData]) -> Result<String>;
            fn get_unique_constraint_declaration_sql(&self, name: &str, constraint: &UniqueConstraint) -> Result<String>;
            fn get_index_declaration_sql(&self, name: &str, index: &Index) -> Result<String>;
            fn get_column_collation_declaration_sql(&self, collation: &str) -> Result<String>;
            fn get_foreign_key_declaration_sql(&self, foreign_key: &ForeignKeyConstraint) -> Result<String>;
            fn get_advanced_foreign_key_options_sql(&self, foreign_key: &ForeignKeyConstraint) -> Result<String>;
            fn get_foreign_key_referential_action_sql(&self, action: &ForeignKeyReferentialAction) -> Result<String>;
            fn get_foreign_key_base_declaration_sql(&self, foreign_key: &ForeignKeyConstraint) -> Result<String>;
            fn get_columns_field_declaration_list_sql(&self, columns: &[String]) -> Result<String>;
            fn on_schema_alter_table_add_column(&self, column: &Column, diff: &TableDiff, column_sql: Vec<String>) -> Result<(bool, Vec<String>)>;
            fn on_schema_alter_table_remove_column(&self, column: &Column, diff: &TableDiff, column_sql: Vec<String>) -> Result<(bool, Vec<String>)>;
            fn on_schema_alter_table_change_column(&self, column_diff: &ColumnDiff, diff: &TableDiff, column_sql: Vec<String>) -> Result<(bool, Vec<String>)>;
            fn on_schema_alter_table_rename_column(&self, old_column_name: &str, column: &Column, diff: &TableDiff, column_sql: Vec<String>) -> Result<(bool, Vec<String>)>;
            fn on_schema_alter_table(&self, diff: &TableDiff, sql: Vec<String>) -> Result<(bool, Vec<String>)>;
            fn get_portable_databases_list(&self, databases: Vec<Row>) -> Result<Vec<Identifier>>;
            fn get_portable_database_definition(&self, row: &Row) -> Result<Identifier>;
            fn get_portable_sequences_list(&self, sequences: Vec<Row>) -> Result<Vec<Sequence>>;
            fn get_portable_sequence_definition(&self, row: &Row) -> Result<Sequence>;
            fn get_portable_table_column_list(&self, table: &str, database: &str, table_columns: Vec<Row>) -> Result<Vec<Column>>;
            fn get_portable_table_column_definition(&self, table_column: &Row) -> Result<Column>;
            fn get_portable_table_indexes_list(&self, table_indexes: Vec<Row>, table_name: &str) -> AsyncResult<Vec<Index>>;
            fn get_portable_tables_list(&self, tables: Vec<Row>) -> AsyncResult<Vec<Identifier>>;
            fn get_portable_table_definition(&self, table: &Row) -> AsyncResult<Identifier>;
            fn get_portable_views_list(&self, rows: Vec<Row>) -> Result<Vec<View>>;
            fn get_portable_view_definition(&self, view: &Row) -> Result<Option<View>>;
            fn get_portable_table_foreign_keys_list(&self, table_foreign_keys: Vec<Row>) -> Result<Vec<ForeignKeyConstraint>>;
            fn get_portable_table_foreign_key_definition(&self, foreign_key: &Row) -> Result<ForeignKeyConstraint>;
            fn introspect_schema(&self) -> AsyncResult<Schema>;
            fn create_comparator(&self) -> Box<dyn Comparator + Send + '_>;
        }
    }

    fn as_dyn(&self) -> &dyn SchemaManager {
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::r#type::{
        TypeManager, BOOLEAN, DATE, DATETIME, DECIMAL, INTEGER, STRING, TEXT, TIME,
    };
    use crate::schema::schema_manager::_exec_sql;
    use crate::schema::{
        Asset, Column, ColumnDiff, Comparator, ForeignKeyConstraint, ForeignKeyReferentialAction,
        Index, IntoIdentifier, SchemaDiff, SchemaManager, Sequence, Table, TableDiff,
        UniqueConstraint, View,
    };
    use crate::tests::{
        create_connection, get_database_dsn, FunctionalTestsHelper, MockConnection,
    };
    use crate::SchemaAlterTableRenameColumnEvent;
    use crate::{
        params, Configuration, Connection, ConnectionOptions, Error, EventDispatcher, Result,
        SchemaAlterTableAddColumnEvent, SchemaAlterTableChangeColumnEvent, SchemaAlterTableEvent,
        SchemaAlterTableRemoveColumnEvent, SchemaColumnDefinitionEvent,
        SchemaCreateTableColumnEvent, SchemaCreateTableEvent, SchemaDropTableEvent,
        SchemaIndexDefinitionEvent, Value,
    };
    use itertools::Itertools;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    #[serial]
    async fn can_overwrite_drop_table_sql_via_event_listener() {
        let ev = EventDispatcher::new();
        ev.add_listener(|e: &mut SchemaDropTableEvent| {
            e.prevent_default();
            e.sql = Some(format!("-- DROP SCHEMA {}", e.get_table()));

            Ok(())
        });

        let driver = MockConnection {};
        let connection = Connection::create_with_connection(Box::new(driver), None, Some(ev))
            .await
            .expect("failed to create connection");
        let platform = connection
            .get_platform()
            .expect("failed to create platform");
        let schema_manager = platform.create_schema_manager(&connection);
        let d = schema_manager.get_drop_table_sql(&"table").unwrap();

        assert_eq!("-- DROP SCHEMA \"table\"", d);
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn returns_foreign_key_referential_action_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let tests = [
            (ForeignKeyReferentialAction::Cascade, "CASCADE"),
            (ForeignKeyReferentialAction::SetNull, "SET NULL"),
            (ForeignKeyReferentialAction::NoAction, "NO ACTION"),
            (ForeignKeyReferentialAction::Restrict, "RESTRICT"),
            (ForeignKeyReferentialAction::SetDefault, "SET DEFAULT"),
        ];

        for (action, expected) in tests {
            assert_eq!(
                schema_manager
                    .get_foreign_key_referential_action_sql(&action)
                    .unwrap(),
                expected
            )
        }
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn create_with_no_columns() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let table = Table::new("test".into_identifier());
        let result = schema_manager.get_create_table_sql(&table, None);

        assert!(result.is_err());
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn generates_partial_indexes_sql_only_when_supporting_partial_indexes() {
        let r#where = "test IS NULL AND test2 IS NOT NULL";
        let mut index_def = Index::new(
            "name",
            &["test", "test2"],
            false,
            false,
            &[],
            HashMap::default(),
        );
        index_def.r#where = Some(r#where.to_string());
        let unique_constraint =
            UniqueConstraint::new("name", &["test", "test2"], &[], HashMap::default());

        let expected = format!(" WHERE {}", r#where);
        let mut indexes = vec![];

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        indexes.push(
            schema_manager
                .get_index_declaration_sql("name", &index_def)
                .unwrap(),
        );
        let unique_constraint_sql = schema_manager
            .get_unique_constraint_declaration_sql("name", &unique_constraint)
            .unwrap();

        assert!(
            !unique_constraint_sql.ends_with(&expected),
            "WHERE clause should NOT be present"
        );

        indexes.push(
            schema_manager
                .get_create_index_sql(&index_def, &"table")
                .unwrap(),
        );
        for index in indexes {
            if schema_manager.get_platform().unwrap().get_name() == "postgresql" {
                assert!(index.ends_with(&expected), "WHERE clause should be present");
            } else {
                assert!(
                    !index.ends_with(&expected),
                    "WHERE clause should NOT be present"
                );
            }
        }
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn get_custom_column_declaration_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let mut column = Column::new("foo", INTEGER).unwrap();
        column.set_column_definition("MEDIUMINT(6) UNSIGNED".to_string().into());

        let column_data = column.generate_column_data(&schema_manager.get_platform().unwrap());
        let sql = schema_manager
            .get_column_declaration_sql("foo", &column_data)
            .unwrap();

        assert_eq!(sql, "foo MEDIUMINT(6) UNSIGNED");
    }

    #[derive(Default)]
    struct SqlDispatchEventListener {
        on_schema_create_table_calls: usize,
        on_schema_create_table_column_calls: usize,
        on_schema_drop_table: usize,
        on_schema_alter_table: usize,
        on_schema_alter_table_add_column: usize,
        on_schema_alter_table_remove_column: usize,
        on_schema_alter_table_change_column: usize,
        on_schema_alter_table_rename_column: usize,

        on_schema_column_definition: usize,
        on_schema_index_definition: usize,
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn get_create_table_sql_dispatch_event() {
        let listener = Arc::new(Mutex::new(SqlDispatchEventListener::default()));

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let event_manager = connection.get_event_manager();

        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaCreateTableEvent| {
                listener.lock().unwrap().on_schema_create_table_calls += 1;
                Ok(())
            });
        }

        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaCreateTableColumnEvent| {
                listener.lock().unwrap().on_schema_create_table_column_calls += 1;
                Ok(())
            });
        }

        let mut table = Table::new("test");

        let mut foo_column = Column::new("foo", STRING).unwrap();
        foo_column.set_notnull(false);
        foo_column.set_length(255);
        table.add_column(foo_column);
        let mut bar_column = Column::new("bar", STRING).unwrap();
        bar_column.set_notnull(false);
        bar_column.set_length(255);
        table.add_column(bar_column);

        schema_manager.get_create_table_sql(&table, None).unwrap();

        assert_eq!(listener.lock().unwrap().on_schema_create_table_calls, 1);
        assert_eq!(
            listener.lock().unwrap().on_schema_create_table_column_calls,
            2
        );
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn get_drop_table_sql_dispatch_event() {
        let listener = Arc::new(Mutex::new(SqlDispatchEventListener::default()));

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let event_manager = connection.get_event_manager();

        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaDropTableEvent| {
                listener.lock().unwrap().on_schema_drop_table += 1;
                Ok(())
            });
        }

        schema_manager.get_drop_table_sql(&"TABLE").unwrap();

        assert_eq!(listener.lock().unwrap().on_schema_drop_table, 1);
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn get_alter_table_sql_dispatch_event() {
        let listener = Arc::new(Mutex::new(SqlDispatchEventListener::default()));

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let event_manager = connection.get_event_manager();

        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaAlterTableEvent| {
                listener.lock().unwrap().on_schema_alter_table += 1;
                Ok(())
            });
        }
        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaAlterTableAddColumnEvent| {
                listener.lock().unwrap().on_schema_alter_table_add_column += 1;
                Ok(())
            });
        }
        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaAlterTableRemoveColumnEvent| {
                listener.lock().unwrap().on_schema_alter_table_remove_column += 1;
                Ok(())
            });
        }
        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaAlterTableChangeColumnEvent| {
                listener.lock().unwrap().on_schema_alter_table_change_column += 1;
                Ok(())
            });
        }
        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaAlterTableRenameColumnEvent| {
                listener.lock().unwrap().on_schema_alter_table_rename_column += 1;
                Ok(())
            });
        }

        let mut table = Table::new("mytable");
        table.add_column(Column::new("removed", INTEGER).unwrap());
        table.add_column(Column::new("changed", INTEGER).unwrap());
        table.add_column(Column::new("renamed", INTEGER).unwrap());

        let mut table_diff = TableDiff::new("mytable", Some(&table));
        table_diff
            .added_columns
            .push(Column::new("added", INTEGER).unwrap());
        table_diff
            .removed_columns
            .push(Column::new("removed", INTEGER).unwrap());
        table_diff.changed_columns.push(ColumnDiff::new(
            "changed",
            &Column::new("changed2", STRING).unwrap(),
            &[],
            None,
        ));
        table_diff.renamed_columns.push((
            "renamed".to_string(),
            Column::new("renamed2", INTEGER).unwrap(),
        ));

        schema_manager.get_alter_table_sql(&mut table_diff).unwrap();

        {
            let listener = listener.lock().unwrap();
            assert_eq!(
                listener.on_schema_alter_table, 1,
                "alter table calls count differs"
            );
            assert_eq!(
                listener.on_schema_alter_table_add_column, 1,
                "alter table add column calls count differs"
            );
            assert_eq!(
                listener.on_schema_alter_table_remove_column, 1,
                "alter table remove column calls count differs"
            );
            assert_eq!(
                listener.on_schema_alter_table_change_column, 1,
                "alter table change column calls count differs"
            );
            assert_eq!(
                listener.on_schema_alter_table_rename_column, 1,
                "alter table rename column calls count differs"
            );
        }
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn default_value_comparison() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();
        let platform = schema_manager.get_platform()?;

        let tests = [
            (INTEGER, Value::from(1)),
            (BOOLEAN, Value::from(true)),
            (STRING, Value::from("Creed")),
        ];

        for (ty, value) in tests {
            let mut col = Column::new("test", ty).unwrap();
            if ty == STRING && platform.get_name() == "mysql" {
                let rows = helper.connection
                    .fetch_all("SELECT @@character_set_database AS charset, @@collation_database AS collation", params!())
                    .await?;
                let row = rows.get(0).unwrap();

                col.set_charset(row.get("charset")?.to_string());
                col.set_collation(row.get("collation")?.to_string());
            }

            col.set_default(value);

            let mut table = Table::new("default_value");
            table.add_column(col);

            helper.drop_and_create_table(&table).await?;

            let online_table = schema_manager.introspect_table("default_value").await?;
            let comparator = schema_manager.create_comparator();
            let diff = comparator.diff_table(&table, &online_table)?;
            assert!(diff.is_none());
        }

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn create_sequence() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();
        let platform = schema_manager.get_platform()?;

        if !platform.supports_sequences() {
            return Ok(());
        }

        let name = "create_sequences_test_seq";
        let _ = schema_manager.drop_sequence(&name).await;
        schema_manager
            .create_sequence(&Sequence::new(name, None, None, None))
            .await?;
        assert!(helper.has_element_with_name(&schema_manager.list_sequences().await?, name));

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_sequences() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();
        let platform = schema_manager.get_platform()?;

        if platform.supports_sequences() {
            let name = "list_sequences_test_seq";
            let _ = schema_manager.drop_sequence(&name).await;
            schema_manager
                .create_sequence(&Sequence::new(&name, 20, 10, None))
                .await?;
            let sequences = schema_manager.list_sequences().await?;

            assert!(sequences.into_iter().any(|s| {
                if s.get_name().to_lowercase() == name {
                    assert_eq!(s.get_allocation_size(), 20);
                    assert_eq!(s.get_initial_value(), 10);

                    true
                } else {
                    false
                }
            }));
        }

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_databases() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();
        let platform = schema_manager.get_platform()?;

        if platform.supports_create_drop_database() {
            let name = "test_create_database";
            let _ = schema_manager.drop_database(name).await;
            schema_manager.create_database(&name).await?;

            let databases = schema_manager.list_databases().await?;
            assert!(databases
                .into_iter()
                .any(|d| d.get_name().to_lowercase() == name));
        }

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_schema_names() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();
        let platform = schema_manager.get_platform()?;
        let connection = schema_manager.get_connection();

        if platform.supports_schemas() {
            let name = "test_create_schema";
            let _ = schema_manager.drop_schema(name).await;

            let schemas = schema_manager.list_schema_names().await?;
            assert!(!schemas
                .into_iter()
                .any(|d| d.get_name().to_lowercase() == name));

            connection
                .prepare(schema_manager.get_create_schema_sql(&name)?)?
                .execute(params!())
                .await?;

            let schemas = schema_manager.list_schema_names().await?;
            assert!(schemas
                .into_iter()
                .any(|d| d.get_name().to_lowercase() == name));
        }

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_tables() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();

        let name = "list_tables_test";
        helper.drop_table_if_exists(&name).await;
        helper.create_test_table(name).await?;

        let tables = schema_manager.list_tables().await?;
        let table = tables.into_iter().find(|t| t.get_name() == name);

        assert!(table.is_some());

        let table = table.unwrap();
        assert!(table.has_column(&"id"));
        assert!(table.has_column(&"test"));
        assert!(table.has_column(&"foreign_key_test"));

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_tables_does_not_include_views() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();

        let name = "test_table_for_view";
        let view_name = "test_view";

        let _ = schema_manager.drop_view(&view_name).await;
        helper.drop_table_if_exists(&name).await;

        helper.create_test_table(name).await?;

        let sql = "SELECT * FROM test_table_for_view";

        let view = View::new(view_name, sql);
        schema_manager.create_view(&view).await?;

        let tables = schema_manager.list_tables().await?;
        let view = tables.into_iter().find(|t| t.get_name() == "test_view");
        assert!(view.is_none());

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_tables_with_filter() -> Result<()> {
        let tests = [("filter_test_1", 1), ("filter_test_", 2)];

        for (prefix, expected_count) in tests {
            let configuration =
                Configuration::default().set_schema_assets_filter(Box::new(move |name| {
                    name.to_lowercase().starts_with(prefix)
                }));

            let helper = FunctionalTestsHelper::with_configuration(configuration).await;
            let schema_manager = helper.get_schema_manager();

            helper.drop_table_if_exists(&"filter_test_1").await;
            helper.drop_table_if_exists(&"filter_test_2").await;

            helper.create_test_table("filter_test_1").await?;
            helper.create_test_table("filter_test_2").await?;

            assert_eq!(
                schema_manager.list_table_names().await?.len(),
                expected_count
            );
            assert_eq!(schema_manager.list_tables().await?.len(), expected_count);
        }

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn rename_table() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();

        helper.drop_table_if_exists(&"old_name").await;
        helper.drop_table_if_exists(&"new_name").await;
        helper.create_test_table("old_name").await?;
        schema_manager
            .rename_table(&"old_name", &"new_name")
            .await?;

        assert!(!schema_manager.tables_exist(&["old_name"]).await?);
        assert!(schema_manager.tables_exist(&["new_name"]).await?);

        Ok(())
    }

    fn create_list_table_columns() -> Result<Table> {
        let mut table = Table::new("list_table_columns");
        table.add_column(
            Column::builder("id", INTEGER)?
                .set_notnull(true)
                .get_column(),
        );
        table.add_column(
            Column::builder("test", STRING)?
                .set_length(255)
                .set_notnull(false)
                .set_default("expected default")
                .get_column(),
        );
        table.add_column(Column::builder("foo", TEXT)?.set_notnull(true).get_column());
        table.add_column(
            Column::builder("bar", DECIMAL)?
                .set_precision(10)
                .set_scale(4)
                .set_notnull(false)
                .get_column(),
        );
        table.add_column(Column::new("baz1", DATETIME)?);
        table.add_column(Column::new("baz2", TIME)?);
        table.add_column(Column::new("baz3", DATE)?);
        table.set_primary_key(&["id"], None)?;

        Ok(table)
    }

    fn get_test_composite_table(name: &str) -> Table {
        let mut table = Table::new(name);
        table.add_column(Column::builder("id", INTEGER).unwrap().set_notnull(true));
        table.add_column(
            Column::builder("other_id", INTEGER)
                .unwrap()
                .set_notnull(true),
        );
        table.add_column(Column::builder("test", STRING).unwrap().set_length(255));

        table.set_primary_key(&["id", "other_id"], None).unwrap();

        table
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_table_columns() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();
        let table = create_list_table_columns()?;

        helper.drop_and_create_table(&table).await?;

        let columns = schema_manager
            .list_table_columns("list_table_columns", None)
            .await?;
        let column_keys = columns
            .iter()
            .map(|c| c.get_name().into_owned())
            .collect::<Vec<_>>();

        let id_column = columns
            .iter()
            .find(|c| c.get_name().to_string().to_lowercase() == "id")
            .ok_or::<Error>("cannot find column 'id'".into())?;
        assert_eq!(
            column_keys.iter().find_position(|c| c == &"id").unwrap().0,
            0
        );
        assert_eq!(
            id_column.get_type(),
            TypeManager::get_instance().get_type_by_name(INTEGER)?
        );
        assert!(!id_column.is_unsigned().unwrap_or(false));
        assert!(id_column.is_notnull());

        let test_column = columns
            .iter()
            .find(|c| c.get_name().to_string().to_lowercase() == "test")
            .ok_or::<Error>("cannot find column 'test'".into())?;
        assert_eq!(
            column_keys
                .iter()
                .find_position(|c| c == &"test")
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            test_column.get_type(),
            TypeManager::get_instance().get_type_by_name(STRING)?
        );
        assert_eq!(test_column.get_length(), Some(255));
        assert!(!test_column.is_fixed());
        assert!(!test_column.is_notnull());
        assert_eq!(test_column.get_default(), &Value::from("expected default"));

        let foo_column = columns
            .iter()
            .find(|c| c.get_name() == "foo")
            .ok_or::<Error>("cannot find column 'foo'".into())?;
        assert_eq!(
            column_keys.iter().find_position(|c| c == &"foo").unwrap().0,
            2
        );
        assert_eq!(
            foo_column.get_type(),
            TypeManager::get_instance().get_type_by_name(TEXT)?
        );
        assert!(!foo_column.is_unsigned().unwrap_or(false));
        assert!(!foo_column.is_fixed());
        assert!(foo_column.is_notnull());
        assert_eq!(foo_column.get_default(), &Value::NULL);

        let bar_column = columns
            .iter()
            .find(|c| c.get_name() == "bar")
            .ok_or::<Error>("cannot find column 'bar'".into())?;
        assert_eq!(
            column_keys.iter().find_position(|c| c == &"bar").unwrap().0,
            3
        );
        assert_eq!(
            bar_column.get_type(),
            TypeManager::get_instance().get_type_by_name(DECIMAL)?
        );
        assert_eq!(bar_column.get_precision(), Some(10));
        assert_eq!(bar_column.get_scale(), Some(4));
        assert!(!bar_column.is_unsigned().unwrap_or(false));
        assert!(!bar_column.is_fixed());
        assert!(!bar_column.is_notnull());
        assert_eq!(bar_column.get_default(), &Value::NULL);

        let baz1_column = columns
            .iter()
            .find(|c| c.get_name() == "baz1")
            .ok_or::<Error>("cannot find column 'baz1'".into())?;
        assert_eq!(
            column_keys
                .iter()
                .find_position(|c| c == &"baz1")
                .unwrap()
                .0,
            4
        );
        assert_eq!(
            baz1_column.get_type(),
            TypeManager::get_instance().get_type_by_name(DATETIME)?
        );
        assert!(baz1_column.is_notnull());
        assert_eq!(baz1_column.get_default(), &Value::NULL);

        let baz2_column = columns
            .iter()
            .find(|c| c.get_name() == "baz2")
            .ok_or::<Error>("cannot find column 'baz2'".into())?;
        assert_eq!(
            column_keys
                .iter()
                .find_position(|c| c == &"baz2")
                .unwrap()
                .0,
            5
        );
        assert!([TIME, DATE, DATETIME].contains(&baz2_column.get_type().get_name()));
        assert!(baz2_column.is_notnull());
        assert_eq!(baz2_column.get_default(), &Value::NULL);

        let baz3_column = columns
            .iter()
            .find(|c| c.get_name() == "baz3")
            .ok_or::<Error>("cannot find column 'baz3'".into())?;
        assert_eq!(
            column_keys
                .iter()
                .find_position(|c| c == &"baz3")
                .unwrap()
                .0,
            6
        );
        assert!([TIME, DATE, DATETIME].contains(&baz3_column.get_type().get_name()));
        assert!(baz3_column.is_notnull());
        assert_eq!(baz3_column.get_default(), &Value::NULL);

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_table_columns_with_fixed_string_column() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();

        let table_name = "test_list_table_fixed_string";
        helper.drop_table_if_exists(&table_name).await;

        let mut table = Table::new(table_name);
        table.add_column(
            Column::builder("column_char", STRING)?
                .set_fixed(true)
                .set_length(2),
        );

        schema_manager.create_table(&table).await?;
        let columns = schema_manager.list_table_columns(table_name, None).await?;

        let col = columns.get(0).unwrap();
        assert_eq!(col.get_name(), "column_char");
        assert_eq!(col.get_type().get_name(), STRING);
        assert!(col.is_fixed());
        assert_eq!(col.get_length(), Some(2));

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_table_columns_dispatch_event() -> Result<()> {
        let listener = Arc::new(Mutex::new(SqlDispatchEventListener::default()));

        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();
        let event_manager = helper.connection.get_event_manager();

        let table = create_list_table_columns()?;
        helper.drop_and_create_table(&table).await?;

        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaColumnDefinitionEvent| {
                listener.lock().unwrap().on_schema_column_definition += 1;
                Ok(())
            });
        }

        schema_manager
            .list_table_columns("list_table_columns", None)
            .await?;
        assert_eq!(listener.lock().unwrap().on_schema_column_definition, 7);

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_table_indexes_dispatch_event() -> Result<()> {
        let listener = Arc::new(Mutex::new(SqlDispatchEventListener::default()));

        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();
        let event_manager = helper.connection.get_event_manager();

        let mut table = helper.get_test_table("list_table_indexes_test")?;
        table.add_unique_index(&["test"], Some("test_index_name"), Default::default())?;
        table.add_index(Index::new(
            "test_composite_idx",
            &["id", "test"],
            false,
            false,
            &[],
            Default::default(),
        ));

        helper.drop_and_create_table(&table).await?;

        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaIndexDefinitionEvent| {
                listener.lock().unwrap().on_schema_index_definition += 1;
                Ok(())
            });
        }

        schema_manager
            .list_table_indexes("list_table_indexes_test")
            .await?;
        assert_eq!(listener.lock().unwrap().on_schema_index_definition, 3); // 3 = primary, test_index_name, test_composite_idx

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn dispatch_event_when_database_platform_is_explicitly_passed() -> Result<()> {
        let platform = {
            let helper = FunctionalTestsHelper::default().await;
            helper.connection.get_platform()?.clone()
        };

        let conn_opts = ConnectionOptions::try_from(get_database_dsn().as_str())?
            .with_platform(Some(Arc::into_inner(platform).unwrap()));
        let connection = Connection::create(conn_opts, None, None);

        let helper = FunctionalTestsHelper::new(connection.connect().await?);
        let schema_manager = helper.get_schema_manager();
        let event_manager = helper.connection.get_event_manager();
        helper
            .drop_table_if_exists(&"explicit_db_platform_test")
            .await;
        let table = helper.get_test_table("explicit_db_platform_test")?;

        let listener = Arc::new(Mutex::new(SqlDispatchEventListener::default()));

        {
            let listener = listener.clone();
            event_manager.add_listener(move |_: &mut SchemaCreateTableEvent| {
                listener.lock().unwrap().on_schema_create_table_calls += 1;
                Ok(())
            });
        }

        schema_manager.create_table(&table).await?;
        assert_eq!(listener.lock().unwrap().on_schema_create_table_calls, 1);

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn diff_list_table_columns() -> Result<()> {
        let offline_table = create_list_table_columns()?;
        let helper = FunctionalTestsHelper::default().await;
        helper.drop_and_create_table(&offline_table).await?;

        let schema_manager = helper.get_schema_manager();

        let online_table = schema_manager
            .introspect_table("list_table_columns")
            .await?;
        let comparator = schema_manager.create_comparator();
        let diff = comparator.diff_table(&online_table, &offline_table)?;

        assert!(
            diff.is_none(),
            "No differences should be detected with the offline vs online schema."
        );

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_table_indexes() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();

        let mut table = get_test_composite_table("list_table_indexes_test");
        table.add_unique_index(&["test"], Some("test_index_name"), Default::default())?;
        table.add_index(Index::new(
            "test_composite_idx",
            &["id", "test"],
            false,
            false,
            &[],
            Default::default(),
        ));

        helper.drop_and_create_table(&table).await?;

        let table_indexes = schema_manager
            .list_table_indexes("list_table_indexes_test")
            .await?;

        assert_eq!(table_indexes.len(), 3);

        let primary = table_indexes.iter().find(|idx| idx.is_primary());
        assert!(
            primary.is_some(),
            r#"list_table_indexes() has to return a "primary" index."#
        );

        let primary = primary.unwrap();
        assert_eq!(
            primary
                .get_columns()
                .iter()
                .map(|n| n.to_lowercase())
                .collect::<Vec<_>>(),
            vec!["id", "other_id"]
        );
        assert!(primary.is_primary());
        assert!(primary.is_unique());

        let test_index = table_indexes
            .iter()
            .find(|idx| idx.get_name() == "test_index_name")
            .unwrap();
        assert_eq!(
            test_index
                .get_columns()
                .iter()
                .map(|n| n.to_lowercase())
                .collect::<Vec<_>>(),
            vec!["test"]
        );
        assert!(!test_index.is_primary());
        assert!(test_index.is_unique());

        let test_index = table_indexes
            .iter()
            .find(|idx| idx.get_name() == "test_composite_idx")
            .unwrap();
        assert_eq!(
            test_index
                .get_columns()
                .iter()
                .map(|n| n.to_lowercase())
                .collect::<Vec<_>>(),
            vec!["id", "test"]
        );
        assert!(!test_index.is_primary());
        assert!(!test_index.is_unique());

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn drop_and_create_index() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();

        let mut table = helper.get_test_table("test_create_index")?;
        table.add_unique_index(&["test"], Some("test"), Default::default())?;
        helper.drop_and_create_table(&table).await?;

        let index = table.get_index(&"test").unwrap();
        schema_manager.drop_index(&index, &table).await?;
        schema_manager.create_index(&index, &table).await?;

        let table_indexes = schema_manager
            .list_table_indexes("test_create_index")
            .await?;

        let test_index = table_indexes
            .iter()
            .find(|i| i.get_name() == "test")
            .unwrap();
        assert_eq!(test_index.get_columns(), vec!["test"]);
        assert!(test_index.is_unique());
        assert!(!test_index.is_primary());

        Ok(())
    }

    #[cfg(any(feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn drop_and_create_unique_constraint() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();

        let mut table = Table::new("test_unique_constraint");
        table.add_column(Column::new("id", INTEGER)?);

        helper.drop_and_create_table(&table).await?;

        let unique_constraint = UniqueConstraint::new("uniq_id", &["id"], &[], Default::default());
        schema_manager
            .create_unique_constraint(&unique_constraint, &table)
            .await?;

        // there's currently no API for introspecting unique constraints,
        // so introspect the underlying indexes instead
        let indexes = schema_manager
            .list_table_indexes("test_unique_constraint")
            .await?;
        assert_eq!(indexes.len(), 1);

        let index = indexes.first().unwrap();
        assert_eq!(index.get_name(), "uniq_id");
        assert!(index.is_unique());

        schema_manager
            .drop_unique_constraint(&unique_constraint, &table)
            .await?;

        let indexes = schema_manager
            .list_table_indexes("test_unique_constraint")
            .await?;
        assert!(indexes.is_empty());

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn create_table_with_foreign_keys() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();
        let table_b = helper.get_test_table("test_foreign")?;

        let _ = schema_manager.drop_table(&"test_create_fk").await;
        helper.drop_and_create_table(&table_b).await?;

        let mut table_a = helper.get_test_table("test_create_fk")?;
        table_a.add_foreign_key_constraint(
            &["foreign_key_test"],
            &["id"],
            "test_foreign",
            Default::default(),
            None,
            None,
            Some("test_foreign"),
        )?;

        helper.drop_and_create_table(&table_a).await?;

        let fk_table = schema_manager.introspect_table("test_create_fk").await?;
        let fk_constraints = fk_table.get_foreign_keys();
        assert_eq!(
            fk_constraints.len(),
            1,
            "Table 'test_create_fk' has to have one foreign key."
        );

        let fk_constraint = fk_constraints.first().unwrap();
        assert_eq!(
            fk_constraint.get_local_columns(),
            &vec!["foreign_key_test".into_identifier()]
        );
        assert_eq!(
            fk_constraint.get_foreign_columns(),
            &vec!["id".into_identifier()]
        );

        assert!(fk_table.columns_are_indexed(fk_constraint.get_local_columns().as_slice()));

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn list_foreign_keys() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        helper.create_test_table("test_create_fk1").await?;
        helper.create_test_table("test_create_fk2").await?;

        let foreign_key = ForeignKeyConstraint::new(
            &["foreign_key_test"],
            &["id"],
            "test_create_fk2",
            Default::default(),
            None,
            Some(ForeignKeyReferentialAction::Cascade),
        );

        let schema_manager = helper.get_schema_manager();
        schema_manager
            .create_foreign_key(&foreign_key, &"test_create_fk1")
            .await?;

        let fkeys = schema_manager
            .list_table_foreign_keys("test_create_fk1")
            .await?;
        assert_eq!(
            fkeys.len(),
            1,
            "Table 'test_create_fk1' has to have one foreign key."
        );

        let fkey = fkeys.get(0).unwrap();
        let local_cols = fkey.get_local_columns();
        let foreign_cols = fkey.get_foreign_columns();
        assert_eq!(local_cols.len(), 1);
        assert_eq!(
            local_cols.get(0).unwrap().to_string().to_lowercase(),
            "foreign_key_test"
        );
        assert_eq!(foreign_cols.len(), 1);
        assert_eq!(
            foreign_cols.get(0).unwrap().to_string().to_lowercase(),
            "id"
        );
        assert_eq!(
            fkey.get_foreign_table().to_string().to_lowercase(),
            "test_create_fk2"
        );

        if let Some(on_delete) = fkey.on_delete {
            assert_eq!(on_delete, ForeignKeyReferentialAction::Cascade);
        }

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn create_foreign_key_with_table_object() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        helper.create_test_table("test_create_fk1").await?;
        helper.create_test_table("test_create_fk2").await?;

        let schema_manager = helper.get_schema_manager();
        let mut table = schema_manager.introspect_table("test_create_fk1").await?;
        table.add_foreign_key_constraint(
            &["foreign_key_test"],
            &["id"],
            "test_create_fk2",
            Default::default(),
            None,
            None,
            Some("i"),
        )?;

        let foreign_key = table
            .get_foreign_keys()
            .iter()
            .find(|c| c.get_name() == "i")
            .unwrap();
        schema_manager
            .create_foreign_key(foreign_key, &table)
            .await?;

        let fkeys = schema_manager
            .list_table_foreign_keys("test_create_fk1")
            .await?;
        assert_eq!(
            fkeys.len(),
            1,
            "Table 'test_create_fk1' has to have one foreign key."
        );

        let fkey = fkeys.get(0).unwrap();
        let local_cols = fkey.get_local_columns();
        let foreign_cols = fkey.get_foreign_columns();
        assert_eq!(local_cols.len(), 1);
        assert_eq!(
            local_cols.get(0).unwrap().to_string().to_lowercase(),
            "foreign_key_test"
        );
        assert_eq!(foreign_cols.len(), 1);
        assert_eq!(
            foreign_cols.get(0).unwrap().to_string().to_lowercase(),
            "id"
        );
        assert_eq!(
            fkey.get_foreign_table().to_string().to_lowercase(),
            "test_create_fk2"
        );

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn schema_introspection() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        helper.create_test_table("test_table").await?;

        let schema_manager = helper.get_schema_manager();
        let schema = schema_manager.introspect_schema().await?;

        assert!(schema.has_table("test_table"));

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn migrate_schema() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        helper.create_test_table("table_to_alter").await?;
        helper.create_test_table("table_to_drop").await?;

        let schema_manager = helper.get_schema_manager();
        let mut schema = schema_manager.introspect_schema().await?;

        let table_to_alter = schema.get_table_mut("table_to_alter").unwrap();
        table_to_alter.add_column(Column::new("number", INTEGER)?);
        table_to_alter.drop_column("foreign_key_test");

        schema.drop_table("table_to_drop");

        let table_to_create = schema.create_table("table_to_create")?;
        table_to_create.add_column(Column::builder("id", INTEGER)?.set_notnull(true));
        table_to_create.set_primary_key(&["id"], None)?;

        schema_manager.migrate_schema(schema).await?;
        let schema = schema_manager.introspect_schema().await?;

        assert!(!schema.has_table("table_to_drop"));
        assert!(schema.has_table("table_to_create"));
        assert!(schema.has_table("table_to_alter"));
        assert!(!schema
            .get_table("table_to_alter")
            .unwrap()
            .has_column("foreign_key_test"));
        assert!(schema
            .get_table("table_to_alter")
            .unwrap()
            .has_column("number"));

        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    pub async fn alter_table_scenario() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        helper.create_test_table("alter_table").await?;
        helper.create_test_table("alter_table_foreign").await?;

        let schema_manager = helper.get_schema_manager();
        let table = schema_manager.introspect_table("alter_table").await?;

        assert!(table.has_column("id"));
        assert!(table.has_column("test"));
        assert!(table.has_column("foreign_key_test"));
        assert!(table.get_foreign_keys().is_empty());
        assert_eq!(table.get_indices().len(), 1);

        let mut new_table = table.clone();
        new_table.add_column(Column::new("foo", INTEGER)?);
        new_table.drop_column("test");

        let comparator = schema_manager.create_comparator();

        let diff = comparator.diff_table(&table, &new_table)?;
        assert!(diff.is_some());

        let diff = diff.unwrap();
        schema_manager.alter_table(diff).await?;

        let table = schema_manager.introspect_table("alter_table").await?;
        assert!(!table.has_column("test"));
        assert!(table.has_column("foo"));

        let mut new_table = table.clone();
        new_table.add_index(Index::new(
            "foo_idx",
            &["foo"],
            false,
            false,
            &[],
            Default::default(),
        ));

        let diff = comparator.diff_table(&table, &new_table)?;
        assert!(diff.is_some());

        let diff = diff.unwrap();
        schema_manager.alter_table(diff).await?;

        let table = schema_manager.introspect_table("alter_table").await?;
        assert_eq!(table.get_indices().len(), 2);
        assert!(table.has_index("foo_idx"));
        let index = table.get_index("foo_idx").unwrap();
        assert_eq!(
            index
                .get_columns()
                .iter()
                .map(|c| c.to_lowercase())
                .collect::<Vec<_>>(),
            vec!["foo".to_string()]
        );
        assert!(!index.is_primary());
        assert!(!index.is_unique());

        let mut new_table = table.clone();
        new_table.drop_index("foo_idx");
        new_table.add_index(Index::new(
            "foo_idx",
            &["foo", "foreign_key_test"],
            false,
            false,
            &[],
            Default::default(),
        ));

        let diff = comparator.diff_table(&table, &new_table)?;
        assert!(diff.is_some());

        let diff = diff.unwrap();
        schema_manager.alter_table(diff).await?;

        let table = schema_manager.introspect_table("alter_table").await?;
        assert_eq!(table.get_indices().len(), 2);
        assert!(table.has_index("foo_idx"));
        assert_eq!(
            table
                .get_index("foo_idx")
                .unwrap()
                .get_columns()
                .iter()
                .map(|c| c.to_lowercase())
                .sorted()
                .collect::<Vec<_>>(),
            vec!["foo".to_string(), "foreign_key_test".to_string()]
        );

        let mut new_table = table.clone();
        new_table.drop_index("foo_idx");
        new_table.add_index(Index::new(
            "bar_idx",
            &["foo", "foreign_key_test"],
            false,
            false,
            &[],
            Default::default(),
        ));

        let diff = comparator.diff_table(&table, &new_table)?;
        assert!(diff.is_some());

        let diff = diff.unwrap();
        schema_manager.alter_table(diff).await?;

        let table = schema_manager.introspect_table("alter_table").await?;
        assert_eq!(table.get_indices().len(), 2);
        assert!(table.has_index("bar_idx"));
        assert!(!table.has_index("foo_idx"));
        assert_eq!(
            table
                .get_index("bar_idx")
                .unwrap()
                .get_columns()
                .iter()
                .map(|c| c.to_lowercase())
                .sorted()
                .collect::<Vec<_>>(),
            vec!["foo".to_string(), "foreign_key_test".to_string()]
        );
        assert!(!table.get_index("bar_idx").unwrap().is_primary());
        assert!(!table.get_index("bar_idx").unwrap().is_unique());

        let mut new_table = table.clone();
        new_table.drop_index("bar_idx");
        new_table.add_foreign_key_constraint(
            &["foreign_key_test"],
            &["id"],
            "alter_table_foreign",
            Default::default(),
            None,
            None,
            None::<&str>,
        )?;

        let diff = comparator.diff_table(&table, &new_table)?;
        assert!(diff.is_some());

        let diff = diff.unwrap();
        schema_manager.alter_table(diff).await?;

        let table = schema_manager.introspect_table("alter_table").await?;

        // don't check for index size here, some platforms automatically add indexes for foreign keys.
        assert!(!table.has_index("bar_idx"));

        let fks = table.get_foreign_keys();
        assert_eq!(fks.len(), 1);

        let foreign_key = fks.first().unwrap();
        assert_eq!(
            foreign_key.get_foreign_table().get_name().to_lowercase(),
            "alter_table_foreign"
        );
        assert_eq!(
            foreign_key
                .get_local_columns()
                .iter()
                .map(|c| c.get_name().to_lowercase())
                .collect::<Vec<_>>(),
            vec!["foreign_key_test"]
        );
        assert_eq!(
            foreign_key
                .get_foreign_columns()
                .iter()
                .map(|c| c.get_name().to_lowercase())
                .collect::<Vec<_>>(),
            vec!["id"]
        );

        Ok(())
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    #[serial]
    pub async fn table_in_namespace() -> Result<()> {
        let helper = FunctionalTestsHelper::default().await;
        let schema_manager = helper.get_schema_manager();

        let mut diff = SchemaDiff::default();
        diff.new_namespaces.push("testschema".to_string());

        _exec_sql(
            &helper.connection,
            schema_manager.get_alter_schema_sql(diff)?,
        )
        .await?;

        // Test if table is create in namespace
        helper
            .create_test_table("testschema.my_table_in_namespace")
            .await?;
        assert!(schema_manager
            .list_table_names()
            .await?
            .iter()
            .any(|n| n == "testschema.my_table_in_namespace"));

        // Tables without namespace should be created in default namespace
        // Default namespaces are ignored in table listings
        helper
            .create_test_table("my_table_not_in_namespace")
            .await?;
        assert!(schema_manager
            .list_table_names()
            .await?
            .iter()
            .any(|n| n == "my_table_not_in_namespace"));

        Ok(())
    }
}
