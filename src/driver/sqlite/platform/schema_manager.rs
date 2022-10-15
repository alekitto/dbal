use super::sqlite;
use crate::platform::CreateFlags;
use crate::schema::{
    Column, ColumnData, Comparator, ForeignKeyConstraint, GenericComparator, Identifier, Index,
    IntoIdentifier, SchemaManager, Table, TableDiff, TableOptions,
};
use crate::{Connection, Error, Result, Row};

pub struct SQLiteSchemaManager<'a> {
    connection: &'a Connection,
}

impl<'a> SQLiteSchemaManager<'a> {
    pub fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }
}

pub trait AbstractSQLiteSchemaManager: SchemaManager {}
impl AbstractSQLiteSchemaManager for SQLiteSchemaManager<'_> {}

impl<'a> SchemaManager for SQLiteSchemaManager<'a> {
    fn as_dyn(&self) -> &dyn SchemaManager {
        self
    }

    #[inline(always)]
    fn get_list_table_columns_sql(&self, table: &str, _: Option<&str>) -> Result<String> {
        sqlite::get_list_table_columns_sql(self, table)
    }

    #[inline(always)]
    fn get_list_tables_sql(&self) -> Result<String> {
        sqlite::get_list_tables_sql()
    }

    #[inline(always)]
    fn get_list_views_sql(&self, _: &str) -> Result<String> {
        sqlite::get_list_views_sql()
    }

    #[inline(always)]
    fn get_list_table_foreign_keys_sql(&self, table: &str) -> Result<String> {
        sqlite::get_list_table_foreign_keys_sql(self, table)
    }

    #[inline(always)]
    fn get_drop_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>> {
        sqlite::get_drop_tables_sql(self, tables)
    }

    #[inline(always)]
    fn get_create_table_sql(
        &self,
        table: &Table,
        create_flags: Option<CreateFlags>,
    ) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        sqlite::get_create_table_sql(self, table, create_flags)
    }

    #[inline(always)]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        sqlite::get_alter_table_sql(self, diff)
    }

    fn get_list_table_constraints_sql(&self, table: &str) -> Result<String> {
        sqlite::get_list_table_constraints_sql(self, table)
    }

    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>> {
        sqlite::_get_create_table_sql(self, name, columns, options)
    }

    fn get_inline_column_comment_sql(&self, comment: &str) -> Result<String> {
        sqlite::get_inline_column_comment_sql(comment)
    }

    fn get_create_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>> {
        sqlite::get_create_tables_sql(self, tables)
    }

    fn get_create_primary_key_sql(&self, _: &Index, _: &dyn IntoIdentifier) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sqlite platform does not support alter primary key.",
        ))
    }

    fn get_drop_foreign_key_sql(
        &self,
        _: &dyn IntoIdentifier,
        _: &dyn IntoIdentifier,
    ) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sqlite platform does not support alter foreign key.",
        ))
    }
    fn get_pre_alter_table_index_foreign_key_sql(&self, _: &mut TableDiff) -> Result<Vec<String>> {
        sqlite::get_pre_alter_table_index_foreign_key_sql()
    }

    fn get_post_alter_table_index_foreign_key_sql(&self, diff: &TableDiff) -> Result<Vec<String>> {
        sqlite::get_post_alter_table_index_foreign_key_sql(self, diff)
    }

    fn get_advanced_foreign_key_options_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        sqlite::get_advanced_foreign_key_options_sql(self, foreign_key)
    }

    fn get_create_foreign_key_sql(
        &self,
        _: &ForeignKeyConstraint,
        _: &dyn IntoIdentifier,
    ) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sqlite platform does not support alter foreign key.",
        ))
    }

    fn get_connection(&self) -> &'a Connection {
        self.connection
    }

    fn get_portable_table_column_definition(&self, table_column: &Row) -> Result<Column> {
        todo!()
    }

    fn create_comparator(&self) -> Box<dyn Comparator + Send + '_> {
        Box::new(GenericComparator::new(self))
    }
}

#[cfg(test)]
mod tests {
    use crate::r#type::{BOOLEAN, INTEGER, STRING};
    use crate::schema::{
        ChangedProperty, Column, ColumnDiff, Index, Table, TableDiff, UniqueConstraint,
    };
    use crate::tests::create_connection;
    use std::collections::HashMap;

    #[tokio::test]
    pub async fn generates_table_creation_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let mut table = Table::new("test");
        let mut id_column = Column::new("id", INTEGER).unwrap();
        id_column.set_notnull(true);
        id_column.set_autoincrement(true);
        table.add_column(id_column);

        let mut test_column = Column::new("test", STRING).unwrap();
        test_column.set_notnull(false);
        test_column.set_length(255);
        table.add_column(test_column);

        table
            .set_primary_key(&["id"], None)
            .expect("Failed to set primary key");

        let sql = schema_manager
            .get_create_table_sql(&table, None)
            .expect("Failed to generate table SQL");
        assert_eq!(sql, vec![
            "CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, test VARCHAR(255) DEFAULT NULL, PRIMARY KEY(id))"
        ]);
    }

    #[tokio::test]
    pub async fn generate_table_with_multi_column_unique_index() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let mut table = Table::new("test");
        let mut foo_column = Column::new("foo", STRING).unwrap();
        foo_column.set_notnull(false);
        foo_column.set_length(255);
        table.add_column(foo_column);

        let mut bar_column = Column::new("bar", STRING).unwrap();
        bar_column.set_notnull(false);
        bar_column.set_length(255);
        table.add_column(bar_column);

        table
            .add_unique_index(&["foo", "bar"], None, HashMap::default())
            .unwrap();

        let sql = schema_manager.get_create_table_sql(&table, None).unwrap();
        assert_eq!(
            sql,
            vec![
                "CREATE TABLE test (foo VARCHAR(255) DEFAULT NULL, bar VARCHAR(255) DEFAULT NULL)",
                "CREATE UNIQUE INDEX UNIQ_D87F7E0C8C73652176FF8CAA ON test (foo, bar)"
            ]
        );
    }

    #[tokio::test]
    pub async fn generates_index_creation_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let index_def = Index::new(
            "my_idx",
            &["user_name", "last_login"],
            false,
            false,
            &[],
            HashMap::default(),
        );
        let sql = schema_manager
            .get_create_index_sql(&index_def, &"mytable")
            .unwrap();

        assert_eq!(
            sql,
            "CREATE INDEX my_idx ON mytable (user_name, last_login)"
        );
    }

    #[tokio::test]
    pub async fn generates_unique_index_creation_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let index_def = Index::new(
            "index_name",
            &["test", "test2"],
            true,
            false,
            &[],
            HashMap::default(),
        );
        let sql = schema_manager
            .get_create_index_sql(&index_def, &"test")
            .unwrap();

        assert_eq!(sql, "CREATE UNIQUE INDEX index_name ON test (test, test2)");
    }

    #[tokio::test]
    pub async fn test_generates_constraint_creation_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let idx = UniqueConstraint::new("constraint_name", &["test"], &[], HashMap::default());
        let sql = schema_manager
            .get_create_unique_constraint_sql(&idx, &"test")
            .unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE test ADD CONSTRAINT constraint_name UNIQUE (test)"
        );
    }

    #[tokio::test]
    pub async fn generates_table_alteration_sql() {
        let mut table = Table::new("mytable");
        let mut id_column = Column::new("id", INTEGER).unwrap();
        id_column.set_autoincrement(true);
        table.add_column(id_column);
        table.add_column(Column::new("foo", INTEGER).unwrap());
        table.add_column(Column::new("bar", STRING).unwrap());
        table.add_column(Column::new("bloo", BOOLEAN).unwrap());
        table.set_primary_key(&["id"], None).unwrap();

        let mut table_diff = TableDiff::new("mytable", Some(&table));
        table_diff.new_name = "userlist".to_string().into();
        let mut quota = Column::new("quota", INTEGER).unwrap();
        quota.set_notnull(false);
        table_diff.added_columns.push(quota);
        table_diff
            .removed_columns
            .push(Column::new("foo", INTEGER).unwrap());

        let mut baz = Column::new("baz", STRING).unwrap();
        baz.set_length(255);
        baz.set_default("def".into());
        table_diff.changed_columns.push(ColumnDiff::new(
            "bar",
            &baz,
            &[
                ChangedProperty::Type,
                ChangedProperty::NotNull,
                ChangedProperty::Default,
            ],
            None,
        ));

        let mut bloo_column = Column::new("bloo", BOOLEAN).unwrap();
        bloo_column.set_default(false.into());
        table_diff.changed_columns.push(ColumnDiff::new(
            "bloo",
            &bloo_column,
            &[
                ChangedProperty::Type,
                ChangedProperty::NotNull,
                ChangedProperty::Default,
            ],
            None,
        ));

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let sql = schema_manager.get_alter_table_sql(&mut table_diff).unwrap();
        assert_eq!(sql, &[
            "CREATE TEMPORARY TABLE __temp__mytable AS SELECT id, bar, bloo FROM mytable",
            "DROP TABLE mytable",
            "CREATE TABLE mytable (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, baz VARCHAR(255) DEFAULT 'def' NOT NULL, bloo BOOLEAN DEFAULT 0 NOT NULL, quota INTEGER DEFAULT NULL, PRIMARY KEY(id))",
            "INSERT INTO mytable (id, baz, bloo) SELECT id, bar, bloo FROM __temp__mytable",
            "DROP TABLE __temp__mytable",
            "ALTER TABLE mytable RENAME TO userlist",
        ]);
    }

    #[tokio::test]
    pub async fn quoted_column_in_primary_key_propagation() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let mut table = Table::new("`quoted`");
        let mut col = Column::new("create", STRING).unwrap();
        col.set_length(255);
        table.add_column(col);
        table
            .set_primary_key(&["create"], None)
            .expect("failed to set primary key");

        let sql = schema_manager.get_create_table_sql(&table, None).unwrap();
        assert_eq!(sql, &["CREATE TABLE \"quoted\" (\"create\" VARCHAR(255) NOT NULL, PRIMARY KEY(\"create\"))"]);
    }

    #[tokio::test]
    pub async fn quoted_column_in_index_propagation() {
        let mut table = Table::new("`quoted`");
        let mut col = Column::new("create", STRING).unwrap();
        col.set_length(255);
        table.add_column(col);
        table.add_index(Index::new::<&str, _, &str>(
            None,
            &["create"],
            false,
            false,
            &[],
            HashMap::default(),
        ));

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let sql = schema_manager.get_create_table_sql(&table, None).unwrap();
        assert_eq!(
            sql,
            &[
                "CREATE TABLE \"quoted\" (\"create\" VARCHAR(255) NOT NULL)",
                "CREATE INDEX IDX_22660D028FD6E0FB ON \"quoted\" (\"create\")",
            ]
        );
    }
}
