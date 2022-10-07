use super::mysql;
use crate::driver::mysql::platform::MySQLVariant;
use crate::platform::default;
use crate::schema::{
    Column, ColumnData, Comparator, ForeignKeyConstraint, GenericComparator, Identifier, Index,
    SchemaManager, TableDiff, TableOptions,
};
use crate::{Connection, Result, Row};

pub struct MySQLSchemaManager<'a> {
    variant: MySQLVariant,
    connection: &'a Connection,
}

impl<'a> MySQLSchemaManager<'a> {
    pub fn new(connection: &'a Connection, variant: MySQLVariant) -> Self {
        Self {
            variant,
            connection,
        }
    }
}

pub trait AbstractMySQLSchemaManager: SchemaManager {
    fn as_mysql_dyn(&self) -> &dyn AbstractMySQLSchemaManager;

    /// Build SQL for table options
    fn build_table_options(&self, options: &TableOptions) -> Result<String> {
        mysql::build_table_options(self.as_mysql_dyn(), options)
    }

    /// Build SQL for partition options
    fn build_partition_options(&self, options: &TableOptions) -> String {
        mysql::build_partition_options(options)
    }
}

impl AbstractMySQLSchemaManager for MySQLSchemaManager<'_> {
    fn as_mysql_dyn(&self) -> &dyn AbstractMySQLSchemaManager {
        self
    }
}

impl<'a> SchemaManager for MySQLSchemaManager<'a> {
    fn get_connection(&self) -> &'a Connection {
        self.connection
    }

    fn as_dyn(&self) -> &dyn SchemaManager {
        self
    }

    #[inline]
    fn get_rename_index_sql(
        &self,
        old_index_name: &Identifier,
        index: &Index,
        table_name: &Identifier,
    ) -> Result<Vec<String>> {
        match self.variant {
            MySQLVariant::MySQL | MySQLVariant::MySQL80 => mysql::get_rename_index_sql(
                self.get_platform()?.as_dyn(),
                old_index_name,
                index,
                table_name,
            ),
            _ => default::get_rename_index_sql(self.as_dyn(), old_index_name, index, table_name),
        }
    }

    fn get_portable_table_column_definition(&self, table_column: &Row) -> Result<Column> {
        todo!()
    }

    #[inline]
    fn get_list_databases_sql(&self) -> Result<String> {
        mysql::get_list_databases_sql()
    }

    #[inline]
    fn get_list_views_sql(&self, database: &str) -> Result<String> {
        mysql::get_list_views_sql(self.as_dyn(), database)
    }

    #[inline]
    fn get_drop_index_sql(&self, index: &Identifier, table: &Identifier) -> Result<String> {
        mysql::get_drop_index_sql(self.get_platform()?, index, table)
    }

    #[inline]
    fn get_drop_unique_constraint_sql(
        &self,
        name: &Identifier,
        table_name: &Identifier,
    ) -> Result<String> {
        mysql::get_drop_unique_constraint_sql(self.as_dyn(), name, table_name)
    }

    /// MySQL commits a transaction implicitly when DROP TABLE is executed, however not
    /// if DROP TEMPORARY TABLE is executed.
    #[inline]
    fn get_drop_temporary_table_sql(&self, table: &Identifier) -> Result<String> {
        mysql::get_drop_temporary_table_sql(self.as_dyn(), table)
    }

    #[inline]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        mysql::get_alter_table_sql(self.as_dyn(), diff)
    }

    #[inline]
    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>> {
        mysql::_get_create_table_sql(self.as_mysql_dyn(), name, columns, options)
    }

    #[inline]
    fn get_create_index_sql_flags(&self, index: &Index) -> String {
        mysql::get_create_index_sql_flags(index)
    }

    #[inline]
    fn get_pre_alter_table_index_foreign_key_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        mysql::get_pre_alter_table_index_foreign_key_sql(self.as_dyn(), diff)
    }

    #[inline]
    fn get_column_collation_declaration_sql(&self, collation: &str) -> Result<String> {
        mysql::get_column_collation_declaration_sql(self.get_platform()?.as_dyn(), collation)
    }

    #[inline]
    fn get_advanced_foreign_key_options_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        mysql::get_advanced_foreign_key_options_sql(self.as_dyn(), foreign_key)
    }

    fn create_comparator(&self) -> Box<dyn Comparator + Send + '_> {
        Box::new(GenericComparator::new(self))
    }
}

#[cfg(test)]
mod tests {
    use crate::r#type::{INTEGER, STRING};
    use crate::schema::{Column, ForeignKeyConstraint, Index, Table, UniqueConstraint};
    use crate::tests::create_connection;
    use std::collections::HashMap;

    #[tokio::test]
    pub async fn generates_table_creation_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let mut table = Table::new("test");
        let mut id_column = Column::new("id", INTEGER).unwrap();
        id_column.set_notnull(true);
        id_column.set_autoincrement(Some(true));
        table.add_column(id_column);

        let mut test_column = Column::new("test", STRING).unwrap();
        test_column.set_notnull(false);
        test_column.set_length(Some(255));
        table.add_column(test_column);

        table
            .set_primary_key(&["id"], None)
            .expect("Failed to set primary key");

        let sql = schema_manager
            .get_create_table_sql(&table, None)
            .expect("Failed to generate table SQL");
        assert_eq!(sql[0], "CREATE TABLE test (id INT AUTO_INCREMENT NOT NULL, test VARCHAR(255) DEFAULT NULL, PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE `utf8_unicode_ci` ENGINE = InnoDB");
    }

    #[tokio::test]
    pub async fn generate_table_with_multi_column_unique_index() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let mut table = Table::new("test");
        let mut foo_column = Column::new("foo", STRING).unwrap();
        foo_column.set_notnull(false);
        foo_column.set_length(Some(255));
        table.add_column(foo_column);

        let mut bar_column = Column::new("bar", STRING).unwrap();
        bar_column.set_notnull(false);
        bar_column.set_length(Some(255));
        table.add_column(bar_column);

        table
            .add_unique_index(&["foo", "bar"], None, HashMap::default())
            .unwrap();

        let sql = schema_manager.get_create_table_sql(&table, None).unwrap();
        assert_eq!(sql, vec![
            "CREATE TABLE test (foo VARCHAR(255) DEFAULT NULL, bar VARCHAR(255) DEFAULT NULL, UNIQUE INDEX UNIQ_D87F7E0C8C73652176FF8CAA (foo, bar)) DEFAULT CHARACTER SET utf8 COLLATE `utf8_unicode_ci` ENGINE = InnoDB",
        ]);
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
    pub async fn generates_foreign_key_creation_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let fk = ForeignKeyConstraint::new(
            &["fk_name_id"],
            &["id"],
            "other_table",
            HashMap::default(),
            None,
            None,
        );
        let sql = schema_manager
            .get_create_foreign_key_sql(&fk, &"test")
            .unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE test ADD FOREIGN KEY (fk_name_id) REFERENCES other_table (id)"
        );
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

        let pk = Index::new(
            "constraint_name",
            &["test"],
            true,
            true,
            &[],
            HashMap::default(),
        );
        let sql = schema_manager.get_create_index_sql(&pk, &"test").unwrap();
        assert_eq!(sql, "ALTER TABLE test ADD PRIMARY KEY (test)");

        let fk = ForeignKeyConstraint::new(
            &["fk_name"],
            &["id"],
            "foreign",
            HashMap::default(),
            None,
            None,
        );
        let sql = schema_manager
            .get_create_foreign_key_sql(&fk, &"test")
            .unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE test ADD FOREIGN KEY (fk_name) REFERENCES foreign (id)"
        );
    }
}
