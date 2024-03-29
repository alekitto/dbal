use super::mysql;
use crate::driver::mysql::platform::MySQLVariant;
use crate::platform::default;
use crate::schema::{
    Column, ColumnData, Comparator, FKConstraintList, ForeignKeyConstraint, GenericComparator,
    Identifier, Index, SchemaManager, TableDiff, TableOptions,
};
use crate::{AsyncResult, Connection, Result, Row};
use std::collections::HashMap;

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
    fn get_column_charset_declaration_sql(&self, charset: &str) -> String {
        mysql::get_column_charset_declaration_sql(self.as_dyn(), charset)
    }

    #[inline]
    fn get_list_databases_sql(&self) -> Result<String> {
        mysql::get_list_databases_sql()
    }

    fn get_list_tables_sql(&self) -> Result<String> {
        mysql::get_list_tables_sql()
    }

    #[inline]
    fn get_list_table_columns_sql(&self, table: &str, database: &str) -> Result<String> {
        mysql::get_list_table_columns_sql(self.as_dyn(), table, database)
    }

    fn get_list_table_indexes_sql(&self, table: &str, database: &str) -> Result<String> {
        mysql::get_list_table_indexes_sql(self, table, database)
    }

    fn get_list_table_foreign_keys_sql(&self, table: &str, database: &str) -> Result<String> {
        mysql::get_list_table_foreign_keys_sql(self.as_dyn(), table, database)
    }

    #[inline]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        mysql::get_alter_table_sql(self.as_dyn(), diff)
    }

    /// MySQL commits a transaction implicitly when DROP TABLE is executed, however not
    /// if DROP TEMPORARY TABLE is executed.
    #[inline]
    fn get_drop_temporary_table_sql(&self, table: &Identifier) -> Result<String> {
        mysql::get_drop_temporary_table_sql(self.as_dyn(), table)
    }

    #[inline]
    fn get_drop_index_sql(&self, index: &Identifier, table: &Identifier) -> Result<String> {
        mysql::get_drop_index_sql(self.get_platform()?, index, table)
    }

    #[inline]
    fn columns_equal(&self, column1: &Column, column2: &Column) -> Result<bool> {
        mysql::columns_equal(self.as_dyn(), column1, column2)
    }

    #[inline]
    fn get_drop_unique_constraint_sql(
        &self,
        name: &Identifier,
        table_name: &Identifier,
    ) -> Result<String> {
        mysql::get_drop_unique_constraint_sql(self.as_dyn(), name, table_name)
    }

    #[inline]
    fn get_list_views_sql(&self, database: &str) -> Result<String> {
        mysql::get_list_views_sql(self.as_dyn(), database)
    }

    #[inline]
    fn get_pre_alter_table_index_foreign_key_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        mysql::get_pre_alter_table_index_foreign_key_sql(self.as_dyn(), diff)
    }

    #[inline]
    fn get_rename_index_sql(
        &self,
        old_index_name: &Identifier,
        index: &Index,
        table_name: &Identifier,
    ) -> Result<Vec<String>> {
        match self.variant {
            MySQLVariant::MySQL5_7 | MySQLVariant::MySQL8_0 | MySQLVariant::MariaDB => {
                mysql::get_rename_index_sql(
                    self.get_platform()?.as_dyn(),
                    old_index_name,
                    index,
                    table_name,
                )
            }
            _ => default::get_rename_index_sql(self.as_dyn(), old_index_name, index, table_name),
        }
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

    fn get_portable_table_column_definition(&self, table_column: &Row) -> Result<Column> {
        mysql::get_portable_table_column_definition(self.as_dyn(), table_column)
    }

    fn get_portable_table_foreign_keys_list(
        &self,
        table_foreign_keys: Vec<Row>,
    ) -> Result<FKConstraintList> {
        mysql::get_portable_table_foreign_keys_list(self.as_dyn(), table_foreign_keys)
    }

    fn create_comparator(&self) -> Box<dyn Comparator + Send + '_> {
        Box::new(GenericComparator::new(self))
    }

    fn fetch_table_options_by_table(
        &self,
        database_name: &str,
        table_name: Option<&str>,
    ) -> AsyncResult<HashMap<String, Row>> {
        let database_name = database_name.to_string();
        let table_name = table_name.map(|t| t.to_string());
        Box::pin(async move {
            mysql::fetch_table_options_by_table(self.as_dyn(), database_name, table_name).await
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::platform::CreateFlags;
    use crate::r#type::{IntoType, BOOLEAN};
    use crate::r#type::{INTEGER, SIMPLE_ARRAY, STRING};
    use crate::schema::{
        Asset, ChangedProperty, Column, ColumnDiff, ForeignKeyConstraint, Index, Table, TableDiff,
        UniqueConstraint,
    };
    use crate::tests::create_connection;
    use crate::Result;
    use std::collections::HashMap;
    use version_compare::{compare_to, Cmp};

    #[tokio::test]
    pub async fn generates_table_creation_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let mut table = Table::new("test");
        let mut id_column = Column::new("id", INTEGER.into_type()?);
        id_column.set_notnull(true);
        id_column.set_autoincrement(true);
        table.add_column(id_column);

        let mut test_column = Column::new("test", STRING.into_type()?);
        test_column.set_notnull(false);
        test_column.set_length(255);
        table.add_column(test_column);

        table.set_primary_key(&["id"], None)?;

        let sql = schema_manager.get_create_table_sql(&table, None)?;
        assert_eq!(sql[0], "CREATE TABLE test (id INT AUTO_INCREMENT NOT NULL, test VARCHAR(255) DEFAULT NULL, PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE `utf8_unicode_ci` ENGINE = InnoDB");

        Ok(())
    }

    #[tokio::test]
    pub async fn generate_table_with_multi_column_unique_index() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let mut table = Table::new("test");
        let mut foo_column = Column::new("foo", STRING.into_type()?);
        foo_column.set_notnull(false);
        foo_column.set_length(255);
        table.add_column(foo_column);

        let mut bar_column = Column::new("bar", STRING.into_type()?);
        bar_column.set_notnull(false);
        bar_column.set_length(255);
        table.add_column(bar_column);

        table.add_unique_index(&["foo", "bar"], None, HashMap::default())?;

        let sql = schema_manager.get_create_table_sql(&table, None)?;
        assert_eq!(sql, vec![
            "CREATE TABLE test (foo VARCHAR(255) DEFAULT NULL, bar VARCHAR(255) DEFAULT NULL, UNIQUE INDEX UNIQ_D87F7E0C8C73652176FF8CAA (foo, bar)) DEFAULT CHARACTER SET utf8 COLLATE `utf8_unicode_ci` ENGINE = InnoDB",
        ]);

        Ok(())
    }

    #[tokio::test]
    pub async fn generates_index_creation_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let index_def = Index::new(
            "my_idx",
            &["user_name", "last_login"],
            false,
            false,
            &[],
            HashMap::default(),
        );

        let sql = schema_manager.get_create_index_sql(&index_def, &"mytable")?;
        assert_eq!(
            sql,
            "CREATE INDEX my_idx ON mytable (user_name, last_login)"
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn generates_unique_index_creation_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let index_def = Index::new(
            "index_name",
            &["test", "test2"],
            true,
            false,
            &[],
            HashMap::default(),
        );

        let sql = schema_manager.get_create_index_sql(&index_def, &"test")?;
        assert_eq!(sql, "CREATE UNIQUE INDEX index_name ON test (test, test2)");

        Ok(())
    }

    #[tokio::test]
    pub async fn generates_foreign_key_creation_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let fk = ForeignKeyConstraint::new(
            &["fk_name_id"],
            &["id"],
            "other_table",
            HashMap::default(),
            None,
            None,
        );

        let sql = schema_manager.get_create_foreign_key_sql(&fk, &"test")?;
        assert_eq!(
            sql,
            "ALTER TABLE test ADD FOREIGN KEY (fk_name_id) REFERENCES other_table (id)"
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn generates_constraint_creation_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let idx = UniqueConstraint::new("constraint_name", &["test"], &[], HashMap::default());
        let sql = schema_manager.get_create_unique_constraint_sql(&idx, &"test")?;
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
        let sql = schema_manager.get_create_index_sql(&pk, &"test")?;
        assert_eq!(sql, "ALTER TABLE test ADD PRIMARY KEY (test)");

        let fk = ForeignKeyConstraint::new(
            &["fk_name"],
            &["id"],
            "foreign",
            HashMap::default(),
            None,
            None,
        );

        let sql = schema_manager.get_create_foreign_key_sql(&fk, &"test")?;
        assert_eq!(
            sql,
            "ALTER TABLE test ADD FOREIGN KEY (fk_name) REFERENCES `foreign` (id)"
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn generates_table_alteration_sql() -> Result<()> {
        let mut table = Table::new("mytable");
        let mut id_column = Column::new("id", INTEGER.into_type()?);
        id_column.set_autoincrement(true);
        table.add_column(id_column);
        table.add_column(Column::new("foo", INTEGER.into_type()?));
        table.add_column(Column::new("bar", STRING.into_type()?));
        table.add_column(Column::new("bloo", BOOLEAN.into_type()?));
        table.set_primary_key(&["id"], None).unwrap();

        let mut table_diff = TableDiff::new("mytable", Some(&table));
        table_diff.new_name = "userlist".to_string().into();
        let mut quota = Column::new("quota", INTEGER.into_type()?);
        quota.set_notnull(false);
        table_diff.added_columns.push(quota);
        table_diff
            .removed_columns
            .push(Column::new("foo", INTEGER.into_type()?));

        let mut baz = Column::new("baz", STRING.into_type()?);
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

        let mut bloo_column = Column::new("bloo", BOOLEAN.into_type()?);
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

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let sql = schema_manager.get_alter_table_sql(&mut table_diff)?;
        assert_eq!(sql, &["ALTER TABLE mytable RENAME TO userlist, ADD quota INT DEFAULT NULL, DROP foo, CHANGE bar baz VARCHAR(255) DEFAULT 'def' NOT NULL, CHANGE bloo bloo TINYINT(1) DEFAULT 0 NOT NULL COMMENT '(CRType:boolean)'"]);

        Ok(())
    }

    #[tokio::test]
    pub async fn create_table_column_comments() -> Result<()> {
        let mut table = Table::new("test");
        let mut id_col = Column::new("id", INTEGER.into_type()?);
        id_col.set_comment("This is a comment");
        table.add_column(id_col);
        table.set_primary_key(&["id"], None)?;

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let sql = schema_manager.get_create_table_sql(&table, None)?;
        assert_eq!(sql, &["CREATE TABLE test (id INT NOT NULL COMMENT 'This is a comment', PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE `utf8_unicode_ci` ENGINE = InnoDB"]);

        Ok(())
    }

    #[tokio::test]
    pub async fn alter_table_column_comments() -> Result<()> {
        let mut table_diff = TableDiff::new("mytable", None);
        let mut col = Column::new("quota", INTEGER.into_type()?);
        col.set_comment("A comment");
        table_diff.added_columns.push(col);

        table_diff.changed_columns.push(ColumnDiff::new(
            "foo",
            &Column::new("foo", STRING.into_type()?),
            &[ChangedProperty::Comment],
            None,
        ));
        let mut col = Column::new("baz", STRING.into_type()?);
        col.set_comment("B comment");
        table_diff.changed_columns.push(ColumnDiff::new(
            "bar",
            &col,
            &[ChangedProperty::Comment],
            None,
        ));

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let sql = schema_manager.get_alter_table_sql(&mut table_diff)?;
        assert_eq!(sql, &["ALTER TABLE mytable ADD quota INT NOT NULL COMMENT 'A comment', CHANGE foo foo VARCHAR(255) NOT NULL, CHANGE bar baz VARCHAR(255) NOT NULL COMMENT 'B comment'"]);

        Ok(())
    }

    #[tokio::test]
    pub async fn create_table_column_type_comments() -> Result<()> {
        let mut table = Table::new("test");
        table.add_column(Column::new("id", INTEGER.into_type()?));
        table.add_column(Column::new("data", SIMPLE_ARRAY.into_type()?));
        table.set_primary_key(&["id"], None).unwrap();

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let sql = schema_manager.get_create_table_sql(&table, None)?;
        assert_eq!(sql, &[
            "CREATE TABLE test (id INT NOT NULL, data LONGTEXT NOT NULL COMMENT '(CRType:simple_array)', PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8 COLLATE `utf8_unicode_ci` ENGINE = InnoDB",
        ]);

        Ok(())
    }

    #[tokio::test]
    pub async fn quoted_column_in_primary_key_propagation() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let mut table = Table::new("`quoted`");
        table.add_column(Column::new("create", STRING.into_type()?));
        table.set_primary_key(&["create"], None)?;

        let sql = schema_manager.get_create_table_sql(&table, None)?;
        assert_eq!(sql, &["CREATE TABLE `quoted` (`create` VARCHAR(255) NOT NULL, PRIMARY KEY(`create`)) DEFAULT CHARACTER SET utf8 COLLATE `utf8_unicode_ci` ENGINE = InnoDB"]);

        Ok(())
    }

    #[tokio::test]
    pub async fn quoted_column_in_index_propagation() -> Result<()> {
        let mut table = Table::new("`quoted`");
        table.add_column(Column::new("create", STRING.into_type()?));
        table.add_index(Index::new::<&str, _, &str>(
            None,
            &["create"],
            false,
            false,
            &[],
            HashMap::default(),
        ));

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let sql = schema_manager.get_create_table_sql(&table, None)?;
        assert_eq!(sql, &[
            "CREATE TABLE `quoted` (`create` VARCHAR(255) NOT NULL, INDEX IDX_22660D028FD6E0FB (`create`)) DEFAULT CHARACTER SET utf8 COLLATE `utf8_unicode_ci` ENGINE = InnoDB",
        ]);

        Ok(())
    }

    #[tokio::test]
    pub async fn test_quoted_name_in_index_sql() -> Result<()> {
        let mut table = Table::new("test");
        table.add_column(Column::new("column1", STRING.into_type()?));
        table.add_index(Index::new::<_, _, &str>(
            "`key`",
            &["column1"],
            false,
            false,
            &[],
            HashMap::default(),
        ));

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let sql = schema_manager.get_create_table_sql(&table, None)?;
        assert_eq!(sql, &[
            "CREATE TABLE test (column1 VARCHAR(255) NOT NULL, INDEX `key` (column1)) DEFAULT CHARACTER SET utf8 COLLATE `utf8_unicode_ci` ENGINE = InnoDB",
        ]);

        Ok(())
    }

    #[tokio::test]
    pub async fn quoted_column_in_foreign_key_propagation() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let mut table = Table::new("`quoted`");
        table.add_column(Column::new("create", STRING.into_type()?));
        table.add_column(Column::new("foo", STRING.into_type()?));
        table.add_column(Column::new("`bar`", STRING.into_type()?));

        // Foreign table with reserved keyword as name (needs quotation).
        let mut foreign_table = Table::new("foreign");

        // Foreign column with reserved keyword as name (needs quotation).
        foreign_table.add_column(Column::new("create", STRING.into_type()?));

        // Foreign column with non-reserved keyword as name (does not need quotation).
        foreign_table.add_column(Column::new("bar", STRING.into_type()?));

        // Foreign table with special character in name
        foreign_table.add_column(Column::new("`foo-bar`", STRING.into_type()?));

        table.add_foreign_key_constraint(
            &["create", "foo", "`bar`"],
            &["create", "bar", "`foo-bar`"],
            &foreign_table,
            HashMap::default(),
            None,
            None,
            Some("FK_WITH_RESERVED_KEYWORD"),
        )?;

        // Foreign table with non-reserved keyword as name (does not need quotation).
        let mut foreign_table = Table::new("foo");

        // Foreign column with reserved keyword as name (needs quotation).
        foreign_table.add_column(Column::new("create", STRING.into_type()?));

        // Foreign column with non-reserved keyword as name (does not need quotation).
        foreign_table.add_column(Column::new("bar", STRING.into_type()?));

        // Foreign table with special character in name
        foreign_table.add_column(Column::new("`foo-bar`", STRING.into_type()?));

        table.add_foreign_key_constraint(
            &["create", "foo", "`bar`"],
            &["create", "bar", "`foo-bar`"],
            &foreign_table,
            HashMap::default(),
            None,
            None,
            Some("FK_WITH_NON_RESERVED_KEYWORD"),
        )?;

        // Foreign table with special character in name.
        let mut foreign_table = Table::new("`foo-bar`");

        // Foreign column with reserved keyword as name (needs quotation).
        foreign_table.add_column(Column::new("create", STRING.into_type()?));

        // Foreign column with non-reserved keyword as name (does not need quotation).
        foreign_table.add_column(Column::new("bar", STRING.into_type()?));

        // Foreign table with special character in name
        foreign_table.add_column(Column::new("`foo-bar`", STRING.into_type()?));

        table.add_foreign_key_constraint(
            &["create", "foo", "`bar`"],
            &["create", "bar", "`foo-bar`"],
            &foreign_table,
            HashMap::default(),
            None,
            None,
            Some("FK_WITH_INTENDED_QUOTATION"),
        )?;

        let sql = schema_manager.get_create_table_sql(
            &table,
            Some(CreateFlags::CREATE_INDEXES | CreateFlags::CREATE_FOREIGN_KEYS),
        )?;
        assert_eq!(sql, &[
            "CREATE TABLE `quoted` (`create` VARCHAR(255) NOT NULL, foo VARCHAR(255) NOT NULL, \
            `bar` VARCHAR(255) NOT NULL, INDEX IDX_22660D028FD6E0FB8C73652176FF8CAA (`create`, foo, `bar`)) \
            DEFAULT CHARACTER SET utf8 COLLATE `utf8_unicode_ci` ENGINE = InnoDB",
            "ALTER TABLE `quoted` ADD CONSTRAINT FK_WITH_RESERVED_KEYWORD FOREIGN KEY (`create`, foo, `bar`) \
            REFERENCES `foreign` (`create`, bar, `foo-bar`)",
            "ALTER TABLE `quoted` ADD CONSTRAINT FK_WITH_NON_RESERVED_KEYWORD FOREIGN KEY (`create`, foo, `bar`) \
            REFERENCES foo (`create`, bar, `foo-bar`)",
            "ALTER TABLE `quoted` ADD CONSTRAINT FK_WITH_INTENDED_QUOTATION FOREIGN KEY (`create`, foo, `bar`) \
            REFERENCES `foo-bar` (`create`, bar, `foo-bar`)",
        ]);

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_reserved_keyword_in_unique_constraint_declaration_sql() -> Result<()> {
        let constraint = UniqueConstraint::new("select", &["foo"], &[], HashMap::default());

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let sql = schema_manager.get_unique_constraint_declaration_sql("select", &constraint)?;
        assert_eq!(sql, "CONSTRAINT `select` UNIQUE (foo)");

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_reserved_keyword_in_truncate_table_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        assert_eq!(
            schema_manager.get_truncate_table_sql(&"select", false)?,
            "TRUNCATE `select`"
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_reserved_keyword_in_index_declaration_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let index = Index::new("select", &["foo"], false, false, &[], HashMap::default());

        assert_eq!(
            schema_manager.get_index_declaration_sql("select", &index)?,
            "INDEX `select` (foo)"
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn get_create_schema_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        assert!(schema_manager.get_create_schema_sql(&"schema").is_err());

        Ok(())
    }

    #[tokio::test]
    pub async fn alter_table_change_quoted_column() -> Result<()> {
        let mut table = Table::new("mytable");
        table.add_column(Column::new("select", INTEGER.into_type()?));

        let mut table_diff = TableDiff::new("mytable", &table);
        table_diff.changed_columns.push(ColumnDiff::new(
            "select",
            &Column::new("select", STRING.into_type()?),
            &[ChangedProperty::Type],
            None,
        ));

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let platform = schema_manager.get_platform()?;
        let sql = schema_manager.get_alter_table_sql(&mut table_diff)?;
        assert!(sql.join(";").contains(&platform.quote_identifier("select")));

        Ok(())
    }

    #[tokio::test]
    pub async fn alter_table_rename_index() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let mut table = Table::new("mytable");
        table.add_column(Column::new("id", INTEGER.into_type()?));
        table.set_primary_key(&["id"], None)?;

        let mut table_diff = TableDiff::new("mytable", &table);
        table_diff.renamed_indexes.push((
            "idx_foo".to_string(),
            Index::new("idx_bar", &["id"], false, false, &[], Default::default()),
        ));

        let alter_table_sql = schema_manager.get_alter_table_sql(&mut table_diff)?;
        let version = connection.server_version().await?;
        if compare_to(&version, "5.7", Cmp::Lt).unwrap()
            || (compare_to(&version, "10", Cmp::Ge).unwrap()
                && compare_to(&version, "10.5.2", Cmp::Lt).unwrap())
        {
            assert_eq!(
                alter_table_sql,
                &[
                    "DROP INDEX idx_foo ON mytable",
                    "CREATE INDEX idx_bar ON mytable (id)"
                ]
            )
        } else {
            assert_eq!(
                alter_table_sql,
                &["ALTER TABLE mytable RENAME INDEX idx_foo TO idx_bar"]
            );
        }

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_alter_table_rename_index() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let mut table = Table::new("table");
        table.add_column(Column::new("id", INTEGER.into_type()?));
        table.set_primary_key(&["id"], None)?;

        let mut table_diff = TableDiff::new("table", &table);

        table_diff.renamed_indexes.push((
            "create".to_string(),
            Index::new("select", &["id"], false, false, &[], HashMap::default()),
        ));
        table_diff.renamed_indexes.push((
            "`foo`".to_string(),
            Index::new("`bar`", &["id"], false, false, &[], HashMap::default()),
        ));

        let sql = schema_manager.get_alter_table_sql(&mut table_diff)?;
        let version = connection.server_version().await?;
        if compare_to(&version, "5.7", Cmp::Lt).unwrap()
            || (compare_to(&version, "10", Cmp::Ge).unwrap()
                && compare_to(&version, "10.5.2", Cmp::Lt).unwrap())
        {
            assert_eq!(
                sql,
                &[
                    "DROP INDEX `create` ON `table`",
                    "CREATE INDEX `select` ON `table` (id)",
                    "DROP INDEX `foo` ON `table`",
                    "CREATE INDEX `bar` ON `table` (id)",
                ]
            );
        } else {
            assert_eq!(
                sql,
                &[
                    "ALTER TABLE `table` RENAME INDEX `create` TO `select`",
                    "ALTER TABLE `table` RENAME INDEX `foo` TO `bar`",
                ]
            );
        }

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_alter_table_rename_column() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let mut from_table = Table::new("mytable");

        from_table.add_column({
            let mut column = Column::new("unquoted1", INTEGER.into_type()?);
            column.set_comment("Unquoted 1");
            column
        });
        from_table.add_column({
            let mut column = Column::new("unquoted2", INTEGER.into_type()?);
            column.set_comment("Unquoted 2");
            column
        });
        from_table.add_column({
            let mut column = Column::new("unquoted3", INTEGER.into_type()?);
            column.set_comment("Unquoted 3");
            column
        });

        from_table.add_column({
            let mut column = Column::new("create", INTEGER.into_type()?);
            column.set_comment("Reserved keyword 1");
            column
        });
        from_table.add_column({
            let mut column = Column::new("table", INTEGER.into_type()?);
            column.set_comment("Reserved keyword 2");
            column
        });
        from_table.add_column({
            let mut column = Column::new("select", INTEGER.into_type()?);
            column.set_comment("Reserved keyword 3");
            column
        });

        from_table.add_column({
            let mut column = Column::new("`quoted1`", INTEGER.into_type()?);
            column.set_comment("Quoted 1");
            column
        });
        from_table.add_column({
            let mut column = Column::new("`quoted2`", INTEGER.into_type()?);
            column.set_comment("Quoted 2");
            column
        });
        from_table.add_column({
            let mut column = Column::new("`quoted3`", INTEGER.into_type()?);
            column.set_comment("Quoted 3");
            column
        });

        let mut to_table = Table::new("mytable");

        // unquoted -> unquoted
        to_table.add_column({
            let mut column = Column::new("unquoted", INTEGER.into_type()?);
            column.set_comment("Unquoted 1");
            column
        });

        // unquoted -> reserved keyword
        to_table.add_column({
            let mut column = Column::new("where", INTEGER.into_type()?);
            column.set_comment("Unquoted 2");
            column
        });

        // unquoted -> quoted
        to_table.add_column({
            let mut column = Column::new("`foo`", INTEGER.into_type()?);
            column.set_comment("Unquoted 3");
            column
        });

        // reserved keyword -> unquoted
        to_table.add_column({
            let mut column = Column::new("reserved_keyword", INTEGER.into_type()?);
            column.set_comment("Reserved keyword 1");
            column
        });

        // reserved keyword -> reserved keyword
        to_table.add_column({
            let mut column = Column::new("from", INTEGER.into_type()?);
            column.set_comment("Reserved keyword 2");
            column
        });

        // reserved keyword -> quoted
        to_table.add_column({
            let mut column = Column::new("`bar`", INTEGER.into_type()?);
            column.set_comment("Reserved keyword 3");
            column
        });

        // quoted -> unquoted
        to_table.add_column({
            let mut column = Column::new("quoted", INTEGER.into_type()?);
            column.set_comment("Quoted 1");
            column
        });

        // quoted -> reserved keyword
        to_table.add_column({
            let mut column = Column::new("and", INTEGER.into_type()?);
            column.set_comment("Quoted 2");
            column
        });

        // quoted -> quoted
        to_table.add_column({
            let mut column = Column::new("`baz`", INTEGER.into_type()?);
            column.set_comment("Quoted 3");
            column
        });

        let comparator = schema_manager.create_comparator();
        let diff = comparator.diff_table(&from_table, &to_table).unwrap();
        assert!(diff.is_some());
        assert_eq!(
            schema_manager.get_alter_table_sql(&mut diff.unwrap())?,
            &["ALTER TABLE mytable \
            CHANGE quoted2 `and` INT NOT NULL COMMENT 'Quoted 2', \
            CHANGE `select` `bar` INT NOT NULL COMMENT 'Reserved keyword 3', \
            CHANGE quoted3 `baz` INT NOT NULL COMMENT 'Quoted 3', \
            CHANGE unquoted3 `foo` INT NOT NULL COMMENT 'Unquoted 3', \
            CHANGE `table` `from` INT NOT NULL COMMENT 'Reserved keyword 2', \
            CHANGE quoted1 quoted INT NOT NULL COMMENT 'Quoted 1', \
            CHANGE `create` reserved_keyword INT NOT NULL COMMENT 'Reserved keyword 1', \
            CHANGE unquoted1 unquoted INT NOT NULL COMMENT 'Unquoted 1', \
            CHANGE unquoted2 `where` INT NOT NULL COMMENT 'Unquoted 2'",]
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_alter_table_change_column_length() -> Result<()> {
        let mut from_table = Table::new("mytable");

        let mut column = Column::new("unquoted1", STRING.into_type()?);
        column.set_comment("Unquoted 1");
        column.set_length(10);
        from_table.add_column(column);

        let mut column = Column::new("unquoted2", STRING.into_type()?);
        column.set_comment("Unquoted 2");
        column.set_length(10);
        from_table.add_column(column);

        let mut column = Column::new("unquoted3", STRING.into_type()?);
        column.set_comment("Unquoted 3");
        column.set_length(10);
        from_table.add_column(column);

        let mut column = Column::new("create", STRING.into_type()?);
        column.set_comment("Reserved keyword 1");
        column.set_length(10);
        from_table.add_column(column);

        let mut column = Column::new("table", STRING.into_type()?);
        column.set_comment("Reserved keyword 2");
        column.set_length(10);
        from_table.add_column(column);

        let mut column = Column::new("select", STRING.into_type()?);
        column.set_comment("Reserved keyword 3");
        column.set_length(10);
        from_table.add_column(column);

        let mut to_table = Table::new("mytable");

        let mut column = Column::new("unquoted1", STRING.into_type()?);
        column.set_comment("Unquoted 1");
        column.set_length(255);
        to_table.add_column(column);

        let mut column = Column::new("unquoted2", STRING.into_type()?);
        column.set_comment("Unquoted 2");
        column.set_length(255);
        to_table.add_column(column);

        let mut column = Column::new("unquoted3", STRING.into_type()?);
        column.set_comment("Unquoted 3");
        column.set_length(255);
        to_table.add_column(column);

        let mut column = Column::new("create", STRING.into_type()?);
        column.set_comment("Reserved keyword 1");
        column.set_length(255);
        to_table.add_column(column);

        let mut column = Column::new("table", STRING.into_type()?);
        column.set_comment("Reserved keyword 2");
        column.set_length(255);
        to_table.add_column(column);

        let mut column = Column::new("select", STRING.into_type()?);
        column.set_comment("Reserved keyword 3");
        column.set_length(255);
        to_table.add_column(column);

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let comparator = schema_manager.create_comparator();
        let mut diff = comparator.diff_table(&from_table, &to_table).unwrap();

        assert!(diff.is_some());
        assert_eq!(
            schema_manager.get_alter_table_sql(diff.as_mut().unwrap())?,
            &["ALTER TABLE mytable \
            CHANGE unquoted1 unquoted1 VARCHAR(255) NOT NULL COMMENT 'Unquoted 1', \
            CHANGE unquoted2 unquoted2 VARCHAR(255) NOT NULL COMMENT 'Unquoted 2', \
            CHANGE unquoted3 unquoted3 VARCHAR(255) NOT NULL COMMENT 'Unquoted 3', \
            CHANGE `create` `create` VARCHAR(255) NOT NULL COMMENT 'Reserved keyword 1', \
            CHANGE `table` `table` VARCHAR(255) NOT NULL COMMENT 'Reserved keyword 2', \
            CHANGE `select` `select` VARCHAR(255) NOT NULL COMMENT 'Reserved keyword 3'",]
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn alter_table_rename_index_in_schema() -> Result<()> {
        let mut table = Table::new("myschema.mytable");
        table.add_column(Column::new("id", INTEGER.into_type()?));
        table.set_primary_key(&["id"], None)?;

        let mut table_diff = TableDiff::new("myschema.mytable", &table);
        table_diff.renamed_indexes.push((
            "idx_foo".to_string(),
            Index::new("idx_bar", &["id"], false, false, &[], HashMap::default()),
        ));

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let sql = schema_manager.get_alter_table_sql(&mut table_diff)?;

        let version = connection.server_version().await?;
        if compare_to(&version, "5.7", Cmp::Lt).unwrap()
            || (compare_to(&version, "10", Cmp::Ge).unwrap()
                && compare_to(&version, "10.5.2", Cmp::Lt).unwrap())
        {
            assert_eq!(
                sql,
                &[
                    "DROP INDEX idx_foo ON myschema.mytable",
                    "CREATE INDEX idx_bar ON myschema.mytable (id)"
                ]
            );
        } else {
            assert_eq!(
                sql,
                &["ALTER TABLE myschema.mytable RENAME INDEX idx_foo TO idx_bar"]
            );
        }

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_alter_table_rename_index_in_schema() -> Result<()> {
        let mut table = Table::new("`schema`.table");
        table.add_column(Column::new("id", INTEGER.into_type()?));
        table.set_primary_key(&["id"], None)?;

        let mut table_diff = TableDiff::new("`schema`.table", &table);
        table_diff.renamed_indexes.push((
            "create".to_string(),
            Index::new("select", &["id"], false, false, &[], HashMap::default()),
        ));
        table_diff.renamed_indexes.push((
            "`foo`".to_string(),
            Index::new("`bar`", &["id"], false, false, &[], HashMap::default()),
        ));

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let sql = schema_manager.get_alter_table_sql(&mut table_diff)?;

        let version = connection.server_version().await?;
        if compare_to(&version, "5.7", Cmp::Lt).unwrap()
            || (compare_to(&version, "10", Cmp::Ge).unwrap()
                && compare_to(&version, "10.5.2", Cmp::Lt).unwrap())
        {
            assert_eq!(
                sql,
                &[
                    "DROP INDEX `create` ON `schema`.`table`",
                    "CREATE INDEX `select` ON `schema`.`table` (id)",
                    "DROP INDEX `foo` ON `schema`.`table`",
                    "CREATE INDEX `bar` ON `schema`.`table` (id)",
                ]
            );
        } else {
            assert_eq!(
                sql,
                &[
                    "ALTER TABLE `schema`.`table` RENAME INDEX `create` TO `select`",
                    "ALTER TABLE `schema`.`table` RENAME INDEX `foo` TO `bar`",
                ]
            );
        }

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_drop_foreign_key_sql() -> Result<()> {
        let table = Table::new("table");
        let mut foreign_key =
            ForeignKeyConstraint::new(&["x"], &["y"], "foo", HashMap::default(), None, None);
        foreign_key.set_name("select");

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        assert_eq!(
            schema_manager.get_drop_foreign_key_sql(&foreign_key, &table)?,
            "ALTER TABLE `table` DROP FOREIGN KEY `select`"
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn get_comment_on_column_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        assert_eq!(
            &[
                schema_manager.get_comment_on_column_sql(&"foo", &"bar", "comment")?,
                schema_manager.get_comment_on_column_sql(&"`Foo`", &"`BAR`", "comment")?,
                schema_manager.get_comment_on_column_sql(&"select", &"from", "comment")?,
            ],
            &[
                "COMMENT ON COLUMN foo.bar IS 'comment'",
                "COMMENT ON COLUMN `Foo`.`BAR` IS 'comment'",
                "COMMENT ON COLUMN `select`.`from` IS 'comment'",
            ]
        );

        Ok(())
    }
}
