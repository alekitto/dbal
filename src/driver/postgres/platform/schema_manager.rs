use super::postgresql;
use crate::schema::{
    Column, ColumnData, Comparator, ForeignKeyConstraint, GenericComparator, Identifier, Index,
    IntoIdentifier, SchemaManager, Sequence, TableDiff, TableOptions,
};
use crate::{Connection, Result, Row};

pub struct PostgreSQLSchemaManager<'a> {
    connection: &'a Connection,
}

impl<'a> PostgreSQLSchemaManager<'a> {
    pub fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }
}

pub trait AbstractPostgreSQLSchemaManager: SchemaManager {}

impl AbstractPostgreSQLSchemaManager for PostgreSQLSchemaManager<'_> {}
impl<'a> SchemaManager for PostgreSQLSchemaManager<'a> {
    fn get_connection(&self) -> &'a Connection {
        self.connection
    }

    fn as_dyn(&self) -> &dyn SchemaManager {
        self
    }

    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>> {
        postgresql::_get_create_table_sql(self.as_dyn(), name, columns, options)
    }

    #[inline]
    fn get_create_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        postgresql::get_create_sequence_sql(self.get_platform()?.as_dyn(), sequence)
    }

    #[inline]
    fn get_list_databases_sql(&self) -> Result<String> {
        postgresql::get_list_databases_sql()
    }

    #[inline]
    fn get_list_tables_sql(&self) -> Result<String> {
        postgresql::get_list_tables_sql()
    }

    #[inline]
    fn get_list_sequences_sql(&self, database: &str) -> Result<String> {
        postgresql::get_list_sequences_sql(self.as_dyn(), database)
    }

    #[inline]
    fn get_list_table_columns_sql(&self, table: &str, _: Option<&str>) -> Result<String> {
        postgresql::get_list_table_columns_sql(self.as_dyn(), table)
    }

    #[inline]
    fn get_list_table_indexes_sql(&self, table: &str, _: Option<&str>) -> Result<String> {
        postgresql::get_list_table_indexes_sql(self.as_dyn(), table)
    }

    #[inline]
    fn get_list_table_foreign_keys_sql(&self, table: &str) -> Result<String> {
        postgresql::get_list_table_foreign_keys_sql(self.as_dyn(), table)
    }

    #[inline]
    fn get_list_table_constraints_sql(&self, table: &str) -> Result<String> {
        postgresql::get_list_table_constraints_sql(self.as_dyn(), table)
    }

    #[inline]
    fn get_comment_on_column_sql(
        &self,
        table_name: &Identifier,
        column: &Column,
        comment: &str,
    ) -> Result<String> {
        postgresql::get_comment_on_column_sql(
            self.get_platform()?.as_dyn(),
            table_name,
            column,
            comment,
        )
    }

    #[inline]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        postgresql::get_alter_table_sql(self.as_dyn(), diff)
    }

    #[inline]
    fn get_alter_sequence_sql(&self, sequence: &Sequence) -> Result<String> {
        postgresql::get_alter_sequence_sql(self.get_platform()?.as_dyn(), sequence)
    }

    fn get_truncate_table_sql(
        &self,
        table_name: &dyn IntoIdentifier,
        cascade: bool,
    ) -> Result<String> {
        postgresql::get_truncate_table_sql(self, table_name, cascade)
    }

    fn get_drop_foreign_key_sql(
        &self,
        foreign_key: &dyn IntoIdentifier,
        table_name: &dyn IntoIdentifier,
    ) -> Result<String> {
        postgresql::get_drop_foreign_key_sql(self.as_dyn(), foreign_key, table_name)
    }

    #[inline]
    fn get_drop_sequence_sql(&self, sequence: &dyn IntoIdentifier) -> Result<String> {
        postgresql::get_drop_sequence_sql(self.get_platform()?.as_dyn(), sequence)
    }

    #[inline]
    fn get_list_views_sql(&self, _: &str) -> Result<String> {
        postgresql::get_list_views_sql()
    }

    #[inline]
    fn get_sequence_next_val_sql(&self, sequence: &str) -> Result<String> {
        postgresql::get_sequence_next_val_sql(sequence)
    }

    fn get_rename_index_sql(
        &self,
        old_index_name: &Identifier,
        index: &Index,
        table_name: &Identifier,
    ) -> Result<Vec<String>> {
        postgresql::get_rename_index_sql(self.as_dyn(), old_index_name, index, table_name)
    }

    #[inline]
    fn get_column_collation_declaration_sql(&self, collation: &str) -> Result<String> {
        postgresql::get_column_collation_declaration_sql(self.get_platform()?.as_dyn(), collation)
    }

    #[inline]
    fn get_advanced_foreign_key_options_sql(
        &self,
        foreign_key: &ForeignKeyConstraint,
    ) -> Result<String> {
        postgresql::get_advanced_foreign_key_options_sql(self.as_dyn(), foreign_key)
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
    use crate::platform::CreateFlags;
    use crate::r#type::{BOOLEAN, INTEGER, SIMPLE_ARRAY, STRING};
    use crate::schema::{
        ChangedProperty, Column, ColumnDiff, ForeignKeyConstraint, Index, Table, TableDiff,
        UniqueConstraint,
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
            "CREATE TABLE test (id SERIAL NOT NULL, test VARCHAR(255) DEFAULT NULL, PRIMARY KEY(id))"
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
        assert_eq!(sql, "ALTER TABLE test ADD FOREIGN KEY (fk_name_id) REFERENCES other_table (id) NOT DEFERRABLE INITIALLY IMMEDIATE");
    }

    #[tokio::test]
    pub async fn generates_constraint_creation_sql() {
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
        assert_eq!(sql, "ALTER TABLE test ADD FOREIGN KEY (fk_name) REFERENCES \"foreign\" (id) NOT DEFERRABLE INITIALLY IMMEDIATE");
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
        assert_eq!(
            sql,
            &[
                "ALTER TABLE mytable ADD quota INT DEFAULT NULL",
                "ALTER TABLE mytable DROP foo",
                "ALTER TABLE mytable ALTER bar TYPE VARCHAR(255)",
                "ALTER TABLE mytable ALTER bar SET DEFAULT 'def'",
                "ALTER TABLE mytable ALTER bar SET NOT NULL",
                "ALTER TABLE mytable ALTER bloo TYPE BOOLEAN",
                "ALTER TABLE mytable ALTER bloo SET DEFAULT false",
                "ALTER TABLE mytable ALTER bloo SET NOT NULL",
                "ALTER TABLE mytable RENAME TO userlist",
            ]
        );
    }

    #[tokio::test]
    pub async fn create_table_column_comments() {
        let mut table = Table::new("test");
        let mut id_col = Column::new("id", INTEGER).unwrap();
        id_col.set_comment("This is a comment");
        table.add_column(id_col);

        table
            .set_primary_key(&["id"], None)
            .expect("failed to set primary key");

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let sql = schema_manager.get_create_table_sql(&table, None).unwrap();
        assert_eq!(
            sql,
            &[
                "CREATE TABLE test (id INT NOT NULL, PRIMARY KEY(id))",
                "COMMENT ON COLUMN test.id IS 'This is a comment'",
            ]
        );
    }

    #[tokio::test]
    pub async fn alter_table_column_comments() {
        let mut table_diff = TableDiff::new("mytable", None);
        let mut col = Column::new("quota", INTEGER).unwrap();
        col.set_comment("A comment");
        table_diff.added_columns.push(col);

        table_diff.changed_columns.push(ColumnDiff::new(
            "foo",
            &Column::new("foo", STRING).unwrap(),
            &[ChangedProperty::Comment],
            None,
        ));
        let mut col = Column::new("baz", STRING).unwrap();
        col.set_comment("B comment");
        table_diff.changed_columns.push(ColumnDiff::new(
            "bar",
            &col,
            &[ChangedProperty::Comment],
            None,
        ));

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let sql = schema_manager.get_alter_table_sql(&mut table_diff).unwrap();
        assert_eq!(
            sql,
            &[
                "ALTER TABLE mytable ADD quota INT NOT NULL",
                "COMMENT ON COLUMN mytable.quota IS 'A comment'",
                "COMMENT ON COLUMN mytable.foo IS NULL",
                "COMMENT ON COLUMN mytable.baz IS 'B comment'",
            ]
        );
    }

    #[tokio::test]
    pub async fn create_table_column_type_comments() {
        let mut table = Table::new("test");
        table.add_column(Column::new("id", INTEGER).unwrap());
        table.add_column(Column::new("data", SIMPLE_ARRAY).unwrap());
        table.set_primary_key(&["id"], None).unwrap();

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let sql = schema_manager.get_create_table_sql(&table, None).unwrap();

        assert_eq!(
            sql,
            &[
                "CREATE TABLE test (id INT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(id))",
                "COMMENT ON COLUMN test.data IS '(CRType:simple_array)'",
            ]
        );
    }

    #[tokio::test]
    pub async fn quoted_column_in_primary_key_propagation() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let mut table = Table::new("`quoted`");
        table.add_column(Column::new("create", STRING).unwrap());
        table
            .set_primary_key(&["create"], None)
            .expect("failed to set primary key");

        let sql = schema_manager.get_create_table_sql(&table, None).unwrap();
        assert_eq!(sql, &["CREATE TABLE \"quoted\" (\"create\" VARCHAR(255) NOT NULL, PRIMARY KEY(\"create\"))"]);
    }

    #[tokio::test]
    pub async fn quoted_column_in_index_propagation() {
        let mut table = Table::new("`quoted`");
        table.add_column(Column::new("create", STRING).unwrap());
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

    #[tokio::test]
    pub async fn test_quoted_name_in_index_sql() {
        let mut table = Table::new("test");
        table.add_column(Column::new("column1", STRING).unwrap());
        table.add_index(Index::new::<_, _, &str>(
            "`key`",
            &["column1"],
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
                "CREATE TABLE test (column1 VARCHAR(255) NOT NULL)",
                "CREATE INDEX \"key\" ON test (column1)",
            ]
        );
    }

    #[tokio::test]
    pub async fn quoted_column_in_foreign_key_propagation() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let mut table = Table::new("`quoted`");
        table.add_column(Column::new("create", STRING).unwrap());
        table.add_column(Column::new("foo", STRING).unwrap());
        table.add_column(Column::new("`bar`", STRING).unwrap());

        // Foreign table with reserved keyword as name (needs quotation).
        let mut foreign_table = Table::new("foreign");

        // Foreign column with reserved keyword as name (needs quotation).
        foreign_table.add_column(Column::new("create", STRING).unwrap());

        // Foreign column with non-reserved keyword as name (does not need quotation).
        foreign_table.add_column(Column::new("bar", STRING).unwrap());

        // Foreign table with special character in name
        foreign_table.add_column(Column::new("`foo-bar`", STRING).unwrap());

        table
            .add_foreign_key_constraint(
                &["create", "foo", "`bar`"],
                &["create", "bar", "`foo-bar`"],
                &foreign_table,
                HashMap::default(),
                None,
                None,
                Some("FK_WITH_RESERVED_KEYWORD"),
            )
            .expect("cannot add foreign key constraint");

        // Foreign table with non-reserved keyword as name (does not need quotation).
        let mut foreign_table = Table::new("foo");

        // Foreign column with reserved keyword as name (needs quotation).
        foreign_table.add_column(Column::new("create", STRING).unwrap());

        // Foreign column with non-reserved keyword as name (does not need quotation).
        foreign_table.add_column(Column::new("bar", STRING).unwrap());

        // Foreign table with special character in name
        foreign_table.add_column(Column::new("`foo-bar`", STRING).unwrap());

        table
            .add_foreign_key_constraint(
                &["create", "foo", "`bar`"],
                &["create", "bar", "`foo-bar`"],
                &foreign_table,
                HashMap::default(),
                None,
                None,
                Some("FK_WITH_NON_RESERVED_KEYWORD"),
            )
            .expect("cannot add foreign key constraint");

        // Foreign table with special character in name.
        let mut foreign_table = Table::new("`foo-bar`");

        // Foreign column with reserved keyword as name (needs quotation).
        foreign_table.add_column(Column::new("create", STRING).unwrap());

        // Foreign column with non-reserved keyword as name (does not need quotation).
        foreign_table.add_column(Column::new("bar", STRING).unwrap());

        // Foreign table with special character in name
        foreign_table.add_column(Column::new("`foo-bar`", STRING).unwrap());

        table
            .add_foreign_key_constraint(
                &["create", "foo", "`bar`"],
                &["create", "bar", "`foo-bar`"],
                &foreign_table,
                HashMap::default(),
                None,
                None,
                Some("FK_WITH_INTENDED_QUOTATION"),
            )
            .expect("cannot add foreign key constraint");

        let sql = schema_manager
            .get_create_table_sql(
                &table,
                Some(CreateFlags::CREATE_INDEXES | CreateFlags::CREATE_FOREIGN_KEYS),
            )
            .unwrap();
        assert_eq!(
            sql,
            &[
                r#"CREATE TABLE "quoted" ("create" VARCHAR(255) NOT NULL, foo VARCHAR(255) NOT NULL, "bar" VARCHAR(255) NOT NULL)"#,
                r#"CREATE INDEX IDX_22660D028FD6E0FB8C736521D79164E3 ON "quoted" ("create", foo, "bar")"#,
                r#"ALTER TABLE "quoted" ADD CONSTRAINT FK_WITH_RESERVED_KEYWORD FOREIGN KEY ("create", foo, "bar") REFERENCES "foreign" ("create", bar, "foo-bar") NOT DEFERRABLE INITIALLY IMMEDIATE"#,
                r#"ALTER TABLE "quoted" ADD CONSTRAINT FK_WITH_NON_RESERVED_KEYWORD FOREIGN KEY ("create", foo, "bar") REFERENCES foo ("create", bar, "foo-bar") NOT DEFERRABLE INITIALLY IMMEDIATE"#,
                r#"ALTER TABLE "quoted" ADD CONSTRAINT FK_WITH_INTENDED_QUOTATION FOREIGN KEY ("create", foo, "bar") REFERENCES "foo-bar" ("create", bar, "foo-bar") NOT DEFERRABLE INITIALLY IMMEDIATE"#,
            ]
        );
    }

    #[tokio::test]
    pub async fn quotes_reserved_keyword_in_unique_constraint_declaration_sql() {
        let constraint = UniqueConstraint::new("select", &["foo"], &[], HashMap::default());

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let sql = schema_manager
            .get_unique_constraint_declaration_sql("select", &constraint)
            .unwrap();
        assert_eq!(sql, r#"CONSTRAINT "select" UNIQUE (foo)"#);
    }

    #[tokio::test]
    pub async fn quotes_reserved_keyword_in_truncate_table_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        assert_eq!(
            schema_manager
                .get_truncate_table_sql(&"select", false)
                .unwrap(),
            r#"TRUNCATE "select""#
        );
    }

    #[tokio::test]
    pub async fn quotes_reserved_keyword_in_index_declaration_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let index = Index::new("select", &["foo"], false, false, &[], HashMap::default());

        assert_eq!(
            schema_manager
                .get_index_declaration_sql(&"select", &index)
                .unwrap(),
            r#"INDEX "select" (foo)"#
        );
    }

    #[tokio::test]
    pub async fn get_create_schema_sql() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        assert_eq!(
            schema_manager.get_create_schema_sql(&"schema").unwrap(),
            "CREATE SCHEMA schema"
        );
    }

    #[tokio::test]
    pub async fn alter_table_change_quoted_column() {
        let mut table = Table::new("mytable");
        table.add_column(Column::new("select", INTEGER).unwrap());

        let mut table_diff = TableDiff::new("mytable", &table);
        table_diff.changed_columns.push(ColumnDiff::new(
            "select",
            &Column::new("select", STRING).unwrap(),
            &[ChangedProperty::Type],
            None,
        ));

        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();
        let platform = schema_manager.get_platform().unwrap();
        let sql = schema_manager.get_alter_table_sql(&mut table_diff).unwrap();
        assert_eq!(
            sql.join(";").contains(&platform.quote_identifier("select")),
            true
        );
    }

    #[tokio::test]
    pub async fn alter_table_rename_index() {
        let connection = create_connection().await.unwrap();
        let schema_manager = connection.create_schema_manager().unwrap();

        let mut table = Table::new("mytable");
        table.add_column(Column::new("id", INTEGER).unwrap());
        table.set_primary_key(&["id"], None).unwrap();

        let mut table_diff = TableDiff::new("mytable", &table);
        table_diff.renamed_indexes.push((
            "idx_foo".to_string(),
            Index::new("idx_bar", &["id"], false, false, &[], HashMap::default()),
        ));

        assert_eq!(
            schema_manager.get_alter_table_sql(&mut table_diff).unwrap(),
            &["ALTER INDEX idx_foo RENAME TO idx_bar"]
        );
    }
}
