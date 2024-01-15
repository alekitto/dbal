use super::sqlite;
use crate::driver::statement_result::StatementResult;
use crate::platform::{default, CreateFlags};
use crate::schema::{
    extract_type_from_comment, remove_type_from_comment, Asset, Column, ColumnData, ColumnList,
    Comparator, FKConstraintList, ForeignKeyConstraint, GenericComparator, Identifier, Index,
    IndexList, IntoIdentifier, SchemaManager, Table, TableDiff, TableOptions,
};
use crate::{params, AsyncResult, Connection, Error, Parameters, Result, Row, Value};
use regex::Regex;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;

struct FkConstraintDetails {
    constraint_name: Option<String>,
    deferrable: bool,
    deferred: bool,
}

pub struct SQLiteSchemaManager<'a> {
    connection: &'a Connection,
}

impl<'a> SQLiteSchemaManager<'a> {
    pub fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }

    async fn add_details_to_table_foreign_key_columns(
        &self,
        table: &str,
        columns: &Vec<Row>,
    ) -> Result<Vec<Row>> {
        let foreign_key_details = self.get_foreign_key_details(table).await?;
        let foreign_key_count = foreign_key_details.len();

        let mut result = vec![];

        let mut local_columns_by_id = HashMap::new();
        let mut foreign_columns_by_id = HashMap::new();
        let mut table_by_id = HashMap::new();
        let mut foreign_key_ids = HashMap::new();

        for column in columns {
            let id = column.get("id").unwrap().to_string();
            let table = column.get("table").unwrap().clone();

            foreign_key_ids.insert(id.clone(), table.clone());
            match local_columns_by_id.entry(id.clone()) {
                Vacant(e) => {
                    e.insert(vec![column.get("from").unwrap().to_string()]);
                }
                Occupied(mut e) => {
                    e.get_mut().push(column.get("from").unwrap().to_string());
                }
            };

            match foreign_columns_by_id.entry(id.clone()) {
                Vacant(e) => {
                    e.insert(vec![column.get("to").unwrap().to_string()]);
                }
                Occupied(mut e) => {
                    e.get_mut().push(column.get("to").unwrap().to_string());
                }
            };

            table_by_id.insert(id, table);
        }

        for (id, foreign_table) in foreign_key_ids {
            let detail = foreign_key_details
                .get(foreign_key_count - id.parse::<usize>().unwrap() - 1)
                .unwrap();
            let constraint_name = detail.constraint_name.clone().unwrap_or_default();

            result.push(Row::new(
                vec![
                    "constraint_name".into(),
                    "table_name".into(),
                    "foreign_table".into(),
                    "foreign_columns".into(),
                    "local_columns".into(),
                    "deferrable".into(),
                    "deferred".into(),
                ],
                vec![
                    if constraint_name.is_empty() {
                        Value::NULL
                    } else {
                        Value::from(constraint_name)
                    },
                    Value::from(table.to_string()),
                    foreign_table.clone(),
                    foreign_columns_by_id.get(&id).unwrap().join(",").into(),
                    local_columns_by_id.get(&id).unwrap().join(",").into(),
                    detail.deferrable.into(),
                    detail.deferred.into(),
                ],
            ))
        }

        Ok(result)
    }

    async fn get_foreign_key_details(&self, table: &str) -> Result<Vec<FkConstraintDetails>> {
        let create_sql = self
            .get_connection()
            .query(
                r#"
    SELECT sql
    FROM (
        SELECT *
            FROM sqlite_master
        UNION ALL
        SELECT *
            FROM sqlite_temp_master
    )
    WHERE type = 'table'
AND name = ?
"#,
                params![0 => Value::from(table)],
            )
            .await?
            .fetch_one()
            .await?;

        if create_sql.is_none() {
            return Ok(vec![]);
        }

        let create_sql = create_sql.unwrap().get("sql").unwrap().to_string();
        let r = Regex::new(
            r"(?:CONSTRAINT\s+(\S+)\s+)?(?:FOREIGN\s+KEY[^)]+\)\s*)?REFERENCES\s+\S+\s*(?:\([^)]+\))?(?:[^,]*?(NOT\s+DEFERRABLE|DEFERRABLE)(?:\s+INITIALLY\s+(DEFERRED|IMMEDIATE))?)?",
        )?;

        let mut details = vec![];
        for captures in r.captures_iter(&create_sql) {
            let name = captures.get(1);
            let deferrable = captures.get(2);
            let deferred = captures.get(3);

            details.push(FkConstraintDetails {
                constraint_name: name.and_then(|n| {
                    if n.is_empty() {
                        None
                    } else {
                        Some(n.as_str().to_string())
                    }
                }),
                deferrable: deferrable.is_some_and(|m| m.as_str().to_lowercase() == "deferrable"),
                deferred: deferred.is_some_and(|m| m.as_str().to_lowercase() == "deferred"),
            })
        }

        Ok(details)
    }
}

fn parse_column_comment_from_sql(column: &str, quoted_column: &str, sql: &str) -> Option<String> {
    let pattern = format!(
        "[\\s(,](?:\\W{}\\W|\\W{}\\W)(?:\\([^)]*?\\)|[^,(])*?,?((?:[^\\S\\r\\n]*--[^\\n]*\\n?)+)",
        regex::escape(quoted_column),
        regex::escape(column)
    );
    let pattern = Regex::new(&pattern).unwrap();

    let m = pattern.captures(sql)?;
    let comment = Regex::new("^\\s*--")
        .unwrap()
        .replace(m.get(1).unwrap().as_str().trim_end(), "")
        .to_string();

    if comment.is_empty() {
        None
    } else {
        Some(comment)
    }
}

pub trait AbstractSQLiteSchemaManager: SchemaManager {}
impl AbstractSQLiteSchemaManager for SQLiteSchemaManager<'_> {}

impl<'a> SchemaManager for SQLiteSchemaManager<'a> {
    fn get_connection(&self) -> &'a Connection {
        self.connection
    }

    fn as_dyn(&self) -> &dyn SchemaManager {
        self
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

    fn get_create_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>> {
        sqlite::get_create_tables_sql(self, tables)
    }

    fn _get_create_table_sql(
        &self,
        name: &Identifier,
        columns: &[ColumnData],
        options: &TableOptions,
    ) -> Result<Vec<String>> {
        sqlite::_get_create_table_sql(self, name, columns, options)
    }

    fn get_create_primary_key_sql(&self, _: &Index, _: &dyn IntoIdentifier) -> Result<String> {
        Err(Error::platform_feature_unsupported(
            "Sqlite platform does not support alter primary key.",
        ))
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

    #[inline(always)]
    fn get_list_tables_sql(&self) -> Result<String> {
        sqlite::get_list_tables_sql()
    }

    #[inline(always)]
    fn get_list_table_columns_sql(&self, table: &str, _: &str) -> Result<String> {
        sqlite::get_list_table_columns_sql(self, table)
    }

    #[inline(always)]
    fn get_list_table_foreign_keys_sql(&self, table: &str, _: &str) -> Result<String> {
        sqlite::get_list_table_foreign_keys_sql(self, table)
    }

    fn get_list_table_constraints_sql(&self, table: &str) -> Result<String> {
        sqlite::get_list_table_constraints_sql(self, table)
    }

    fn get_inline_column_comment_sql(&self, comment: &str) -> Result<String> {
        sqlite::get_inline_column_comment_sql(comment)
    }

    #[inline(always)]
    fn get_alter_table_sql(&self, diff: &mut TableDiff) -> Result<Vec<String>>
    where
        Self: Sync,
    {
        sqlite::get_alter_table_sql(self, diff)
    }

    #[inline(always)]
    fn get_drop_tables_sql(&self, tables: &[Table]) -> Result<Vec<String>> {
        sqlite::get_drop_tables_sql(self, tables)
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

    #[inline(always)]
    fn get_list_views_sql(&self, _: &str) -> Result<String> {
        sqlite::get_list_views_sql()
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

    fn get_truncate_table_sql(&self, table_name: &dyn IntoIdentifier, _: bool) -> Result<String> {
        sqlite::get_truncate_table_sql(self, table_name)
    }

    fn get_portable_table_column_definition(&self, table_column: &Row) -> Result<Column> {
        sqlite::get_portable_table_column_definition(self.as_dyn(), table_column)
    }

    fn get_portable_table_column_list(
        &self,
        table: &str,
        database: &str,
        table_columns: Vec<Row>,
    ) -> AsyncResult<ColumnList> {
        let table = table.to_string();
        let database = database.to_string();

        Box::pin(async move {
            let mut list = default::get_portable_table_column_list(
                self.as_dyn(),
                &table,
                &database,
                table_columns.clone(),
            )?;

            let Some(create_sql) = self
                .get_connection()
                .query(
                    r#"
    SELECT sql
    FROM (
        SELECT *
            FROM sqlite_master
        UNION ALL
        SELECT *
            FROM sqlite_temp_master
    )
    WHERE type = 'table'
AND name = ?
"#,
                    params![0  => Value::from(table)],
                )
                .await?
                .fetch_one()
                .await?
            else {
                return Ok(ColumnList::default());
            };

            let create_sql = create_sql.get("sql").unwrap().to_string();
            if create_sql.contains("AUTOINCREMENT") {
                // find column with autoincrement
                let mut autoincrement_column = None;
                let mut autoincrement_count = 0;

                for table_column in table_columns {
                    if table_column.get("pk").unwrap().to_string() == "0" {
                        continue;
                    }

                    autoincrement_count += 1;
                    if autoincrement_column.is_some()
                        || table_column.get("type").unwrap().to_string().to_lowercase() != "integer"
                    {
                        continue;
                    }

                    autoincrement_column = Some(table_column.get("name").unwrap().to_string());
                }

                if autoincrement_count == 1 {
                    if let Some(autoincrement_column) = autoincrement_column {
                        for column in list.iter_mut() {
                            if autoincrement_column != column.get_name() {
                                continue;
                            }

                            column.set_autoincrement(true);
                        }
                    }
                }
            }

            let platform = self.get_platform()?;
            for column in list.iter_mut() {
                let column_name = column.get_name();
                let mut r#type = column.get_type();

                let comment = parse_column_comment_from_sql(
                    column_name.as_ref(),
                    &platform.quote_single_identifier(column_name.as_ref()),
                    &create_sql,
                );
                r#type = extract_type_from_comment(comment.clone(), r#type)?;

                let comment = remove_type_from_comment(comment, r#type);
                column.set_comment::<String, Option<String>>(comment);
            }

            Ok(list)
        })
    }

    fn get_portable_table_indexes_list(
        &self,
        table_indexes: Vec<Row>,
        table_name: &str,
    ) -> AsyncResult<IndexList> {
        let table_name = table_name.to_string();
        Box::pin(async move {
            sqlite::get_portable_table_indexes_list(self.as_dyn(), table_indexes, table_name).await
        })
    }

    fn list_table_indexes(&self, table: &str) -> AsyncResult<IndexList> {
        let table = self.normalize_name(table);

        Box::pin(async move {
            let rows = self
                .select_index_columns("'main'", Some(&table))
                .await?
                .fetch_all()
                .await?;
            self.get_portable_table_indexes_list(rows, &table).await
        })
    }

    fn create_comparator(&self) -> Box<dyn Comparator + Send + '_> {
        Box::new(GenericComparator::new(self))
    }

    fn list_table_foreign_keys(&self, table: &str) -> AsyncResult<FKConstraintList> {
        let table = table.to_string();
        Box::pin(async move {
            let columns = self
                .select_foreign_key_columns("", Some(table.as_str()))
                .await?
                .fetch_all()
                .await?;
            let columns = self
                .add_details_to_table_foreign_key_columns(&table, &columns)
                .await?;

            self.get_portable_table_foreign_keys_list(columns)
        })
    }

    fn select_foreign_key_columns(
        &self,
        _: &str,
        table_name: Option<&str>,
    ) -> AsyncResult<StatementResult> {
        let table_name = table_name.map(|t| t.to_string());
        Box::pin(async move { sqlite::select_foreign_key_columns(self.as_dyn(), table_name).await })
    }

    fn select_index_columns(
        &self,
        _database_name: &str,
        table_name: Option<&str>,
    ) -> AsyncResult<StatementResult> {
        let sql = r#"
SELECT t.name AS table_name,
i.*
    FROM sqlite_master t
JOIN pragma_index_list(t.name) i
        "#;

        let mut params = vec![];
        let mut conditions = vec![
            "t.type = 'table'",
            "t.name NOT IN ('geometry_columns', 'spatial_ref_sys', 'sqlite_sequence')",
        ];

        if let Some(table_name) = table_name {
            conditions.push("t.name = ?");
            params.push(table_name.replace('.', "__"));
        }

        let sql = format!(
            "{} WHERE {} ORDER BY t.name, i.seq",
            sql,
            conditions.join(" AND ")
        );
        Box::pin(self.connection.query(sql, Parameters::from(params)))
    }

    fn fetch_table_options_by_table(
        &self,
        _: &str,
        table_name: Option<&str>,
    ) -> AsyncResult<HashMap<String, Row>> {
        let table_name = table_name.map(|t| t.to_string());
        Box::pin(
            async move { sqlite::fetch_table_options_by_table(self.as_dyn(), table_name).await },
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::platform::CreateFlags;
    use crate::r#type::{IntoType, BOOLEAN, INTEGER, STRING};
    use crate::schema::{
        ChangedProperty, Column, ColumnDiff, Index, Table, TableDiff, UniqueConstraint,
    };
    use crate::tests::create_connection;
    use crate::Result;
    use std::collections::HashMap;

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

        table
            .set_primary_key(&["id"], None)
            .expect("Failed to set primary key");

        let sql = schema_manager
            .get_create_table_sql(&table, None)
            .expect("Failed to generate table SQL");
        assert_eq!(sql, vec![
            "CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, test VARCHAR(255) DEFAULT NULL)"
        ]);

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
    pub async fn test_generates_constraint_creation_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let idx = UniqueConstraint::new("constraint_name", &["test"], &[], HashMap::default());
        let sql = schema_manager.get_create_unique_constraint_sql(&idx, &"test")?;
        assert_eq!(
            sql,
            "ALTER TABLE test ADD CONSTRAINT constraint_name UNIQUE (test)"
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
        assert_eq!(sql, &[
            "CREATE TEMPORARY TABLE __temp__mytable AS SELECT id, bar, bloo FROM mytable",
            "DROP TABLE mytable",
            "CREATE TABLE mytable (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, baz VARCHAR(255) DEFAULT 'def' NOT NULL, bloo BOOLEAN DEFAULT 0 NOT NULL, quota INTEGER DEFAULT NULL)",
            "INSERT INTO mytable (id, baz, bloo) SELECT id, bar, bloo FROM __temp__mytable",
            "DROP TABLE __temp__mytable",
            "ALTER TABLE mytable RENAME TO userlist",
        ]);

        Ok(())
    }

    #[tokio::test]
    pub async fn quoted_column_in_primary_key_propagation() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let mut table = Table::new("`quoted`");
        let mut col = Column::new("create", STRING.into_type()?);
        col.set_length(255);
        table.add_column(col);
        table
            .set_primary_key(&["create"], None)
            .expect("failed to set primary key");

        let sql = schema_manager.get_create_table_sql(&table, None)?;
        assert_eq!(sql, &["CREATE TABLE \"quoted\" (\"create\" VARCHAR(255) NOT NULL, PRIMARY KEY(\"create\"))"]);

        Ok(())
    }

    #[tokio::test]
    pub async fn quoted_column_in_index_propagation() -> Result<()> {
        let mut table = Table::new("`quoted`");
        let mut col = Column::new("create", STRING.into_type()?);
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

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        let sql = schema_manager.get_create_table_sql(&table, None)?;
        assert_eq!(
            sql,
            &[
                "CREATE TABLE \"quoted\" (\"create\" VARCHAR(255) NOT NULL)",
                "CREATE INDEX IDX_22660D028FD6E0FB ON \"quoted\" (\"create\")",
            ]
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn test_quoted_name_in_index_sql() -> Result<()> {
        let mut table = Table::new("test");
        let mut col = Column::new("column1", STRING.into_type()?);
        col.set_length(255);
        table.add_column(col);
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
        assert_eq!(
            sql,
            &[
                "CREATE TABLE test (column1 VARCHAR(255) NOT NULL)",
                "CREATE INDEX \"key\" ON test (column1)",
            ]
        );

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
            "CREATE TABLE \"quoted\" (\
            \"create\" VARCHAR(255) NOT NULL, foo VARCHAR(255) NOT NULL, \"bar\" VARCHAR(255) NOT NULL, \
            CONSTRAINT FK_WITH_RESERVED_KEYWORD FOREIGN KEY (\"create\", foo, \"bar\") \
            REFERENCES \"foreign\" (\"create\", bar, \"foo-bar\") NOT DEFERRABLE INITIALLY IMMEDIATE, \
            CONSTRAINT FK_WITH_NON_RESERVED_KEYWORD FOREIGN KEY (\"create\", foo, \"bar\") \
            REFERENCES foo (\"create\", bar, \"foo-bar\") NOT DEFERRABLE INITIALLY IMMEDIATE, \
            CONSTRAINT FK_WITH_INTENDED_QUOTATION FOREIGN KEY (\"create\", foo, \"bar\") \
            REFERENCES \"foo-bar\" (\"create\", bar, \"foo-bar\") NOT DEFERRABLE INITIALLY IMMEDIATE)",
            "CREATE INDEX IDX_22660D028FD6E0FB8C73652176FF8CAA ON \"quoted\" (\"create\", foo, \"bar\")",
        ]);

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_reserved_keyword_in_unique_constraint_declaration_sql() -> Result<()> {
        let constraint = UniqueConstraint::new("select", &["foo"], &[], HashMap::default());

        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let sql = schema_manager.get_unique_constraint_declaration_sql("select", &constraint)?;
        assert_eq!(sql, r#"CONSTRAINT "select" UNIQUE (foo)"#);

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_reserved_keyword_in_truncate_table_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;

        assert_eq!(
            schema_manager
                .get_truncate_table_sql(&"select", false)
                .unwrap(),
            r#"DELETE FROM "select""#
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn quotes_reserved_keyword_in_index_declaration_sql() -> Result<()> {
        let connection = create_connection().await?;
        let schema_manager = connection.create_schema_manager()?;
        let index = Index::new("select", &["foo"], false, false, &[], HashMap::default());

        assert_eq!(
            schema_manager
                .get_index_declaration_sql("select", &index)
                .unwrap(),
            r#"INDEX "select" (foo)"#
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
            Index::new("idx_bar", &["id"], false, false, &[], HashMap::default()),
        ));

        assert_eq!(
            schema_manager.get_alter_table_sql(&mut table_diff)?,
            &[
                "CREATE TEMPORARY TABLE __temp__mytable AS SELECT id FROM mytable",
                "DROP TABLE mytable",
                "CREATE TABLE mytable (id INTEGER NOT NULL, PRIMARY KEY(id))",
                "INSERT INTO mytable (id) SELECT id FROM __temp__mytable",
                "DROP TABLE __temp__mytable",
                "CREATE INDEX idx_bar ON mytable (id)",
            ]
        );

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

        assert_eq!(
            schema_manager.get_alter_table_sql(&mut table_diff)?,
            &[
                r#"CREATE TEMPORARY TABLE __temp__table AS SELECT id FROM "table""#,
                r#"DROP TABLE "table""#,
                r#"CREATE TABLE "table" (id INTEGER NOT NULL, PRIMARY KEY(id))"#,
                r#"INSERT INTO "table" (id) SELECT id FROM __temp__table"#,
                r#"DROP TABLE __temp__table"#,
                r#"CREATE INDEX "bar" ON "table" (id)"#,
                r#"CREATE INDEX "select" ON "table" (id)"#,
            ]
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn test_quotes_alter_table_rename_column() -> Result<()> {
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
        let diff = comparator.diff_table(&from_table, &to_table)?;
        assert!(diff.is_some());
        assert_eq!(
            schema_manager.get_alter_table_sql(&mut diff.unwrap())?,
            &[
                r#"CREATE TEMPORARY TABLE __temp__mytable AS SELECT unquoted1, unquoted2, unquoted3, "create", "table", "select", "quoted1", "quoted2", "quoted3" FROM mytable"#,
                r#"DROP TABLE mytable"#,
                r#"CREATE TABLE mytable (unquoted INTEGER NOT NULL, "where" INTEGER NOT NULL, "foo" INTEGER NOT NULL, reserved_keyword INTEGER NOT NULL, "from" INTEGER NOT NULL, "bar" INTEGER NOT NULL, quoted INTEGER NOT NULL, "and" INTEGER NOT NULL, "baz" INTEGER NOT NULL)"#,
                r#"INSERT INTO mytable (unquoted, "where", "foo", reserved_keyword, "from", "bar", quoted, "and", "baz") SELECT unquoted1, unquoted2, unquoted3, "create", "table", "select", "quoted1", "quoted2", "quoted3" FROM __temp__mytable"#,
                r#"DROP TABLE __temp__mytable"#,
            ]
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
        let mut diff = comparator.diff_table(&from_table, &to_table)?;

        assert!(diff.is_some());
        assert_eq!(
            schema_manager.get_alter_table_sql(diff.as_mut().unwrap())?,
            &[
                r#"CREATE TEMPORARY TABLE __temp__mytable AS SELECT unquoted1, unquoted2, unquoted3, "create", "table", "select" FROM mytable"#,
                "DROP TABLE mytable",
                r#"CREATE TABLE mytable (unquoted1 VARCHAR(255) NOT NULL, unquoted2 VARCHAR(255) NOT NULL, unquoted3 VARCHAR(255) NOT NULL, "create" VARCHAR(255) NOT NULL, "table" VARCHAR(255) NOT NULL, "select" VARCHAR(255) NOT NULL)"#,
                r#"INSERT INTO mytable (unquoted1, unquoted2, unquoted3, "create", "table", "select") SELECT unquoted1, unquoted2, unquoted3, "create", "table", "select" FROM __temp__mytable"#,
                r#"DROP TABLE __temp__mytable"#,
            ]
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
                r#"COMMENT ON COLUMN foo.bar IS 'comment'"#,
                r#"COMMENT ON COLUMN "Foo"."BAR" IS 'comment'"#,
                r#"COMMENT ON COLUMN "select"."from" IS 'comment'"#,
            ]
        );

        Ok(())
    }
}
