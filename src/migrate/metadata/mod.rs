mod executed_migration;

use crate::migrate::execution_result::ExecutionResult;
use crate::migrate::Direction;
use crate::r#type::{IntoType, BIGINT, DATETIME, INTEGER};
use crate::schema::{Column, Table};
use crate::{params, AsyncResult, Connection};
use chrono::TimeZone;
use creed::Value;
use creed_macros::value_map;
pub use executed_migration::{ExecutedMigration, ExecutedMigrationList};
use std::sync::atomic::{AtomicBool, Ordering};

pub trait MetadataStorage {
    fn get_executed_migration(&self) -> AsyncResult<ExecutedMigrationList>;
    fn complete(&self, execution_result: ExecutionResult) -> AsyncResult<()>;
}

pub struct TableMetadataStorage<'conn> {
    connection: &'conn Connection,
    is_initialized: AtomicBool,
    schema_up_to_date: AtomicBool,
    table_name: String,
    version_column_name: String,
    executed_at_column_name: String,
    execution_time_column_name: String,
}

impl<'conn> TableMetadataStorage<'conn> {
    pub fn new(connection: &'conn Connection) -> Self {
        Self {
            connection,
            is_initialized: AtomicBool::new(false),
            schema_up_to_date: AtomicBool::new(false),
            table_name: "migration_versions".to_string(),
            version_column_name: "version".to_string(),
            executed_at_column_name: "executed_at".to_string(),
            execution_time_column_name: "execution_time".to_string(),
        }
    }

    pub fn with_table_name(mut self, table_name: &str) -> Self {
        self.table_name = table_name.to_string();
        self
    }

    pub fn with_version_column_name(mut self, version_column_name: &str) -> Self {
        self.version_column_name = version_column_name.to_string();
        self
    }

    pub fn with_executed_at_column_name(mut self, executed_at_column_name: &str) -> Self {
        self.executed_at_column_name = executed_at_column_name.to_string();
        self
    }

    pub fn with_execution_time_column_name(mut self, execution_time_column_name: &str) -> Self {
        self.execution_time_column_name = execution_time_column_name.to_string();
        self
    }

    async fn is_initialized(&self, connection: &Connection) -> crate::Result<bool> {
        let schema_manager = connection.create_schema_manager()?;
        schema_manager.tables_exist(&[&self.table_name]).await
    }

    async fn ensure_initialized(&self, connection: &Connection) -> crate::Result<()> {
        let table = self.get_expected_table();
        let schema_manager = connection.create_schema_manager()?;

        if !self.is_initialized(connection).await? {
            schema_manager.create_table(&table).await?;
            self.is_initialized.store(true, Ordering::SeqCst);
            self.schema_up_to_date.store(true, Ordering::SeqCst);
        } else {
            self.is_initialized.store(true, Ordering::SeqCst);
            if !self.schema_up_to_date.load(Ordering::SeqCst) {
                let comparator = schema_manager.create_comparator();
                let online_table = schema_manager.introspect_table(&self.table_name).await?;
                let diff = comparator.diff_table(&online_table, &table)?;
                if let Some(table_diff) = diff {
                    schema_manager.alter_table(table_diff).await?;
                }

                self.schema_up_to_date.store(true, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    fn get_expected_table(&self) -> Table {
        let mut table = Table::new(&self.table_name);
        table.add_column(
            Column::builder(&self.version_column_name, BIGINT)
                .expect("unable to create version column")
                .set_notnull(true),
        );
        table.add_column(
            Column::builder(&self.executed_at_column_name, DATETIME)
                .expect("unable to create executed_at column")
                .set_notnull(false),
        );
        table.add_column(
            Column::builder(&self.execution_time_column_name, INTEGER)
                .expect("unable to create execution_time column")
                .set_notnull(false),
        );

        table
            .set_primary_key(&[&self.version_column_name], None)
            .expect("unable to set primary_key");

        table
    }
}

impl MetadataStorage for TableMetadataStorage<'_> {
    fn get_executed_migration(&self) -> AsyncResult<ExecutedMigrationList> {
        Box::pin(async move {
            if !self.is_initialized(self.connection).await? {
                Ok(ExecutedMigrationList { items: vec![] })
            } else {
                self.ensure_initialized(self.connection).await?;
                let platform = self.connection.get_platform()?;

                let rows = self
                    .connection
                    .fetch_all(format!("SELECT * FROM {}", self.table_name), params!())
                    .await?;
                let mut migrations = vec![];
                for row in rows {
                    let version = row.get(self.version_column_name.as_str())?.to_string();
                    let executed_at = DATETIME.into_type()?.convert_to_value(
                        row.get(self.executed_at_column_name.as_str())?,
                        &platform,
                    )?;
                    let execution_time = INTEGER.into_type()?.convert_to_value(
                        row.get(self.execution_time_column_name.as_str())?,
                        &platform,
                    )?;

                    migrations.push(ExecutedMigration {
                        version: version.parse()?,
                        executed_at: if let Value::DateTime(dt) = executed_at {
                            chrono::Utc.from_local_datetime(&dt.naive_local()).single()
                        } else {
                            None
                        },
                        execution_time: if let Value::Int(time) = execution_time {
                            Some(time as u64)
                        } else if let Value::UInt(time) = execution_time {
                            Some(time)
                        } else {
                            None
                        },
                    });
                }

                Ok(ExecutedMigrationList { items: migrations })
            }
        })
    }

    fn complete(&self, execution_result: ExecutionResult) -> AsyncResult<()> {
        let version_column_name = self.version_column_name.clone();
        let execution_time_column_name = self.execution_time_column_name.clone();
        let executed_at_column_name = self.executed_at_column_name.clone();

        Box::pin(async move {
            self.ensure_initialized(self.connection).await?;

            if execution_result.direction == Direction::Up {
                self.connection
                    .insert(
                        &self.table_name,
                        value_map! {
                            version_column_name.as_str() => BIGINT; execution_result.version,
                            execution_time_column_name.as_str() => INTEGER; execution_result.execution_time,
                            executed_at_column_name.as_str() => DATETIME; Value::DateTime(execution_result.executed_at.into()),
                        },
                    )
                    .await?;
            } else {
                self.connection
                    .delete(
                        &self.table_name,
                        value_map! {
                            version_column_name.as_str() => BIGINT; execution_result.version,
                        },
                    )
                    .await?;
            }

            Ok(())
        })
    }
}
