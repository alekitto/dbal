use crate::error::ErrorKind;
use crate::migrate::Direction;
use crate::migrate::execution_result::ExecutionResult;
use crate::migrate::migration_plan::MigrationPlan;
use crate::parameter::NO_PARAMS;
use crate::schema::{Schema, SchemaManager};
use crate::util::PlatformBox;
use crate::{Connection, Result};
use log::info;
use std::fmt::Display;

pub struct Executor<'conn> {
    connection: &'conn Connection,
    sql: Vec<String>,
}

impl<'conn> Executor<'conn> {
    pub fn new(connection: &'conn Connection) -> Self {
        Self {
            connection,
            sql: vec![],
        }
    }

    pub fn get_platform(&self) -> Result<PlatformBox> {
        self.connection.get_platform()
    }

    pub fn add_sql(&mut self, sql: impl Display) {
        self.sql.push(sql.to_string());
    }

    pub(super) async fn execute(
        &mut self,
        migration: &mut MigrationPlan,
        from_schema: Option<Schema>,
    ) -> Result<usize> {
        let schema_manager = self.connection.create_schema_manager()?;
        let comparator = schema_manager.create_comparator();

        let description = migration.migration.description;
        info!(target: "creed::migrate", "++ {} {} ({})", if migration.direction == Direction::Up {
            "migrating"
        } else {
            "reverting"
        }, migration.version, description());

        let from_schema = if let Some(from_schema) = from_schema {
            from_schema
        } else {
            schema_manager.introspect_schema().await?
        };

        let mut skipped = false;
        let mut error = None;

        let to_schema = if migration.direction == Direction::Up {
            let to_schema = if let Some(pre_up) = migration.migration.pre_up {
                pre_up(&from_schema)?
            } else {
                from_schema.clone()
            };

            let diff = comparator.compare_schemas(&from_schema, &to_schema)?;
            self.sql.append(&mut diff.to_sql(&schema_manager)?);

            let func = migration.migration.up;
            match func(self, &to_schema) {
                Ok(_) => (),
                Err(crate::Error {
                    kind: ErrorKind::SkipMigration,
                    ..
                }) => {
                    skipped = true;
                }
                Err(e) => {
                    let _ = error.insert(e);
                }
            };

            to_schema
        } else {
            let to_schema = if let Some(pre_down) = migration.migration.pre_down {
                pre_down(&from_schema)?
            } else {
                from_schema.clone()
            };

            let diff = comparator.compare_schemas(&from_schema, &to_schema)?;
            self.sql.append(&mut diff.to_sql(&schema_manager)?);

            let func = migration.migration.down;
            match func(self, &to_schema) {
                Ok(_) => (),
                Err(crate::Error {
                    kind: ErrorKind::SkipMigration,
                    ..
                }) => {
                    skipped = true;
                }
                Err(e) => {
                    let _ = error.insert(e);
                }
            };

            to_schema
        };

        let start = chrono::Utc::now();
        if error.is_none() && !self.sql.is_empty() {
            for q in self.sql.iter() {
                if let Err(e) = self
                    .connection
                    .execute_statement(q.as_str(), NO_PARAMS)
                    .await
                {
                    let _ = error.insert(e);
                    break;
                }
            }
        }

        if error.is_none() {
            let post_result = if migration.direction == Direction::Up {
                if let Some(post_up) = migration.migration.post_up {
                    post_up(&to_schema)
                } else {
                    Ok(())
                }
            } else if let Some(post_down) = migration.migration.post_down {
                post_down(&to_schema)
            } else {
                Ok(())
            };

            if let Err(e) = post_result {
                let _ = error.insert(e);
            }
        }

        let diff = chrono::Utc::now() - start;

        let sql_count = self.sql.len();
        let _ = migration.execution_result.insert(ExecutionResult {
            sql: self.sql.drain(..).collect(),
            version: migration.version,
            direction: migration.direction,
            executed_at: chrono::Utc::now(),
            execution_time: diff.num_milliseconds(),
            skipped,
            error,
            to_schema: Some(to_schema),
        });

        Ok(sql_count)
    }
}
