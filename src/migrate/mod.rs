mod execution_result;
mod executor;
pub mod metadata;
mod migration_plan;

pub use crate::migrate::executor::Executor;
use crate::migrate::metadata::{ExecutedMigrationList, MetadataStorage, TableMetadataStorage};
use crate::schema::Schema;
use crate::sync::Mutex;
use crate::{Connection, Result};
use log::{error, info};
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Direction {
    Up,
    Down,
}

impl Display for Direction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Up => "up",
                Self::Down => "down",
            }
        )
    }
}

pub type OpClosure = dyn (Fn(&mut Executor, &Schema) -> Result<()>) + Send + Sync;
pub type PreOpClosure = dyn (Fn(&Schema) -> Result<Schema>) + Send + Sync;
pub type PostOpClosure = dyn (Fn(&Schema) -> Result<()>) + Send + Sync;

#[derive(Clone)]
pub struct Migration {
    pub version: i64,
    pub description: &'static (dyn (Fn() -> &'static str) + Send + Sync),
    pub up: &'static OpClosure,
    pub down: &'static OpClosure,
    pub pre_up: Option<&'static PreOpClosure>,
    pub post_up: Option<&'static PostOpClosure>,
    pub pre_down: Option<&'static PreOpClosure>,
    pub post_down: Option<&'static PostOpClosure>,
    pub checksum: Cow<'static, [u8]>,
}

#[allow(dead_code)]
pub struct Migrator {
    migrations: Cow<'static, [Migration]>,
    ignore_missing: bool,
    locking: bool,
    metadata_storage: Mutex<Option<Box<dyn MetadataStorage>>>,
}

impl Migrator {
    pub const fn new(
        migrations: Cow<'static, [Migration]>,
        ignore_missing: bool,
        locking: bool,
    ) -> Self {
        Self {
            migrations,
            ignore_missing,
            locking,
            metadata_storage: Mutex::const_new(None),
        }
    }

    pub async fn with_metadata_storage<M: MetadataStorage + 'static>(
        self,
        metadata_storage: Box<M>,
    ) -> Self {
        {
            let mut guard = self.metadata_storage.lock().await;
            let _ = guard.insert(metadata_storage);
        }

        self
    }

    pub async fn migrate(&self, connection: &Connection) -> Result<()> {
        let Some(last_migration) = self.migrations.last() else {
            return Ok(());
        };

        let (plans, direction) = self
            .get_plan_until_version(last_migration.version, connection)
            .await?;

        info!(target: "creed::migrate", "Migrating {} to {}", direction, last_migration.version);

        if plans.is_empty() {
            info!(target: "creed::migrate", "No migration to execute.");
            return Ok(());
        }

        let mut executor = executor::Executor::new(connection);
        let plans_count = plans.len();
        let mut sql_count: usize = 0;

        let global_start = chrono::Utc::now();
        let guard = self.metadata_storage.lock().await;
        connection.begin_transaction().await?;

        // todo: dispatch event
        let mut to_schema = None;
        for mut plan in plans {
            sql_count += executor.execute(&mut plan, to_schema).await?;

            if let Some(execution_result) = plan.execution_result {
                if let Some(error) = execution_result.error {
                    error!(target: "creed::migrate", "Error while executing migration {}: {}", plan.version, error);
                    connection.roll_back().await?;

                    return Err(error);
                }

                to_schema = execution_result.to_schema.clone();
                if let Some(metadata_storage) = guard.deref() {
                    metadata_storage.complete(execution_result).await
                } else {
                    let storage = TableMetadataStorage::new(connection);
                    storage.complete(execution_result).await
                }?;
            } else {
                to_schema = None;
            }
        }

        connection.commit().await?;
        let total_time = chrono::Utc::now() - global_start;

        info!(target: "creed::migrate", "Migrated database in {}ms, {} migrations executed, {} sql queries", total_time.num_milliseconds(), plans_count, sql_count);

        Ok(())
    }

    async fn get_executed_migrations(
        &self,
        connection: &Connection,
    ) -> Result<metadata::ExecutedMigrationList> {
        let guard = self.metadata_storage.lock().await;
        let list = if let Some(metadata_storage) = guard.deref() {
            metadata_storage.get_executed_migration().await
        } else {
            let storage = TableMetadataStorage::new(connection);
            storage.get_executed_migration().await
        }?;

        Ok(list)
    }

    async fn get_plan_until_version(
        &self,
        version: i64,
        connection: &Connection,
    ) -> Result<(Vec<migration_plan::MigrationPlan>, Direction)> {
        let executed_migrations = self.get_executed_migrations(connection).await?;
        let direction = self.find_direction(version, &executed_migrations);

        let mut sorted_migrations = self.migrations.to_vec();
        sorted_migrations.sort_by_key(|m| m.version);
        if direction == Direction::Down {
            sorted_migrations.reverse()
        }

        Ok((
            sorted_migrations
                .iter()
                .filter_map(|m| {
                    let has_migration = executed_migrations.has_migration(m.version);
                    if (direction == Direction::Up && !has_migration)
                        || (direction == Direction::Down && has_migration)
                    {
                        Some(migration_plan::MigrationPlan::new(m.clone(), direction))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
            direction,
        ))
    }

    fn find_direction(
        &self,
        version: i64,
        executed_migration_list: &ExecutedMigrationList,
    ) -> Direction {
        if version == 0 {
            Direction::Down
        } else {
            for migration in self.migrations.iter() {
                if migration.version == version {
                    break;
                }

                if !executed_migration_list.has_migration(migration.version) {
                    return Direction::Up;
                }
            }

            if executed_migration_list.has_migration(version)
                && !executed_migration_list
                    .last()
                    .is_some_and(|v| v.version == version)
            {
                Direction::Down
            } else {
                Direction::Up
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Connection, ConnectionOptions};
    use creed_macros::migrator;
    use serial_test::serial;

    migrator!(MIGRATOR, "tests/migrations");

    #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
    #[tokio::test]
    #[serial]
    async fn can_migrate() {
        let options =
            ConnectionOptions::try_from(std::env::var("DATABASE_DSN").unwrap().as_ref()).unwrap();
        let connection = Connection::create(options, None, None)
            .connect()
            .await
            .expect("unable to connect");

        let result = MIGRATOR.migrate(&connection).await;
        assert!(result.is_ok(), "error: {:#?}", result.unwrap_err());
    }
}
