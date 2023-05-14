use crate::r#type::{INTEGER, STRING};
use crate::schema::{Asset, Column, IntoIdentifier, SchemaManager, Table};
use crate::tests::{create_connection, get_database_dsn};
use crate::util::PlatformBox;
use crate::{Configuration, Connection, Result};

pub struct FunctionalTestsHelper {
    pub connection: Connection,
    pub platform: PlatformBox,
}

impl FunctionalTestsHelper {
    pub async fn default() -> Self {
        Self::new(create_connection().await.unwrap())
    }

    pub fn new(connection: Connection) -> Self {
        let platform = connection.get_platform().unwrap();

        Self {
            connection,
            platform,
        }
    }

    pub async fn with_configuration(configuration: Configuration) -> Self {
        Self::new(
            Connection::create_from_dsn(&get_database_dsn(), Some(configuration), None)
                .unwrap()
                .connect()
                .await
                .unwrap(),
        )
    }

    pub fn get_schema_manager(&self) -> Box<dyn SchemaManager + '_> {
        self.platform.create_schema_manager(&self.connection)
    }

    /// Drops the table with the specified name, if it exists.
    pub async fn drop_table_if_exists(&self, name: &dyn IntoIdentifier) {
        let schema_manager = self.get_schema_manager();
        let _ = schema_manager.drop_table(name).await;
    }

    /// Drops and creates a new table.
    pub async fn drop_and_create_table(&self, table: &Table) -> Result<()> {
        let schema_manager = self.get_schema_manager();
        self.drop_table_if_exists(table).await;
        schema_manager.create_table(table).await?;

        Ok(())
    }

    pub fn has_element_with_name<T: Asset, S: AsRef<str>>(&self, items: &[T], name: S) -> bool {
        let name = name.as_ref();

        items.iter().any(|item| {
            item.get_shortest_name(&item.get_namespace_name().unwrap_or_default()) == name
        })
    }

    pub fn filter_elements_by_name<T: Asset + Clone, S: AsRef<str>>(
        &self,
        items: &[T],
        name: S,
    ) -> Vec<T> {
        let name = name.as_ref();
        items
            .iter()
            .filter(|item| {
                item.get_shortest_name(&item.get_namespace_name().unwrap_or_default()) == name
            })
            .cloned()
            .collect()
    }

    pub fn get_test_table<S: IntoIdentifier>(&self, name: S) -> Result<Table> {
        let mut table = Table::new(name);

        let mut col = Column::new("id", INTEGER)?;
        col.set_notnull(true);
        table.add_column(col);

        let mut col = Column::new("test", STRING)?;
        col.set_length(255);
        table.add_column(col);

        table.add_column(Column::new("foreign_key_test", INTEGER)?);

        table.set_primary_key(&["id"], None)?;

        Ok(table)
    }

    pub async fn create_test_table<S: IntoIdentifier>(&self, name: S) -> Result<Table> {
        let table = self.get_test_table(name)?;
        self.drop_and_create_table(&table).await?;

        Ok(table)
    }
}
