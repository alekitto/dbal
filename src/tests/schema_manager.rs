use crate::schema::{Column, Comparator, SchemaManager};
use crate::{Connection, Row};

pub struct MockSchemaManager<'a> {
    connection: &'a Connection,
}

impl<'a> MockSchemaManager<'a> {
    pub fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }
}

impl<'a> SchemaManager for MockSchemaManager<'a> {
    fn get_connection(&self) -> &'a Connection {
        self.connection
    }

    fn as_dyn(&self) -> &dyn SchemaManager {
        self
    }

    fn get_portable_table_column_definition(&self, _: &Row) -> crate::Result<Column> {
        todo!()
    }

    fn create_comparator(&self) -> Box<dyn Comparator + Send + '_> {
        todo!()
    }
}
