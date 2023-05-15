use crate::schema::{Comparator, SchemaManager};

pub struct MySQLComparator<'a> {
    schema_manager: &'a dyn SchemaManager,
}

impl<'a> MySQLComparator<'a> {
    pub fn new(schema_manager: &'a dyn SchemaManager) -> Self {
        Self { schema_manager }
    }
}

impl<'a> Comparator for MySQLComparator<'a> {
    fn get_schema_manager(&self) -> &'a dyn SchemaManager {
        self.schema_manager
    }
}
