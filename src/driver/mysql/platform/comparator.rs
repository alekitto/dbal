use crate::schema::{diff_column, ChangedProperty, Column, Comparator, SchemaManager};

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

    /// Returns the difference between the columns
    ///
    /// If there are differences this method returns the changed properties as a
    /// string vector, otherwise an empty vector gets returned.
    fn diff_column(&self, column1: &Column, column2: &Column) -> Vec<ChangedProperty> {
        let platform = self.get_schema_manager().get_platform().unwrap();
        let mut properties1 = column1.generate_column_data(&platform);
        let mut properties2 = column2.generate_column_data(&platform);

        properties1.charset = None;
        properties2.charset = None;
        properties1.collation = None;
        properties2.collation = None;

        diff_column(properties1, properties2)
    }
}
