use crate::schema::{Asset, Column, Identifier};

/// Represents the change of a column.
pub struct ColumnDiff {
    old_column_name: String,
    pub column: Column,
    pub changed_properties: Vec<String>,
    pub from_column: Identifier,
}

impl ColumnDiff {
    pub fn new(
        old_column_name: &str,
        column: &Column,
        changed_properties: &[String],
        from_column: &str,
    ) -> Self {
        Self {
            old_column_name: old_column_name.to_string(),
            column: column.clone(),
            changed_properties: changed_properties.clone().to_vec(),
            from_column: Identifier::new(from_column, false),
        }
    }

    pub fn has_changed(&self, property_name: &str) -> bool {
        self.changed_properties.iter().any(|p| property_name.eq(p))
    }

    pub fn get_old_column_name(&self) -> Identifier {
        Identifier::new(&self.old_column_name, self.from_column.is_quoted())
    }
}
