use crate::schema::{Asset, Column, Identifier};
use std::borrow::Borrow;

/// Represents the change of a column.
#[derive(Clone)]
pub struct ColumnDiff {
    old_column_name: String,
    pub column: Column,
    pub changed_properties: Vec<String>,
    pub from_column: Option<Column>,
}

impl ColumnDiff {
    pub fn new<S: Borrow<str>>(
        old_column_name: &str,
        column: &Column,
        changed_properties: &[S],
        from_column: Option<Column>,
    ) -> Self {
        Self {
            old_column_name: old_column_name.to_string(),
            column: column.clone(),
            changed_properties: changed_properties
                .iter()
                .map(|c| c.borrow().to_string())
                .collect(),
            from_column,
        }
    }

    pub fn has_changed(&self, property_name: &str) -> bool {
        self.changed_properties.iter().any(|p| property_name.eq(p))
    }

    pub fn get_old_column_name(&self) -> Identifier {
        Identifier::new(
            &self.old_column_name,
            self.from_column
                .as_ref()
                .map(|c| c.is_quoted())
                .unwrap_or(false),
        )
    }
}
