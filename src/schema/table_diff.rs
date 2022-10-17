use crate::schema::{Asset, Column, ColumnDiff, ForeignKeyConstraint, Identifier, Index, Table};

#[derive(Clone)]
pub struct TableDiff<'a>
where
    Self: 'a,
{
    pub name: String,
    pub new_name: Option<String>,

    pub added_columns: Vec<Column>,
    pub changed_columns: Vec<ColumnDiff>,
    pub removed_columns: Vec<Column>,
    /// Columns that are only renamed.
    /// Old name is in the first element of the tuple.
    pub renamed_columns: Vec<(String, Column)>,

    pub added_indexes: Vec<Index>,
    pub changed_indexes: Vec<Index>,
    pub removed_indexes: Vec<Index>,
    /// Indexes that are only renamed but are identical otherwise.
    /// Old name is in the first element of the tuple.
    pub renamed_indexes: Vec<(String, Index)>,

    pub added_foreign_keys: Vec<ForeignKeyConstraint>,
    pub changed_foreign_keys: Vec<ForeignKeyConstraint>,
    pub removed_foreign_keys: Vec<ForeignKeyConstraint>,

    pub from_table: Option<&'a Table>,
}

impl<'a> TableDiff<'a>
where
    Self: 'a,
{
    pub fn new<S: AsRef<str>, I: Into<Option<&'a Table>>>(table_name: S, from_table: I) -> Self {
        Self {
            name: table_name.as_ref().to_string(),
            new_name: None,
            added_columns: vec![],
            changed_columns: vec![],
            removed_columns: vec![],
            renamed_columns: vec![],
            added_indexes: vec![],
            changed_indexes: vec![],
            removed_indexes: vec![],
            renamed_indexes: vec![],
            added_foreign_keys: vec![],
            changed_foreign_keys: vec![],
            removed_foreign_keys: vec![],
            from_table: from_table.into(),
        }
    }

    pub fn get_name(&self) -> Identifier {
        if let Some(t) = self.from_table {
            t.get_table_name().clone()
        } else {
            Identifier::new(&self.name, false)
        }
    }

    pub fn get_new_name(&self) -> Option<Identifier> {
        self.new_name.as_ref().map(|t| Identifier::new(t, false))
    }

    pub fn get_added_column(&self, column_name: &str) -> Option<&Column> {
        self.added_columns
            .iter()
            .find(|column| (*column).get_name() == column_name)
    }
}
