use crate::platform::DatabasePlatform;
use crate::schema::{
    Asset, ForeignKeyConstraint, Identifier, Schema, SchemaManager, Sequence, Table, TableDiff,
};
use crate::Result;
use std::collections::BTreeMap;

/// Differences between two schemas.
///
/// The object contains the operations to change the schema stored in $fromSchema
/// to a target schema.
#[derive(Clone, Default)]
pub struct SchemaDiff<'a> {
    #[allow(dead_code)]
    /// The optional "from" schema.
    from_schema: Option<&'a Schema>,
    /// All added namespaces.
    pub new_namespaces: Vec<String>,
    /// All removed namespaces.
    pub removed_namespaces: Vec<String>,
    /// All added tables.
    new_tables: Vec<&'a Table>,
    /// All changed tables.
    pub changed_tables: BTreeMap<String, TableDiff<'a>>,
    /// All removed tables.
    pub removed_tables: Vec<&'a Table>,

    pub new_sequences: Vec<&'a Sequence>,
    pub changed_sequences: Vec<&'a Sequence>,
    pub removed_sequences: Vec<&'a Sequence>,

    pub orphaned_foreign_keys: Vec<(ForeignKeyConstraint, Identifier)>,
}

impl<'a> SchemaDiff<'a> {
    /// Creates a new SchemaDiff.
    pub fn new(
        new_tables: Vec<&'a Table>,
        changed_tables: BTreeMap<String, TableDiff<'a>>,
        removed_tables: Vec<&'a Table>,
        from_schema: Option<&'a Schema>,
    ) -> Self {
        Self {
            from_schema,
            new_namespaces: vec![],
            removed_namespaces: vec![],
            new_tables,
            changed_tables,
            removed_tables,
            new_sequences: vec![],
            changed_sequences: vec![],
            removed_sequences: vec![],
            orphaned_foreign_keys: vec![],
        }
    }

    /// The to save sql mode ensures that the following things don't happen:
    ///
    /// 1. Tables are deleted
    /// 2. Sequences are deleted
    /// 3. Foreign Keys which reference tables that would otherwise be deleted.
    pub fn to_save_sql<S: SchemaManager + ?Sized>(
        mut self,
        schema_manager: &S,
    ) -> Result<Vec<String>> {
        let platform = schema_manager.get_platform()?;
        let mut sql = vec![];

        if platform.supports_schemas() {
            for new_namespace in &self.new_namespaces {
                sql.push(schema_manager.get_create_schema_sql(new_namespace)?);
            }
        }

        if platform.supports_sequences() {
            for sequence in &self.changed_sequences {
                sql.push(schema_manager.get_alter_sequence_sql(sequence)?);
            }

            for sequence in &self.new_sequences {
                sql.push(schema_manager.get_create_sequence_sql(sequence)?);
            }
        }

        let mut foreign_key_sql = vec![];
        for table in &self.new_tables {
            sql.append(&mut schema_manager.get_create_table_sql(table, None)?);
        }

        sql.append(&mut foreign_key_sql);

        for (_, table_diff) in self.changed_tables.iter_mut() {
            sql.append(&mut schema_manager.get_alter_table_sql(table_diff)?);
        }

        Ok(sql)
    }

    pub fn to_sql<S: SchemaManager + ?Sized>(mut self, schema_manager: &S) -> Result<Vec<String>> {
        let platform = schema_manager.get_platform()?;
        let mut sql = vec![];

        if platform.supports_schemas() {
            for new_namespace in &self.new_namespaces {
                sql.push(schema_manager.get_create_schema_sql(new_namespace)?);
            }
        }

        if platform.supports_foreign_key_constraints() {
            for (orphaned_foreign_key, table_name) in &self.orphaned_foreign_keys {
                sql.push(
                    schema_manager.get_drop_foreign_key_sql(orphaned_foreign_key, table_name)?,
                );
            }
        }

        if platform.supports_sequences() {
            for sequence in &self.changed_sequences {
                sql.push(schema_manager.get_alter_sequence_sql(sequence)?);
            }

            for sequence in &self.removed_sequences {
                sql.push(
                    schema_manager.get_drop_database_sql(&sequence.get_quoted_name(&platform))?,
                );
            }

            for sequence in &self.new_sequences {
                sql.push(schema_manager.get_create_sequence_sql(sequence)?);
            }
        }

        let mut foreign_key_sql = vec![];
        for table in &self.new_tables {
            sql.append(&mut schema_manager.get_create_table_sql(table, None)?);
        }

        sql.append(&mut foreign_key_sql);
        for table in &self.removed_tables {
            sql.push(schema_manager.get_drop_table_sql(table.get_table_name())?);
        }

        for (_, table_diff) in self.changed_tables.iter_mut() {
            sql.append(&mut schema_manager.get_alter_table_sql(table_diff)?);
        }

        Ok(sql)
    }
}
