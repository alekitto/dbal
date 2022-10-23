use crate::r#type::{IntoType, BINARY, GUID, STRING};
use crate::schema::{
    Asset, ChangedProperty, Column, ColumnData, Index, Schema, SchemaDiff, SchemaManager, Sequence,
    Table, TableDiff,
};
use crate::{Result, Value};
use creed::r#type::DECIMAL;
use creed::schema::ColumnDiff;
use itertools::Itertools;
use std::collections::btree_map::Entry::{Occupied, Vacant};
use std::collections::BTreeMap;

fn is_auto_increment_sequence_in_schema(schema: &Schema, sequence: &Sequence) -> bool {
    schema
        .get_tables()
        .iter()
        .any(|t| sequence.is_autoincrement_for(t))
}

fn diff_sequence(sequence1: &Sequence, sequence2: &Sequence) -> bool {
    sequence1.get_allocation_size() != sequence2.get_allocation_size()
        || sequence1.get_initial_value() != sequence2.get_initial_value()
}

/// Try to find columns that only changed their name, rename operations maybe cheaper than add/drop
/// however ambiguities between different possibilities should not lead to renaming at all.
fn detect_column_renames<S: SchemaManager + ?Sized>(
    schema_manager: &S,
    table_differences: &mut TableDiff,
) {
    let mut rename_candidates = BTreeMap::new();
    for added_column in &table_differences.added_columns {
        for removed_column in &table_differences.removed_columns {
            if !schema_manager
                .columns_equal(added_column, removed_column)
                .unwrap_or(false)
            {
                continue;
            }

            match rename_candidates.entry(added_column.get_name()) {
                Vacant(e) => {
                    e.insert(vec![(removed_column, added_column)]);
                }
                Occupied(mut e) => {
                    e.get_mut().push((removed_column, added_column));
                }
            }
        }
    }

    let mut added_columns_names_to_be_removed = vec![];
    let mut removed_columns_names_to_be_removed = vec![];
    for candidate_columns in rename_candidates.into_values() {
        if candidate_columns.len() != 1 {
            continue;
        }

        let (removed_column, added_column) = *candidate_columns.get(0).unwrap();
        let removed_column_name = removed_column.get_name();
        let added_column_name = added_column.get_name().to_lowercase();

        if table_differences
            .renamed_columns
            .iter()
            .any(|(old_column_name, _)| old_column_name == &removed_column_name)
        {
            continue;
        }

        table_differences.renamed_columns.push((
            removed_column_name.clone().into_owned(),
            added_column.clone(),
        ));

        added_columns_names_to_be_removed.push(added_column_name);
        removed_columns_names_to_be_removed.push(removed_column_name.into_owned());
    }

    table_differences
        .added_columns
        .retain(|col| !added_columns_names_to_be_removed.contains(&col.get_name().to_lowercase()));
    table_differences.removed_columns.retain(|col| {
        !removed_columns_names_to_be_removed.contains(&col.get_name().to_lowercase())
    });
}

/// Try to find indexes that only changed their name, rename operations maybe cheaper than add/drop
/// however ambiguities between different possibilities should not lead to renaming at all.
fn detect_index_renames<C: Comparator + ?Sized>(comparator: &C, table_differences: &mut TableDiff) {
    let mut rename_candidates = BTreeMap::new();

    // Gather possible rename candidates by comparing each added and removed index based on semantics.
    for added_index in &table_differences.added_indexes {
        for removed_index in &table_differences.removed_indexes {
            if comparator.diff_index(added_index, removed_index) {
                continue;
            }

            match rename_candidates.entry(added_index.get_name()) {
                Vacant(e) => {
                    e.insert(vec![(removed_index, added_index)]);
                }
                Occupied(mut e) => {
                    e.get_mut().push((removed_index, added_index));
                }
            }
        }
    }

    let mut added_indexes_names_to_be_removed = vec![];
    let mut removed_indexes_names_to_be_removed = vec![];
    for candidate_indexes in rename_candidates.into_values() {
        // If the current rename candidate contains exactly one semantically equal index,
        // we can safely rename it.
        // Otherwise it is unclear if a rename action is really intended,
        // therefore we let those ambiguous indexes be added/dropped.
        if candidate_indexes.len() != 1 {
            continue;
        }

        let (removed_index, added_index) = candidate_indexes[0];

        let removed_index_name = removed_index.get_name().to_lowercase();
        let added_index_name = added_index.get_name().to_lowercase();

        if table_differences
            .renamed_indexes
            .iter()
            .any(|(name, _)| name == &removed_index_name)
        {
            continue;
        }

        table_differences
            .renamed_indexes
            .push((removed_index_name.clone(), added_index.clone()));
        added_indexes_names_to_be_removed.push(added_index_name);
        removed_indexes_names_to_be_removed.push(removed_index_name);
    }

    table_differences
        .added_indexes
        .retain(|col| !added_indexes_names_to_be_removed.contains(&col.get_name().to_lowercase()));
    table_differences.removed_indexes.retain(|col| {
        !removed_indexes_names_to_be_removed.contains(&col.get_name().to_lowercase())
    });
}

/// Returns the difference between the columns
///
/// If there are differences this method returns the changed properties as a
/// string vector, otherwise an empty vector gets returned.
pub fn diff_column(properties1: ColumnData, properties2: ColumnData) -> Vec<ChangedProperty> {
    let mut changed_properties = vec![];
    if properties1.r#type != properties2.r#type {
        changed_properties.push(ChangedProperty::Type);
    }

    if properties1.notnull != properties2.notnull {
        changed_properties.push(ChangedProperty::NotNull);
    }

    if properties1.unsigned.unwrap_or(false) != properties2.unsigned.unwrap_or(false) {
        changed_properties.push(ChangedProperty::Unsigned);
    }

    if properties1.autoincrement != properties2.autoincrement {
        changed_properties.push(ChangedProperty::AutoIncrement);
    }

    // Null values need to be checked additionally as they tell whether to create or drop a default value.
    // null != 0, null != false, null != '' etc. This affects platform's table alteration SQL generation.
    if ((properties1.default == Value::NULL) != (properties2.default == Value::NULL))
        || properties1.default != properties2.default
    {
        changed_properties.push(ChangedProperty::Default);
    }

    if properties1.r#type == STRING.into_type().unwrap()
        && properties1.r#type != GUID.into_type().unwrap()
        || properties1.r#type == BINARY.into_type().unwrap()
    {
        // check if value of length is set at all, default value assumed otherwise.
        let length1 = properties1.length.unwrap_or(255);
        let length2 = properties2.length.unwrap_or(255);
        if length1 != length2 {
            changed_properties.push(ChangedProperty::Length);
        }

        if properties1.fixed != properties2.fixed {
            changed_properties.push(ChangedProperty::Fixed);
        }
    } else if properties1.r#type == DECIMAL.into_type().unwrap() {
        if properties1.precision.unwrap_or(10) != properties2.precision.unwrap_or(10) {
            changed_properties.push(ChangedProperty::Precision);
        }

        if properties1.scale != properties2.scale {
            changed_properties.push(ChangedProperty::Scale);
        }
    }

    changed_properties.into_iter().unique().collect()
}

pub trait Comparator {
    fn get_schema_manager(&self) -> &dyn SchemaManager;

    fn compare_schemas<'a>(
        &'a self,
        from_schema: &'a Schema,
        to_schema: &'a Schema,
    ) -> Result<SchemaDiff<'a>> {
        let src_schema_name = from_schema.get_name();
        let dest_schema_name = to_schema.get_name();

        let mut foreign_keys_to_table = BTreeMap::new();
        let mut new_schema_names = vec![];
        let mut removed_schema_names = vec![];

        for namespace in to_schema.get_schema_names() {
            if !from_schema.has_schema_name(namespace) {
                new_schema_names.push(namespace);
            }
        }

        for namespace in from_schema.get_schema_names() {
            if !to_schema.has_schema_name(&namespace) {
                removed_schema_names.push(namespace);
            }
        }

        let mut new_tables = vec![];
        let mut changed_tables = BTreeMap::new();
        let mut removed_tables = vec![];

        for table in to_schema.get_tables() {
            let table_name = table.get_shortest_name(&dest_schema_name);
            if !from_schema.has_table(&table_name) {
                new_tables.push(table);
            } else if let Some(table_differences) = self.diff_table(
                unsafe { from_schema.get_table_unchecked(&table_name) },
                unsafe { to_schema.get_table_unchecked(&table_name) },
            )? {
                changed_tables.insert(table_name.to_lowercase(), table_differences);
            }
        }

        /* Check if there are tables removed */
        for table in from_schema.get_tables() {
            let table_name = table.get_shortest_name(&src_schema_name);
            if to_schema.has_table(&table_name) {
                removed_tables.push(table);
            }

            // also remember all foreign keys that point to a specific table
            for foreign_key in table.get_foreign_keys() {
                let foreign_table = foreign_key.get_foreign_table();
                let table_name = foreign_table.get_name();
                match foreign_keys_to_table.entry(table_name) {
                    Vacant(e) => {
                        e.insert(vec![(foreign_key, foreign_table.clone())]);
                    }
                    Occupied(mut e) => {
                        e.get_mut().push((foreign_key, foreign_table.clone()));
                    }
                };
            }
        }

        let mut diff = SchemaDiff::new(
            new_tables,
            changed_tables,
            removed_tables,
            Some(from_schema),
        );
        for table in &diff.removed_tables {
            if let Occupied(e) = foreign_keys_to_table.entry(table.get_name()) {
                for (foreign_key, local_table) in e.get() {
                    if diff
                        .removed_tables
                        .iter()
                        .any(|t| t.get_name().to_lowercase() == local_table.get_name())
                    {
                        continue;
                    }

                    diff.orphaned_foreign_keys
                        .push(((*foreign_key).clone(), local_table.clone()));
                }

                // deleting duplicated foreign keys present on both on the orphanedForeignKey
                // and the removedForeignKeys from changedTables
                for (_, local_table_name) in e.get() {
                    let local_table_name = local_table_name.get_name().to_lowercase();
                    if let Occupied(mut c) = diff.changed_tables.entry(local_table_name) {
                        let changed_table = c.get_mut();
                        let mut removed_foreign_keys = vec![];
                        for removed_foreign_key in changed_table.removed_foreign_keys.drain(..) {
                            if e.key() != &removed_foreign_key.get_foreign_table().get_name() {
                                removed_foreign_keys.push(removed_foreign_key)
                            }
                        }

                        changed_table.removed_foreign_keys = removed_foreign_keys;
                    }
                }
            }
        }

        for sequence in to_schema.get_sequences() {
            let sequence_name = sequence.get_shortest_name(&dest_schema_name);
            if !from_schema.has_sequence(&sequence_name) {
                if !is_auto_increment_sequence_in_schema(from_schema, sequence) {
                    diff.new_sequences.push(sequence);
                }
            } else if diff_sequence(sequence, unsafe {
                from_schema.get_sequence_unchecked(&sequence_name)
            }) {
                diff.changed_sequences.push(sequence);
            }
        }

        for sequence in from_schema.get_sequences() {
            if !is_auto_increment_sequence_in_schema(to_schema, sequence) {
                let sequence_name = sequence.get_shortest_name(&src_schema_name);
                if !to_schema.has_sequence(&sequence_name) {
                    diff.removed_sequences.push(sequence);
                }
            }
        }

        Ok(diff)
    }

    fn diff_table<'a>(
        &'a self,
        from_table: &'a Table,
        to_table: &'_ Table,
    ) -> Result<Option<TableDiff<'a>>> {
        let schema_manager = self.get_schema_manager();
        let mut changes = 0;
        let mut table_differences = TableDiff::new(from_table.get_name(), Some(from_table));

        let from_table_columns = from_table.get_columns();
        let to_table_columns = to_table.get_columns();

        /* See if all the columns in "from" table exist in "to" table */
        for column in to_table_columns {
            let column_name = column.get_name();
            if from_table.has_column(&column_name) {
                continue;
            }

            table_differences.added_columns.push(column.clone());
            changes += 1;
        }

        /* See if there are any removed columns in "to" table */
        for column in from_table_columns {
            // See if column is removed in "to" table.
            let column_name = column.get_name();
            if let Some(to_column) = to_table.get_column(&column_name) {
                // See if column has changed properties in "to" table.
                let changed_properties = self.diff_column(column, to_column);
                if !schema_manager.columns_equal(column, to_column)? {
                    table_differences.changed_columns.push(ColumnDiff::new(
                        &column.get_name(),
                        to_column,
                        changed_properties.as_slice(),
                        Some(column.clone()),
                    ));

                    changes += 1;
                }
            } else {
                table_differences.removed_columns.push(column.clone());
                changes += 1;
            }
        }

        detect_column_renames(schema_manager, &mut table_differences);

        let from_table_indexes = from_table.get_indices();
        let to_table_indexes = to_table.get_indices();

        /* See if all the indexes in "from" table exist in "to" table */
        for index in to_table_indexes {
            let index_name = index.get_name();
            if (index.is_primary() && from_table.has_primary_key())
                || from_table.has_index(&index_name)
            {
                continue;
            }

            table_differences.added_indexes.push(index.clone());
            changes += 1;
        }

        /* See if there are any removed indexes in "to" table */
        for index in from_table_indexes {
            let index_name = index.get_name();

            // See if index is removed in "to" table.
            if index.is_primary() {
                if !to_table.has_primary_key() {
                    table_differences.removed_indexes.push(index.clone());
                    changes += 1;
                    continue;
                }
            } else if !to_table.has_index(&index_name) {
                table_differences.removed_indexes.push(index.clone());
                changes += 1;
                continue;
            }

            if let Some(to_table_index) = if index.is_primary() {
                to_table.get_primary_key()
            } else {
                to_table.get_index(&index_name)
            } {
                if self.diff_index(index, to_table_index) {
                    table_differences
                        .changed_indexes
                        .push(to_table_index.clone());
                    changes += 1;
                }
            }
        }

        detect_index_renames(self, &mut table_differences);

        let from_foreign_keys = from_table.get_foreign_keys();
        let to_foreign_keys = to_table.get_foreign_keys();

        for from_constraint in from_foreign_keys {
            for to_constraint in to_foreign_keys {
                if from_constraint == to_constraint {
                    if from_constraint.get_name().to_lowercase()
                        == to_constraint.get_name().to_lowercase()
                    {
                        table_differences
                            .changed_foreign_keys
                            .push(to_constraint.clone());
                    } else {
                        table_differences
                            .removed_foreign_keys
                            .push(from_constraint.clone());
                        table_differences
                            .added_foreign_keys
                            .push(to_constraint.clone());
                    }

                    changes += 1;
                }
            }
        }

        Ok(if changes > 0 {
            Some(table_differences)
        } else {
            None
        })
    }

    /// Returns the difference between the columns
    ///
    /// If there are differences this method returns the changed properties as a
    /// string vector, otherwise an empty vector gets returned.
    fn diff_column(&self, column1: &Column, column2: &Column) -> Vec<ChangedProperty> {
        let platform = self.get_schema_manager().get_platform().unwrap();
        let properties1 = column1.generate_column_data(&platform);
        let properties2 = column2.generate_column_data(&platform);

        diff_column(properties1, properties2)
    }

    /// Finds the difference between the indexes $index1 and $index2.
    /// Compares index1 with index2 and returns if there are any differences.
    fn diff_index(&self, index1: &Index, index2: &Index) -> bool {
        !index1.is_fulfilled_by(index2) && index2.is_fulfilled_by(index1)
    }
}

pub struct GenericComparator<'a> {
    schema_manager: &'a dyn SchemaManager,
}

impl<'a> GenericComparator<'a> {
    pub fn new(schema_manager: &'a dyn SchemaManager) -> Self {
        Self { schema_manager }
    }
}

impl<'a> Comparator for GenericComparator<'a> {
    fn get_schema_manager(&self) -> &'a dyn SchemaManager {
        self.schema_manager
    }
}

impl<C: Comparator + ?Sized> Comparator for &mut C {
    delegate::delegate! {
        to(**self) {
            fn diff_table<'a>(&'a self, from_table: &'a Table, to_table: &'_ Table) -> Result<Option<TableDiff<'a>>>;
            fn compare_schemas<'a>(&'a self, from_schema: &'a Schema, to_schema: &'a Schema) -> Result<SchemaDiff<'a>>;
            fn get_schema_manager(&self) -> &dyn SchemaManager;
            fn diff_column(&self, column1: &Column, column2: &Column) -> Vec<ChangedProperty>;
            fn diff_index(&self, index1: &Index, index2: &Index) -> bool;
        }
    }
}

impl<C: Comparator + ?Sized> Comparator for Box<C> {
    delegate::delegate! {
        to(**self) {
            fn diff_table<'a>(&'a self, from_table: &'a Table, to_table: &'_ Table) -> Result<Option<TableDiff<'a>>>;
            fn compare_schemas<'a>(&'a self, from_schema: &'a Schema, to_schema: &'a Schema) -> Result<SchemaDiff<'a>>;
            fn get_schema_manager(&self) -> &dyn SchemaManager;
            fn diff_column(&self, column1: &Column, column2: &Column) -> Vec<ChangedProperty>;
            fn diff_index(&self, index1: &Index, index2: &Index) -> bool;
        }
    }
}
