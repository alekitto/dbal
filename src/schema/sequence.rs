use crate::schema::asset::{AbstractAsset, impl_asset};
use crate::schema::{Asset, Identifier, IntoIdentifier, Table};

#[derive(Clone, Debug, IntoIdentifier, Eq, PartialEq)]
pub struct Sequence {
    asset: AbstractAsset,
    allocation_size: usize,
    initial_value: usize,
    cache: Option<usize>,
}

impl Sequence {
    pub fn new<
        S: AsRef<str>,
        AS: Into<Option<usize>>,
        IV: Into<Option<usize>>,
        C: Into<Option<usize>>,
    >(
        name: S,
        allocation_size: AS,
        initial_value: IV,
        cache: C,
    ) -> Self {
        let mut asset = AbstractAsset::default();
        asset.set_name(name.as_ref());

        Self {
            asset,
            allocation_size: allocation_size.into().unwrap_or(1),
            initial_value: initial_value.into().unwrap_or(1),
            cache: cache.into(),
        }
    }

    pub fn get_allocation_size(&self) -> usize {
        self.allocation_size
    }

    pub fn get_initial_value(&self) -> usize {
        self.initial_value
    }

    pub fn get_cache(&self) -> Option<usize> {
        self.cache
    }

    pub fn set_allocation_size(&mut self, mut allocation_size: usize) {
        if allocation_size == 0 {
            allocation_size = 1;
        }

        self.allocation_size = allocation_size;
    }

    pub fn set_initial_value(&mut self, mut initial_value: usize) {
        if initial_value == 0 {
            initial_value = 1;
        }

        self.initial_value = initial_value;
    }

    pub fn set_cache(&mut self, cache: Option<usize>) {
        self.cache = cache;
    }

    /// Checks if this sequence is an autoincrement sequence for a given table.
    ///
    /// This is used inside the comparator to not report sequences as missing,
    /// when the "from" schema implicitly creates the sequences.
    pub fn is_autoincrement_for(&self, table: &Table) -> bool {
        let primary_key = table.get_primary_key();
        if let Some(primary_key) = primary_key {
            let cols = primary_key.get_columns();
            if cols.len() > 1 || cols.is_empty() {
                return false;
            }

            let pk_column = Identifier::new(cols.first().unwrap(), false);
            if let Some(pk_column) = table.get_column(&pk_column) {
                if !pk_column.is_autoincrement() {
                    return false;
                }

                let sequence_name = self
                    .asset
                    .get_shortest_name(&table.get_namespace_name().unwrap_or_default());
                let table_name =
                    table.get_shortest_name(&table.get_namespace_name().unwrap_or_default());
                let table_sequence_name = format!(
                    "{}_{}_seq",
                    table_name,
                    pk_column.get_shortest_name(&table.get_namespace_name().unwrap_or_default())
                );

                return table_sequence_name == sequence_name;
            }
        }

        false
    }
}

impl_asset!(Sequence, asset);
