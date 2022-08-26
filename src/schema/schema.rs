use crate::platform::DatabasePlatform;
use crate::schema::asset::{impl_asset, AbstractAsset};
use crate::schema::{Asset, Identifier, IntoIdentifier, Sequence, Table, View};
use crate::Result;

#[derive(IntoIdentifier)]
pub struct Schema {
    asset: AbstractAsset,
    tables: Vec<Table>,
    sequences: Vec<Sequence>,
    views: Vec<View>,
    schema_names: Vec<Identifier>,
}

impl Schema {
    pub fn new(
        tables: Vec<Table>,
        views: Vec<View>,
        sequences: Vec<Sequence>,
        schema_names: Vec<Identifier>,
    ) -> Self {
        Self {
            asset: AbstractAsset::default(),
            tables,
            sequences,
            views,
            schema_names,
        }
    }

    pub fn get_schema_names(&self) -> &Vec<Identifier> {
        &self.schema_names
    }

    pub fn has_schema_name(&self, name: &dyn IntoIdentifier) -> bool {
        let name = name.into_identifier().get_name();
        self.schema_names.iter().any(|i| i.get_name() == name)
    }

    pub fn get_tables(&self) -> &Vec<Table> {
        &self.tables
    }

    pub fn get_table(&self, name: &dyn IntoIdentifier) -> Option<&Table> {
        let name = name.into_identifier().get_name();
        self.tables.iter().find(|i| i.get_name() == name)
    }

    /// Gets the first table matching name and unwraps the value.
    ///
    /// # Safety
    ///
    /// Calling this method without checking if table exists will _panic_.
    pub unsafe fn get_table_unchecked(&self, name: &dyn IntoIdentifier) -> &Table {
        let name = name.into_identifier().get_name();
        self.tables.iter().find(|i| i.get_name() == name).unwrap()
    }

    pub fn has_table(&self, name: &dyn IntoIdentifier) -> bool {
        let name = name.into_identifier().get_name();
        self.tables.iter().any(|i| i.get_name() == name)
    }

    pub fn get_sequences(&self) -> &Vec<Sequence> {
        &self.sequences
    }

    pub fn get_sequence(&self, name: &dyn IntoIdentifier) -> Option<&Sequence> {
        let name = name.into_identifier().get_name();
        self.sequences.iter().find(|i| i.get_name() == name)
    }

    /// Gets the first sequence matching name and unwraps the value.
    ///
    /// # Safety
    ///
    /// Calling this method without checking if sequence exists will _panic_.
    pub unsafe fn get_sequence_unchecked(&self, name: &dyn IntoIdentifier) -> &Sequence {
        let name = name.into_identifier().get_name();
        self.sequences
            .iter()
            .find(|i| i.get_name() == name)
            .unwrap()
    }

    pub fn has_sequence(&self, name: &dyn IntoIdentifier) -> bool {
        let name = name.into_identifier().get_name();
        self.sequences.iter().any(|i| i.get_name() == name)
    }

    pub fn to_drop_sql(&self, platform: &(dyn DatabasePlatform)) -> Result<String> {
        todo!()
    }
}

impl_asset!(Schema, asset);
