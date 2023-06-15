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

    pub fn has_schema_name<T: IntoIdentifier>(&self, name: T) -> bool {
        let name = name.into_identifier();
        let name = name.get_name();
        self.schema_names.iter().any(|i| i.get_name() == name)
    }

    pub fn get_tables(&self) -> &Vec<Table> {
        &self.tables
    }

    pub fn get_table<T: IntoIdentifier>(&self, name: T) -> Option<&Table> {
        let name = name.into_identifier();
        let name = name.get_name();
        self.tables.iter().find(|i| i.get_name() == name)
    }

    pub fn get_table_mut<T: IntoIdentifier>(&mut self, name: T) -> Option<&mut Table> {
        let name = name.into_identifier();
        let name = name.get_name();
        self.tables.iter_mut().find(|i| i.get_name() == name)
    }

    /// Gets the first table matching name and unwraps the value.
    ///
    /// # Safety
    ///
    /// Calling this method without checking if table exists will _panic_.
    pub unsafe fn get_table_unchecked<T: IntoIdentifier>(&self, name: T) -> &Table {
        let name = name.into_identifier();
        let name = name.get_name();
        self.tables.iter().find(|i| i.get_name() == name).unwrap()
    }

    pub fn has_table<T: IntoIdentifier>(&self, name: T) -> bool {
        let name = name.into_identifier();
        let name = name.get_name();
        self.tables.iter().any(|i| i.get_name() == name)
    }

    pub fn create_table<T: IntoIdentifier>(&mut self, table: T) -> Result<&mut Table> {
        let name = table.into_identifier();
        let name = name.get_name();
        if self.has_table(&name) {
            Err(format!(r#"Table "{}" already exists."#, name).into())
        } else {
            self.tables.push(Table::new(&name));
            Ok(self.get_table_mut(name).unwrap())
        }
    }

    pub fn drop_table<T: IntoIdentifier>(&mut self, name: T) {
        let name = name.into_identifier();
        let name = name.get_name();
        if let Some(pos) = self
            .tables
            .iter()
            .position(|table| table.get_name() == name)
        {
            self.tables.remove(pos);
        }
    }

    pub fn get_sequences(&self) -> &Vec<Sequence> {
        &self.sequences
    }

    pub fn get_sequence<T: IntoIdentifier>(&self, name: T) -> Option<&Sequence> {
        let name = name.into_identifier();
        let name = name.get_name();
        self.sequences.iter().find(|i| i.get_name() == name)
    }

    /// Gets the first sequence matching name and unwraps the value.
    ///
    /// # Safety
    ///
    /// Calling this method without checking if sequence exists will _panic_.
    pub unsafe fn get_sequence_unchecked<T: IntoIdentifier>(&self, name: T) -> &Sequence {
        let name = name.into_identifier();
        let name = name.get_name();
        self.sequences
            .iter()
            .find(|i| i.get_name() == name)
            .unwrap()
    }

    pub fn has_sequence<T: IntoIdentifier>(&self, name: T) -> bool {
        let name = name.into_identifier();
        let name = name.get_name();
        self.sequences.iter().any(|i| i.get_name() == name)
    }

    pub fn to_drop_sql(&self, platform: &dyn DatabasePlatform) -> Result<String> {
        todo!()
    }
}

impl_asset!(Schema, asset);
