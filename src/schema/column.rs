use crate::platform::DatabasePlatform;
use crate::schema::asset::{AbstractAsset, Asset};
use crate::schema::CheckConstraint;
use crate::Value;
use std::any::TypeId;

#[derive(Clone)]
pub struct ColumnData {
    pub name: String,
    pub r#type: TypeId,
    pub default: Value,
    pub notnull: bool,
    pub unique: bool,
    pub length: Option<usize>,
    pub precision: Option<usize>,
    pub scale: Option<usize>,
    pub fixed: Option<bool>,
    pub unsigned: Option<bool>,
    pub autoincrement: Option<bool>,
    pub column_definition: Option<String>,
    pub version: Option<bool>,
    pub comment: Option<String>,
    pub collation: Option<String>,
    pub charset: Option<String>,
    pub primary: bool,
    pub check: Option<CheckConstraint>,
    pub jsonb: Option<bool>,
}

#[derive(Clone)]
pub struct Column {
    asset: AbstractAsset,
    r#type: TypeId,
    default: Value,
    notnull: bool,
    unique: bool,
    length: Option<usize>,
    precision: Option<usize>,
    scale: Option<usize>,
    fixed: Option<bool>,
    unsigned: Option<bool>,
    autoincrement: Option<bool>,
    column_definition: Option<String>,
    version: Option<bool>,
    comment: Option<String>,
    collation: Option<String>,
    charset: Option<String>,
    check: Option<CheckConstraint>,
    jsonb: Option<bool>,
}

impl Column {
    pub fn new(name: String, r#type: TypeId) -> Self {
        let mut asset = AbstractAsset::default();
        asset.set_name(name);
        let default = Value::NULL;
        let notnull = false;

        Self {
            asset,
            r#type,
            default,
            notnull,
            unique: false,
            length: None,
            precision: None,
            scale: None,
            fixed: None,
            unsigned: None,
            autoincrement: None,
            column_definition: None,
            version: None,
            comment: None,
            collation: None,
            charset: None,
            check: None,
            jsonb: None,
        }
    }

    pub fn get_type(&self) -> TypeId {
        self.r#type
    }

    pub fn get_comment(&self) -> &Option<String> {
        &self.comment
    }

    pub fn is_notnull(&self) -> bool {
        self.notnull
    }

    pub fn is_autoincrement(&self) -> bool {
        self.autoincrement.unwrap_or(false)
    }

    pub(crate) fn generate_column_data(&self, platform: &dyn DatabasePlatform) -> ColumnData {
        let name = self.get_quoted_name(platform);

        ColumnData {
            name,
            r#type: self.r#type,
            default: self.default.clone(),
            notnull: self.notnull,
            unique: self.unique,
            length: self.length,
            precision: self.precision,
            scale: self.scale,
            fixed: self.fixed,
            unsigned: self.unsigned,
            autoincrement: self.autoincrement,
            column_definition: self.column_definition.clone(),
            version: self.version,
            comment: self.comment.clone(),
            collation: self.collation.clone(),
            charset: self.charset.clone(),
            primary: false,
            check: self.check.clone(),
            jsonb: self.jsonb,
        }
    }
}

impl Asset for Column {
    fn get_name(&self) -> String {
        self.asset.get_name()
    }

    fn set_name(&mut self, name: String) {
        self.asset.set_name(name)
    }

    fn get_namespace_name(&self) -> Option<String> {
        self.asset.get_namespace_name()
    }

    fn get_shortest_name(&self, default_namespace_name: &str) -> String {
        self.asset.get_shortest_name(default_namespace_name)
    }

    fn is_quoted(&self) -> bool {
        self.asset.is_quoted()
    }
}
