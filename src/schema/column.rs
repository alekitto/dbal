use crate::platform::DatabasePlatform;
use crate::r#type::IntoType;
use crate::schema::asset::{impl_asset, AbstractAsset, Asset};
use crate::schema::{CheckConstraint, IntoIdentifier};
use crate::{Result, Value};
use creed::r#type::TypePtr;

#[derive(Clone)]
pub struct ColumnData {
    pub name: String,
    pub r#type: TypePtr,
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

#[derive(Clone, IntoIdentifier)]
pub struct Column {
    asset: AbstractAsset,
    r#type: TypePtr,
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
    pub fn new<I: IntoType>(name: &str, r#type: I) -> Result<Self> {
        let r#type = r#type.into_type()?;
        let mut asset = AbstractAsset::default();
        asset.set_name(name);

        Ok(Self {
            asset,
            r#type,
            default: Value::NULL,
            notnull: true,
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
        })
    }

    pub fn get_type(&self) -> TypePtr {
        self.r#type.clone()
    }

    pub fn get_default(&self) -> &Value {
        &self.default
    }

    pub fn set_default(&mut self, default: Value) {
        self.default = default;
    }

    pub fn get_comment(&self) -> &Option<String> {
        &self.comment
    }

    pub fn set_notnull(&mut self, notnull: bool) {
        self.notnull = notnull;
    }

    pub fn is_notnull(&self) -> bool {
        self.notnull
    }

    pub fn set_autoincrement<T: Into<Option<bool>>>(&mut self, autoincrement: T) {
        self.autoincrement = autoincrement.into();
    }

    pub fn is_autoincrement(&self) -> bool {
        self.autoincrement.unwrap_or(false)
    }

    pub fn set_length<S: Into<Option<usize>>>(&mut self, length: S) {
        self.length = length.into();
    }

    pub fn get_length(&self) -> Option<usize> {
        self.length
    }

    pub(crate) fn generate_column_data(&self, platform: &dyn DatabasePlatform) -> ColumnData {
        let name = self.get_quoted_name(platform);

        ColumnData {
            name,
            r#type: self.r#type.clone(),
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

impl_asset!(Column, asset);
