use crate::platform::DatabasePlatform;
use crate::r#type::IntoType;
use crate::schema::asset::{impl_asset, AbstractAsset, Asset};
use crate::schema::{CheckConstraint, IntoIdentifier};
use crate::{Result, Value};
use crate::r#type::TypePtr;

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
    pub fixed: bool,
    pub unsigned: Option<bool>,
    pub autoincrement: bool,
    pub column_definition: Option<String>,
    pub version: Option<bool>,
    pub comment: Option<String>,
    pub collation: Option<String>,
    pub charset: Option<String>,
    pub primary: bool,
    pub check: Option<CheckConstraint>,
    pub jsonb: bool,
}

pub struct ColumnBuilder {
    column: Column,
}

impl ColumnBuilder {
    fn new(column: Column) -> Self {
        Self { column }
    }

    pub fn get_column(self) -> Column {
        self.column
    }

    pub fn set_default<I: Into<Value>>(mut self, default: I) -> Self {
        self.column.set_default(default.into());
        self
    }

    pub fn set_comment<T: AsRef<str>, S: Into<Option<T>>>(mut self, comment: S) -> Self {
        self.column.set_comment(comment);
        self
    }

    pub fn set_collation<T: AsRef<str>, S: Into<Option<T>>>(mut self, collation: S) -> Self {
        self.column.set_collation(collation);
        self
    }

    pub fn set_charset<T: AsRef<str>, S: Into<Option<T>>>(mut self, charset: S) -> Self {
        self.column.set_charset(charset);
        self
    }

    pub fn set_notnull(mut self, notnull: bool) -> Self {
        self.column.set_notnull(notnull);
        self
    }

    pub fn set_autoincrement<T: Into<Option<bool>>>(mut self, autoincrement: T) -> Self {
        self.column.set_autoincrement(autoincrement);
        self
    }

    pub fn set_column_definition(mut self, def: Option<String>) -> Self {
        self.column.set_column_definition(def);
        self
    }

    pub fn set_length<S: Into<Option<usize>>>(mut self, length: S) -> Self {
        self.column.set_length(length);
        self
    }

    pub fn set_fixed<I: Into<Option<bool>>>(mut self, fixed: I) -> Self {
        self.column.set_fixed(fixed);
        self
    }

    pub fn set_unsigned<I: Into<Option<bool>>>(mut self, unsigned: I) -> Self {
        self.column.set_unsigned(unsigned);
        self
    }

    pub fn set_precision<I: Into<Option<usize>>>(mut self, precision: I) -> Self {
        self.column.set_precision(precision);
        self
    }

    pub fn set_scale<I: Into<Option<usize>>>(mut self, scale: I) -> Self {
        self.column.set_scale(scale);
        self
    }

    pub fn set_jsonb<I: Into<Option<bool>>>(mut self, jsonb: I) -> Self {
        self.column.set_jsonb(jsonb);
        self
    }
}

impl From<ColumnBuilder> for Column {
    fn from(value: ColumnBuilder) -> Self {
        value.get_column()
    }
}

#[derive(Clone, Debug, IntoIdentifier)]
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
    pub fn new<S: AsRef<str>, I: IntoType>(name: S, r#type: I) -> Result<Self> {
        let r#type = r#type.into_type()?;
        let mut asset = AbstractAsset::default();
        asset.set_name(name.as_ref());

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

    pub fn builder<S: AsRef<str>, I: IntoType>(name: S, r#type: I) -> Result<ColumnBuilder> {
        Ok(ColumnBuilder::new(Self::new(name, r#type)?))
    }

    pub fn get_type(&self) -> TypePtr {
        self.r#type.clone()
    }

    pub fn get_default(&self) -> &Value {
        &self.default
    }

    pub fn set_default(&mut self, default: Value) -> &mut Self {
        self.default = default;
        self
    }

    pub fn set_comment<T: AsRef<str>, S: Into<Option<T>>>(&mut self, comment: S) -> &mut Self {
        let comment = comment.into();
        self.comment = if let Some(comment) = comment {
            let comment = comment.as_ref();
            if comment.is_empty() {
                None
            } else {
                Some(comment.to_string())
            }
        } else {
            None
        };

        self
    }

    pub fn get_comment(&self) -> &Option<String> {
        &self.comment
    }

    pub fn set_collation<T: AsRef<str>, S: Into<Option<T>>>(&mut self, collation: S) -> &mut Self {
        self.collation = collation.into().map(|c| c.as_ref().to_string());
        self
    }

    pub fn get_collation(&self) -> &Option<String> {
        &self.collation
    }

    pub fn set_charset<T: AsRef<str>, S: Into<Option<T>>>(&mut self, charset: S) -> &mut Self {
        let charset = charset.into();
        self.charset = if let Some(charset) = charset {
            let charset = charset.as_ref();
            if charset.is_empty() {
                None
            } else {
                Some(charset.to_string())
            }
        } else {
            None
        };
        self
    }

    pub fn get_charset(&self) -> &Option<String> {
        &self.charset
    }

    pub fn set_notnull(&mut self, notnull: bool) -> &mut Self {
        self.notnull = notnull;
        self
    }

    pub fn is_notnull(&self) -> bool {
        self.notnull
    }

    pub fn set_autoincrement<T: Into<Option<bool>>>(&mut self, autoincrement: T) -> &mut Self {
        self.autoincrement = autoincrement.into();
        self
    }

    pub fn is_autoincrement(&self) -> bool {
        self.autoincrement.unwrap_or(false)
    }

    pub fn get_column_definition(&self) -> &Option<String> {
        &self.column_definition
    }

    pub fn set_column_definition(&mut self, def: Option<String>) -> &mut Self {
        self.column_definition = def;
        self
    }

    pub fn set_length<S: Into<Option<usize>>>(&mut self, length: S) -> &mut Self {
        self.length = length.into();
        self
    }

    pub fn get_length(&self) -> Option<usize> {
        self.length
    }

    pub fn is_fixed(&self) -> bool {
        self.fixed.unwrap_or(false)
    }

    pub fn set_fixed<I: Into<Option<bool>>>(&mut self, fixed: I) -> &mut Self {
        self.fixed = fixed.into();
        self
    }

    pub fn is_unsigned(&self) -> Option<bool> {
        self.unsigned
    }

    pub fn set_unsigned<I: Into<Option<bool>>>(&mut self, unsigned: I) -> &mut Self {
        self.unsigned = unsigned.into();
        self
    }

    pub fn get_precision(&self) -> Option<usize> {
        self.precision
    }

    pub fn set_precision<I: Into<Option<usize>>>(&mut self, precision: I) -> &mut Self {
        self.precision = precision.into();
        self
    }

    pub fn get_scale(&self) -> Option<usize> {
        self.scale
    }

    pub fn set_scale<I: Into<Option<usize>>>(&mut self, scale: I) -> &mut Self {
        self.scale = scale.into();
        self
    }

    pub fn is_jsonb(&self) -> bool {
        self.jsonb.unwrap_or(false)
    }

    pub fn set_jsonb<I: Into<Option<bool>>>(&mut self, jsonb: I) -> &mut Self {
        self.jsonb = jsonb.into();
        self
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
            fixed: self.is_fixed(),
            unsigned: self.unsigned,
            autoincrement: self.is_autoincrement(),
            column_definition: self.column_definition.clone(),
            version: self.version,
            comment: self.comment.clone(),
            collation: self.collation.clone(),
            charset: self.charset.clone(),
            primary: false,
            check: self.check.clone(),
            jsonb: self.is_jsonb(),
        }
    }
}

impl_asset!(Column, asset);
