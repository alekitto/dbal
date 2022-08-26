mod bigint_type;
mod binary_type;
mod blob_type;
mod boolean_type;
mod date_type;
mod datetime_type;
mod datetime_tz_type;
mod decimal_type;
mod float_type;
mod integer_type;
mod json_type;
mod simple_array_type;
mod string_type;
mod text_type;
mod time_type;

use crate::error::ErrorKind;
use crate::platform::DatabasePlatform;
use crate::schema::ColumnData;
use crate::{Error, ParameterType, Result, Value};
pub use bigint_type::BigintType;
pub use binary_type::BinaryType;
pub use blob_type::BlobType;
pub use boolean_type::BooleanType;
use dashmap::DashMap;
pub use date_type::DateType;
pub use datetime_type::DateTimeType;
pub use datetime_tz_type::DateTimeTzType;
pub use decimal_type::DecimalType;
use delegate::delegate;
pub use float_type::FloatType;
pub use integer_type::IntegerType;
pub use json_type::JsonType;
use lazy_static::lazy_static;
pub use simple_array_type::SimpleArrayType;
use std::any::TypeId;
use std::sync::Arc;
pub use string_type::StringType;
pub use text_type::TextType;
pub use time_type::TimeType;

pub const BIGINT: &str = "binary";
pub const BINARY: &str = "binary";
pub const BLOB: &str = "blob";
pub const BOOLEAN: &str = "boolean";
pub const DATE: &str = "date";
pub const DATETIME: &str = "datetime";
pub const DATETIMETZ: &str = "datetimetz";
pub const DECIMAL: &str = "decimal";
pub const FLOAT: &str = "float";
pub const INTEGER: &str = "integer";
pub const JSON: &str = "json";
pub const SIMPLE_ARRAY: &str = "simple_array";
pub const STRING: &str = "string";
pub const TEXT: &str = "text";
pub const TIME: &str = "time";

pub trait Type {
    fn default() -> Box<dyn Type + Sync + Send>
    where
        Self: Sized;

    /// Converts a value from its Value representation to its database representation of this type.
    #[allow(unused_variables)]
    fn convert_to_database_value(
        &self,
        value: Value,
        platform: &dyn DatabasePlatform,
    ) -> Result<Value> {
        Ok(value)
    }

    /// Converts a value from its database representation to its PHP representation
    /// of this type.
    #[allow(unused_variables)]
    fn convert_to_value(&self, value: &Value, platform: &dyn DatabasePlatform) -> Result<Value> {
        Ok(value.clone())
    }

    fn get_name(&self) -> &'static str;

    #[allow(unused_variables)]
    fn requires_sql_comment_hint(&self, platform: &dyn DatabasePlatform) -> bool {
        false
    }

    fn get_sql_declaration(
        &self,
        column: &ColumnData,
        platform: &dyn DatabasePlatform,
    ) -> Result<String>;

    fn get_binding_type(&self) -> ParameterType {
        ParameterType::String
    }

    /// Gets an array of database types that map to this Doctrine type.
    #[allow(unused_variables)]
    fn get_mapped_database_types(&self, platform: &dyn DatabasePlatform) -> Vec<String> {
        vec![]
    }
}

pub trait IntoType {
    fn into_type(self) -> Result<Arc<Box<dyn Type + Send + Sync>>>;
}

impl IntoType for &str {
    fn into_type(self) -> Result<Arc<Box<dyn Type + Send + Sync>>> {
        TypeManager::get_instance().get_type_by_name(self)
    }
}

impl IntoType for TypeId {
    fn into_type(self) -> Result<Arc<Box<dyn Type + Send + Sync>>> {
        TypeManager::get_instance().get_type(self)
    }
}

impl IntoType for Arc<Box<dyn Type + Send + Sync>> {
    fn into_type(self) -> Result<Arc<Box<dyn Type + Send + Sync>>> {
        Ok(self)
    }
}

impl<T: Type + ?Sized> Type for Box<T> {
    fn default() -> Box<dyn Type + Sync + Send>
    where
        Self: Sized,
    {
        unreachable!()
    }

    delegate! {
        to (**self) {
            fn convert_to_database_value(&self, value: Value, platform: &dyn DatabasePlatform)-> Result<Value>;
            fn convert_to_value(&self, value: &Value, platform: &dyn DatabasePlatform) -> Result<Value>;
            fn get_name(&self) -> &'static str;
            fn requires_sql_comment_hint(&self, platform: &dyn DatabasePlatform) -> bool;
            fn get_sql_declaration(&self, column: &ColumnData, platform: &dyn DatabasePlatform) -> Result<String>;
            fn get_binding_type(&self) -> ParameterType;
            fn get_mapped_database_types(&self, platform: &dyn DatabasePlatform) -> Vec<String>;
        }
    }
}

pub struct TypeManager {
    type_map: DashMap<TypeId, Arc<Box<dyn Type + Sync + Send>>>,
}

lazy_static! {
    static ref TYPE_MANAGER_INSTANCE: TypeManager = TypeManager::new();
}

impl TypeManager {
    fn new() -> Self {
        let type_map = DashMap::new();
        type_map.insert(TypeId::of::<BigintType>(), Arc::new(BigintType::default()));
        type_map.insert(TypeId::of::<BinaryType>(), Arc::new(BinaryType::default()));
        type_map.insert(TypeId::of::<BlobType>(), Arc::new(BlobType::default()));
        type_map.insert(
            TypeId::of::<BooleanType>(),
            Arc::new(BooleanType::default()),
        );
        type_map.insert(TypeId::of::<DateType>(), Arc::new(DateType::default()));
        type_map.insert(
            TypeId::of::<DateTimeType>(),
            Arc::new(DateTimeType::default()),
        );
        type_map.insert(
            TypeId::of::<DateTimeTzType>(),
            Arc::new(DateTimeTzType::default()),
        );
        type_map.insert(
            TypeId::of::<DecimalType>(),
            Arc::new(DecimalType::default()),
        );
        type_map.insert(TypeId::of::<FloatType>(), Arc::new(FloatType::default()));
        type_map.insert(
            TypeId::of::<IntegerType>(),
            Arc::new(IntegerType::default()),
        );
        type_map.insert(TypeId::of::<JsonType>(), Arc::new(JsonType::default()));
        type_map.insert(
            TypeId::of::<SimpleArrayType>(),
            Arc::new(SimpleArrayType::default()),
        );
        type_map.insert(TypeId::of::<StringType>(), Arc::new(StringType::default()));
        type_map.insert(TypeId::of::<TextType>(), Arc::new(TextType::default()));
        type_map.insert(TypeId::of::<TimeType>(), Arc::new(TimeType::default()));

        Self { type_map }
    }

    // pub fn register(&self, )

    pub fn get_instance() -> &'static Self {
        &TYPE_MANAGER_INSTANCE
    }

    pub fn get_type_by_name(&self, type_name: &str) -> Result<Arc<Box<dyn Type + Sync + Send>>> {
        self.type_map
            .iter()
            .find(|t| t.get_name() == type_name)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::new(ErrorKind::UnknownType, format!("You have requested a non-existent type {}. Please register it in the type manager before trying to use it", type_name)))
    }

    pub fn get_type(&self, r#type: TypeId) -> Result<Arc<Box<dyn Type + Sync + Send>>> {
        self.type_map
            .get(&r#type)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::unknown_type(r#type))
    }

    pub fn get_types(&self) -> Result<Vec<TypeId>> {
        Ok(self.type_map.iter().map(|t| *t.key()).collect())
    }
}
