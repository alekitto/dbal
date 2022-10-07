mod bigint_type;
mod binary_type;
mod blob_type;
mod boolean_type;
mod date_type;
mod datetime_type;
mod datetime_tz_type;
mod decimal_type;
mod float_type;
mod guid_type;
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
pub use guid_type::GuidType;
pub use integer_type::IntegerType;
pub use json_type::JsonType;
use lazy_static::lazy_static;
pub use simple_array_type::SimpleArrayType;
use std::any::{type_name, TypeId};
use std::fmt::{Debug, Formatter};
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
pub const GUID: &str = "guid";
pub const INTEGER: &str = "integer";
pub const JSON: &str = "json";
pub const SIMPLE_ARRAY: &str = "simple_array";
pub const STRING: &str = "string";
pub const TEXT: &str = "text";
pub const TIME: &str = "time";

pub trait AsTypeId {
    fn type_id(&self) -> TypeId;
}

#[derive(Clone)]
pub struct TypePtr {
    t: Arc<Box<dyn Type + Send + Sync>>,
    type_id: TypeId,
    type_name: &'static str,
}

impl Debug for TypePtr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypePtr")
            .field("type_id", &self.type_id)
            .field("type_name", &self.type_name)
            .finish()
    }
}

impl PartialEq for TypePtr {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
    }
}

impl TypePtr {
    fn new<T: Type + Send + Sync + 'static>() -> Self {
        Self {
            t: Arc::new(T::default()),
            type_id: TypeId::of::<T>(),
            type_name: type_name::<T>(),
        }
    }

    delegate::delegate! {
        to(**(self.t)) {
            pub fn convert_to_database_value(
                &self,
                value: Value,
                platform: &dyn DatabasePlatform,
            ) -> Result<Value>;
            pub fn convert_to_value(&self, value: &Value, platform: &dyn DatabasePlatform) -> Result<Value>;
            pub fn get_name(&self) -> &'static str;
            pub fn requires_sql_comment_hint(&self, platform: &dyn DatabasePlatform) -> bool;
            pub fn get_sql_declaration(
                &self,
                column: &ColumnData,
                platform: &dyn DatabasePlatform,
            ) -> Result<String>;
            pub fn get_binding_type(&self) -> ParameterType;
            pub fn get_mapped_database_types(&self, platform: &dyn DatabasePlatform) -> Vec<String>;
        }
    }
}

impl AsTypeId for TypePtr {
    fn type_id(&self) -> TypeId {
        self.type_id
    }
}

impl AsTypeId for TypeId {
    fn type_id(&self) -> TypeId {
        *self
    }
}

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
    fn into_type(self) -> Result<TypePtr>;
}

impl IntoType for &str {
    fn into_type(self) -> Result<TypePtr> {
        TypeManager::get_instance().get_type_by_name(self)
    }
}

impl IntoType for &String {
    fn into_type(self) -> Result<TypePtr> {
        TypeManager::get_instance().get_type_by_name(self)
    }
}

impl IntoType for TypeId {
    fn into_type(self) -> Result<TypePtr> {
        TypeManager::get_instance().get_type(self)
    }
}

impl<T: Type + Send + Sync + 'static> IntoType for T {
    fn into_type(self) -> Result<TypePtr> {
        TypeManager::get_instance().get_type(TypePtr::new::<Self>())
    }
}

impl IntoType for TypePtr {
    fn into_type(self) -> Result<TypePtr> {
        Ok(self)
    }
}

impl<T: Type + ?Sized> Type for &mut T {
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
    type_map: DashMap<TypeId, TypePtr>,
}

lazy_static! {
    static ref TYPE_MANAGER_INSTANCE: TypeManager = TypeManager::new();
}

impl TypeManager {
    fn new() -> Self {
        let type_map = DashMap::new();
        type_map.insert(TypeId::of::<BigintType>(), TypePtr::new::<BigintType>());
        type_map.insert(TypeId::of::<BinaryType>(), TypePtr::new::<BinaryType>());
        type_map.insert(TypeId::of::<BlobType>(), TypePtr::new::<BlobType>());
        type_map.insert(TypeId::of::<BooleanType>(), TypePtr::new::<BooleanType>());
        type_map.insert(TypeId::of::<DateType>(), TypePtr::new::<DateType>());
        type_map.insert(TypeId::of::<DateTimeType>(), TypePtr::new::<DateTimeType>());
        type_map.insert(
            TypeId::of::<DateTimeTzType>(),
            TypePtr::new::<DateTimeTzType>(),
        );
        type_map.insert(TypeId::of::<DecimalType>(), TypePtr::new::<DecimalType>());
        type_map.insert(TypeId::of::<FloatType>(), TypePtr::new::<FloatType>());
        type_map.insert(TypeId::of::<GuidType>(), TypePtr::new::<GuidType>());
        type_map.insert(TypeId::of::<IntegerType>(), TypePtr::new::<IntegerType>());
        type_map.insert(TypeId::of::<JsonType>(), TypePtr::new::<JsonType>());
        type_map.insert(
            TypeId::of::<SimpleArrayType>(),
            TypePtr::new::<SimpleArrayType>(),
        );
        type_map.insert(TypeId::of::<StringType>(), TypePtr::new::<StringType>());
        type_map.insert(TypeId::of::<TextType>(), TypePtr::new::<TextType>());
        type_map.insert(TypeId::of::<TimeType>(), TypePtr::new::<TimeType>());

        Self { type_map }
    }

    pub fn register<T: Type + Send + Sync + 'static>(&self) {
        self.type_map.insert(TypeId::of::<T>(), TypePtr::new::<T>());
    }

    pub fn get_instance() -> &'static Self {
        &TYPE_MANAGER_INSTANCE
    }

    pub fn get_type_by_name(&self, type_name: &str) -> Result<TypePtr> {
        self.type_map
            .iter()
            .find(|t| t.get_name() == type_name)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::new(ErrorKind::UnknownType, format!("You have requested a non-existent type {}. Please register it in the type manager before trying to use it", type_name)))
    }

    pub fn get_type<T: AsTypeId>(&self, r#type: T) -> Result<TypePtr> {
        let type_id = r#type.type_id();
        self.type_map
            .get(&type_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::unknown_type(type_id))
    }

    pub fn get_types(&self) -> Result<Vec<TypeId>> {
        Ok(self.type_map.iter().map(|t| *t.key()).collect())
    }
}
