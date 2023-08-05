mod asset;
mod check_constraint;
mod column;
mod column_diff;
mod comparator;
mod foreign_key_constraint;
mod identifier;
mod index;
mod schema;
mod schema_config;
mod schema_diff;
mod schema_manager;
mod sequence;
mod table;
mod table_diff;
mod unique_constraint;
mod view;

pub(crate) use asset::Asset;
pub(crate) use column::ColumnData;
use std::borrow::Cow;
pub(crate) use table::TableOptions;

pub use check_constraint::CheckConstraint;
pub use column::{Column, ColumnList};
pub use column_diff::{ChangedProperty, ColumnDiff};
pub use comparator::{diff_column, Comparator, GenericComparator};
pub use foreign_key_constraint::{
    FKConstraintList, ForeignKeyConstraint, ForeignKeyReferentialAction,
};
pub use identifier::{Identifier, IntoIdentifier};
pub use index::{Index, IndexList, IndexOptions};
pub use schema::Schema;
pub use schema_diff::SchemaDiff;
pub use schema_manager::{extract_type_from_comment, remove_type_from_comment, SchemaManager};
pub(crate) use schema_manager::{get_database, string_from_value};
pub use sequence::Sequence;
pub use table::{Table, TableList};
pub use table_diff::TableDiff;
pub use unique_constraint::UniqueConstraint;
pub use view::View;

pub use ::creed_derive::IntoIdentifier;

pub trait NamedListIndex {
    fn is_usize(&self) -> bool;
    fn as_usize(&self) -> usize;
    fn as_str(&self) -> Cow<str>;
}

impl NamedListIndex for usize {
    fn is_usize(&self) -> bool {
        true
    }

    fn as_usize(&self) -> usize {
        *self
    }

    fn as_str(&self) -> Cow<str> {
        "".into()
    }
}

impl NamedListIndex for &str {
    fn is_usize(&self) -> bool {
        false
    }

    fn as_usize(&self) -> usize {
        0
    }

    fn as_str(&self) -> Cow<str> {
        self.to_string().into()
    }
}

impl NamedListIndex for dyn AsRef<str> {
    fn is_usize(&self) -> bool {
        false
    }

    fn as_usize(&self) -> usize {
        0
    }

    fn as_str(&self) -> Cow<str> {
        self.as_ref().into()
    }
}

impl NamedListIndex for Identifier {
    fn is_usize(&self) -> bool {
        false
    }

    fn as_usize(&self) -> usize {
        0
    }

    fn as_str(&self) -> Cow<str> {
        self.get_name()
    }
}

impl NamedListIndex for String {
    fn is_usize(&self) -> bool {
        false
    }

    fn as_usize(&self) -> usize {
        0
    }

    fn as_str(&self) -> Cow<str> {
        self.into()
    }
}
