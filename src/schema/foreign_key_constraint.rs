use super::asset::Asset;
use crate::platform::DatabasePlatform;
use crate::schema::asset::{impl_asset, AbstractAsset};
use crate::schema::{Identifier, Index, IntoIdentifier, NamedListIndex};
use crate::Value;
use itertools::Itertools;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::slice::Iter;
use std::vec::IntoIter;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForeignKeyReferentialAction {
    Cascade,
    SetNull,
    NoAction,
    Restrict,
    SetDefault,
}

#[derive(Clone, Debug, IntoIdentifier)]
pub struct ForeignKeyConstraint {
    asset: AbstractAsset,
    local_columns: Vec<Identifier>,
    foreign_columns: Vec<Identifier>,
    foreign_table: Identifier,
    options: HashMap<String, Value>,
    pub on_update: Option<ForeignKeyReferentialAction>,
    pub on_delete: Option<ForeignKeyReferentialAction>,
}

pub struct ForeignKeyConstraintBuilder {
    name: Option<String>,
    local_columns: Vec<Identifier>,
    foreign_columns: Vec<Identifier>,
    foreign_table: Identifier,
    options: HashMap<String, Value>,
    on_update: Option<ForeignKeyReferentialAction>,
    on_delete: Option<ForeignKeyReferentialAction>,
}

impl ForeignKeyConstraintBuilder {
    pub fn set_name<S: AsRef<str>>(mut self, name: S) -> Self {
        let name = name.as_ref();
        self.name = if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        };

        self
    }

    pub fn set_local_columns<I: IntoIdentifier>(mut self, cols: Vec<I>) -> Self {
        self.local_columns = cols.iter().map(|c| c.into_identifier()).collect();
        self
    }

    pub fn set_foreign_columns<I: IntoIdentifier>(mut self, cols: Vec<I>) -> Self {
        self.foreign_columns = cols.iter().map(|c| c.into_identifier()).collect();
        self
    }

    pub fn add_option<S: AsRef<str>, V: Into<Value>>(mut self, opt: S, value: V) -> Self {
        let opt = opt.as_ref().to_string();
        let value = value.into();
        self.options.insert(opt, value);
        self
    }

    pub fn set_on_update_action<T: Into<Option<ForeignKeyReferentialAction>>>(
        mut self,
        action: T,
    ) -> Self {
        self.on_update = action.into();
        self
    }

    pub fn set_on_delete_action<T: Into<Option<ForeignKeyReferentialAction>>>(
        mut self,
        action: T,
    ) -> Self {
        self.on_delete = action.into();
        self
    }
}

impl From<ForeignKeyConstraintBuilder> for ForeignKeyConstraint {
    fn from(value: ForeignKeyConstraintBuilder) -> Self {
        let mut asset = AbstractAsset::default();
        if let Some(name) = value.name {
            asset.set_name(name.as_str());
        }

        Self {
            asset,
            local_columns: value.local_columns,
            foreign_columns: value.foreign_columns,
            foreign_table: value.foreign_table,
            options: value.options,
            on_update: value.on_update,
            on_delete: value.on_delete,
        }
    }
}

impl ForeignKeyConstraint {
    pub fn new<LC, FC, FT>(
        local_columns: &[LC],
        foreign_columns: &[FC],
        foreign_table: FT,
        options: HashMap<String, Value>,
        on_update: Option<ForeignKeyReferentialAction>,
        on_delete: Option<ForeignKeyReferentialAction>,
    ) -> Self
    where
        LC: IntoIdentifier,
        FC: IntoIdentifier,
        FT: IntoIdentifier,
    {
        let local_columns = local_columns.iter().map(|c| c.into_identifier()).collect();
        let foreign_columns = foreign_columns
            .iter()
            .map(|c| c.into_identifier())
            .collect();
        let foreign_table = foreign_table.into_identifier();

        Self {
            asset: AbstractAsset::default(),
            local_columns,
            foreign_columns,
            foreign_table,
            options,
            on_update,
            on_delete,
        }
    }

    pub fn builder<T: IntoIdentifier>(foreign_table: T) -> ForeignKeyConstraintBuilder {
        ForeignKeyConstraintBuilder {
            name: None,
            local_columns: vec![],
            foreign_columns: vec![],
            foreign_table: foreign_table.into_identifier(),
            options: Default::default(),
            on_update: None,
            on_delete: None,
        }
    }

    pub fn get_local_columns(&self) -> &Vec<Identifier> {
        &self.local_columns
    }

    pub fn get_quoted_local_columns(&self, platform: &dyn DatabasePlatform) -> Vec<String> {
        self.local_columns
            .iter()
            .map(|c| c.get_quoted_name(platform))
            .collect()
    }

    pub fn get_unquoted_local_columns(&self) -> Vec<String> {
        self.local_columns
            .iter()
            .map(|c| c.get_name().into_owned())
            .collect()
    }

    pub fn get_foreign_columns(&self) -> &Vec<Identifier> {
        &self.foreign_columns
    }

    pub fn get_quoted_foreign_columns(&self, platform: &dyn DatabasePlatform) -> Vec<String> {
        self.foreign_columns
            .iter()
            .map(|c| c.get_quoted_name(platform))
            .collect()
    }

    pub fn get_unquoted_foreign_columns(&self) -> Vec<String> {
        self.foreign_columns
            .iter()
            .map(|c| c.get_name().into_owned())
            .collect()
    }

    pub fn get_foreign_table(&self) -> &Identifier {
        &self.foreign_table
    }

    pub fn get_quoted_foreign_table_name(&self, platform: &dyn DatabasePlatform) -> String {
        self.foreign_table.get_quoted_name(platform)
    }

    pub fn get_options(&self) -> HashMap<String, Value> {
        self.options.clone()
    }

    pub fn get_option(&self, option: &str) -> Option<&Value> {
        self.options.get(option)
    }

    /// Returns the non-schema qualified foreign table name.
    pub fn get_unqualified_foreign_table_name(&self) -> String {
        let name = self.foreign_table.get_name().to_lowercase();
        if let Some(pos) = name.rfind('.') {
            name.split_at(pos + 1).1.to_string()
        } else {
            name
        }
    }

    /// Checks whether this foreign key constraint intersects the given index columns.
    /// Returns `true` if at least one of this foreign key's local columns
    /// matches one of the given index's columns, `false` otherwise.
    pub fn intersects_index_columns(&self, index: &Index) -> bool {
        for index_column in index.get_columns() {
            for local_column in self.local_columns.iter().map(|c| c.get_name()) {
                if local_column.to_lowercase() == index_column.to_lowercase() {
                    return true;
                }
            }
        }

        false
    }
}

fn lowercase_vec<T: Borrow<str>>(v: Vec<T>) -> Vec<String> {
    v.iter().map(|c| c.borrow().to_lowercase()).collect()
}

impl Eq for ForeignKeyConstraint {}
impl PartialEq for ForeignKeyConstraint {
    fn eq(&self, other: &Self) -> bool {
        lowercase_vec(self.get_unquoted_local_columns())
            == lowercase_vec(other.get_unquoted_local_columns())
            && lowercase_vec(self.get_unquoted_foreign_columns())
                == lowercase_vec(other.get_unquoted_foreign_columns())
            && self.get_unqualified_foreign_table_name()
                == other.get_unqualified_foreign_table_name()
            && self.on_update == other.on_update
            && self.on_delete == other.on_delete
    }
}

impl_asset!(ForeignKeyConstraint, asset);

#[derive(Clone, Debug, Default, Eq)]
pub struct FKConstraintList {
    inner: Vec<ForeignKeyConstraint>,
}

impl FKConstraintList {
    pub fn push<T: Into<ForeignKeyConstraint>>(&mut self, constraint: T) {
        self.inner.push(constraint.into())
    }

    pub fn has<T: NamedListIndex>(&self, constraint: T) -> bool {
        self.get(constraint).is_some()
    }

    pub fn filter<P>(&self, predicate: P) -> impl Iterator<Item = &ForeignKeyConstraint>
    where
        Self: Sized,
        P: FnMut(&&ForeignKeyConstraint) -> bool,
    {
        self.inner.iter().filter(predicate)
    }

    pub fn get<T: NamedListIndex>(&self, index: T) -> Option<&ForeignKeyConstraint> {
        if index.is_usize() {
            self.inner.get(index.as_usize())
        } else {
            let name = index.as_str().to_lowercase();
            self.inner
                .iter()
                .find(|c| c.get_name().to_lowercase() == name)
        }
    }

    pub fn get_mut<T: NamedListIndex>(&mut self, index: T) -> Option<&mut ForeignKeyConstraint> {
        if index.is_usize() {
            self.inner.get_mut(index.as_usize())
        } else {
            let name = index.as_str().to_lowercase();
            self.inner
                .iter_mut()
                .find(|c| c.get_name().to_lowercase() == name)
        }
    }

    pub fn get_position<T: NamedListIndex>(
        &self,
        index: T,
    ) -> Option<(usize, &ForeignKeyConstraint)> {
        if index.is_usize() {
            let idx = index.as_usize();
            self.inner.get(idx).map(|i| (idx, i))
        } else {
            let name = index.as_str().to_lowercase();
            self.inner
                .iter()
                .find_position(|c| c.get_name().to_lowercase() == name)
        }
    }

    pub fn remove<T: NamedListIndex>(&mut self, index: T) -> Option<ForeignKeyConstraint> {
        let Some((pos, _)) = self.get_position(index) else {
            return None;
        };

        Some(self.inner.remove(pos))
    }

    pub fn keys(&self) -> impl Iterator<Item = String> + '_ {
        self.inner.iter().map(|c| c.get_name().into_owned())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn contains(&self, other: &ForeignKeyConstraint) -> bool {
        self.inner.iter().any(|i| i == other)
    }

    pub fn iter(&self) -> Iter<ForeignKeyConstraint> {
        self.into_iter()
    }
}

impl PartialEq for FKConstraintList {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            false
        } else {
            for fk in other {
                if !self.contains(fk) {
                    return false;
                }
            }

            true
        }
    }
}

impl IntoIterator for FKConstraintList {
    type Item = ForeignKeyConstraint;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a FKConstraintList {
    type Item = &'a ForeignKeyConstraint;
    type IntoIter = Iter<'a, ForeignKeyConstraint>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl From<Vec<ForeignKeyConstraint>> for FKConstraintList {
    fn from(value: Vec<ForeignKeyConstraint>) -> Self {
        Self { inner: value }
    }
}
