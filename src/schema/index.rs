use crate::platform::DatabasePlatform;
use crate::schema::asset::{impl_asset, AbstractAsset, Asset};
use crate::schema::{Identifier, IntoIdentifier, NamedListIndex};
use crate::Value;
use itertools::Itertools;
use std::collections::{HashMap, VecDeque};
use std::slice::Iter;
use std::vec::IntoIter;

#[derive(Clone, Debug, Eq, IntoIdentifier, PartialEq)]
pub struct Index {
    asset: AbstractAsset,
    columns: Vec<Identifier>,
    flags: Vec<String>,
    options: HashMap<String, Value>,
    is_unique: bool,
    is_primary: bool,
    pub r#where: Option<String>,
}

#[derive(Clone, Debug, Default, Eq)]
pub struct IndexList {
    inner: Vec<Index>,
}

impl IndexList {
    pub fn push<T: Into<Index>>(&mut self, index: T) {
        self.inner.push(index.into())
    }

    pub fn remove<T: NamedListIndex>(&mut self, index: T) {
        let pos = if index.is_usize() {
            index.as_usize()
        } else {
            let idx = index.as_str();
            let Some((p, _)) = self.inner.iter().find_position(|p| p.get_name() == idx) else {
                return;
            };

            p
        };

        self.inner.remove(pos);
    }

    pub fn has<T: NamedListIndex>(&self, index: T) -> bool {
        self.get(index).is_some()
    }

    pub fn filter<P>(&self, predicate: P) -> impl Iterator<Item = &Index>
    where
        Self: Sized,
        P: FnMut(&&Index) -> bool,
    {
        self.inner.iter().filter(predicate)
    }

    pub fn get<T: NamedListIndex>(&self, index: T) -> Option<&Index> {
        if index.is_usize() {
            self.inner.get(index.as_usize())
        } else {
            let name = index.as_str().to_lowercase();
            self.inner
                .iter()
                .find(|c| c.get_name().to_lowercase() == name)
        }
    }

    pub fn get_mut<T: NamedListIndex>(&mut self, index: T) -> Option<&mut Index> {
        if index.is_usize() {
            self.inner.get_mut(index.as_usize())
        } else {
            let name = index.as_str().to_lowercase();
            self.inner
                .iter_mut()
                .find(|c| c.get_name().to_lowercase() == name)
        }
    }

    pub fn get_position<T: NamedListIndex>(&self, index: T) -> Option<(usize, &Index)> {
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

    pub fn keys(&self) -> impl Iterator<Item = String> + '_ {
        self.inner.iter().map(|c| c.get_name().into_owned())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn contains(&self, other: &Index) -> bool {
        self.inner.iter().any(|i| i == other)
    }

    pub fn iter(&self) -> Iter<Index> {
        self.into_iter()
    }
}

impl PartialEq for IndexList {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            false
        } else {
            for index in other {
                if !self.contains(index) {
                    return false;
                }
            }

            true
        }
    }
}

impl IntoIterator for IndexList {
    type Item = Index;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a IndexList {
    type Item = &'a Index;
    type IntoIter = Iter<'a, Index>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl From<Vec<Index>> for IndexList {
    fn from(value: Vec<Index>) -> Self {
        Self { inner: value }
    }
}

pub struct IndexBuilder {
    name: Option<String>,
    columns: Vec<Identifier>,
    is_unique: bool,
    is_primary: bool,
    flags: Vec<String>,
    options: HashMap<String, Value>,
}

impl IndexBuilder {
    pub fn add_column<I: IntoIdentifier>(mut self, column: I) -> Self {
        self.columns.push(column.into_identifier());
        self
    }

    pub fn set_unique(mut self, unique: bool) -> Self {
        self.is_unique = unique;
        self
    }

    pub fn set_primary(mut self, primary: bool) -> Self {
        self.is_primary = primary;
        self
    }

    pub fn add_flag<S: AsRef<str>>(mut self, flag: S) -> Self {
        self.flags.push(flag.as_ref().to_string());
        self
    }

    pub fn set_lengths<I: Into<Value>, T: Into<Vec<I>>>(mut self, lengths: T) -> Self {
        let lengths = lengths
            .into()
            .into_iter()
            .map(|v| match v.into() {
                Value::Int(i) => Value::UInt(i as u64),
                val => val,
            })
            .collect::<Vec<_>>();

        self.options
            .insert("lengths".to_string(), Value::Array(lengths));
        self
    }

    pub fn add_option<S: AsRef<str>, V: Into<Value>>(mut self, opt: S, value: V) -> Self {
        let opt = opt.as_ref().to_string();
        let value = value.into();
        self.options.insert(opt, value);
        self
    }
}

impl From<IndexBuilder> for Index {
    fn from(value: IndexBuilder) -> Self {
        Index::new::<String, _, _>(
            value.name,
            value.columns.as_slice(),
            value.is_unique,
            value.is_primary,
            value.flags.as_slice(),
            value.options,
        )
    }
}

impl Index {
    pub fn new<S: AsRef<str>, N: Into<Option<S>>, C: IntoIdentifier>(
        name: N,
        columns: &[C],
        is_unique: bool,
        is_primary: bool,
        flags: &[String],
        options: HashMap<String, Value>,
    ) -> Self {
        let mut asset = AbstractAsset::default();
        let name = if let Some(name) = name.into() {
            name.as_ref().to_string()
        } else {
            "".to_string()
        };

        asset.set_name(&name);
        let mut this = Self {
            asset,
            columns: vec![],
            flags: vec![],
            options,
            is_unique,
            is_primary,
            r#where: None,
        };

        for column in columns {
            this.add_column(column);
        }

        for flag in flags {
            this.add_flag(flag.as_ref());
        }

        this
    }

    /// Create an builder for index.
    pub fn builder<S: AsRef<str>, N: Into<Option<S>>>(name: N) -> IndexBuilder {
        IndexBuilder {
            name: name.into().map(|s| s.as_ref().to_string()),
            columns: vec![],
            is_unique: false,
            is_primary: false,
            flags: vec![],
            options: Default::default(),
        }
    }

    /// Adds flag for an index that translates to platform specific handling.
    pub fn add_flag(&mut self, flag: &str) {
        if !self.has_flag(flag) {
            self.flags.push(flag.to_string());
        }
    }

    /// Does this index have a specific flag?
    pub fn has_flag(&self, flag: &str) -> bool {
        self.flags.iter().any(|f| f.eq(flag))
    }

    pub fn remove_flag(&mut self, flag: &str) {
        for i in 0..self.flags.len() {
            if self.flags.get(i).unwrap().eq(flag) {
                self.flags.remove(i);
            }
        }
    }

    pub fn get_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|c| c.get_name().into_owned())
            .collect()
    }

    pub fn get_quoted_columns(&self, platform: &dyn DatabasePlatform) -> Vec<String> {
        let mut lengths = match self.get_option("lengths") {
            Some(Value::Array(len)) => {
                if platform.supports_column_length_indexes() {
                    VecDeque::from(len.clone())
                } else {
                    VecDeque::default()
                }
            }
            _ => VecDeque::default(),
        };

        self.columns
            .iter()
            .map(|c| {
                let mut n = c.get_quoted_name(platform);
                if let Some(len) = lengths.pop_front() {
                    if len != Value::NULL {
                        n += &format!("({})", len);
                    }
                }

                n
            })
            .collect()
    }

    pub fn get_flags(&self) -> &Vec<String> {
        &self.flags
    }

    pub fn get_unquoted_columns(&self) -> Vec<String> {
        self.get_columns()
            .iter()
            .map(|col| self.trim_quotes(col))
            .collect()
    }

    /// Does this index have a specific option?
    pub fn has_option(&self, name: &str) -> bool {
        if let Some(opt) = self.options.get(name) {
            opt != &Value::NULL
        } else {
            false
        }
    }

    pub fn get_option(&self, name: &str) -> Option<&Value> {
        self.options.get(name)
    }

    pub fn get_options(&self) -> &HashMap<String, Value> {
        &self.options
    }

    /// Adds a new column to the index.
    fn add_column<I: IntoIdentifier>(&mut self, column: &I) {
        let identifier = column.into_identifier();
        if self.columns.iter().all(|i| i != &identifier) {
            self.columns.push(identifier);
        }
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub fn is_unique(&self) -> bool {
        self.is_unique
    }

    /// Checks if this index exactly spans the given column names in the correct order.
    pub(crate) fn spans_columns(&self, column_names: &[String]) -> bool {
        self.columns.iter().enumerate().all(|(index, column)| {
            column_names.get(index).is_some_and(|column_name| {
                self.trim_quotes(&column.get_name().to_lowercase())
                    == self.trim_quotes(&column_name.to_lowercase())
            })
        })
    }

    pub fn is_fulfilled_by(&self, other: &Index) -> bool {
        // allow the other index to be equally large only. It being larger is an option
        // but it creates a problem with scenarios of the kind PRIMARY KEY(foo,bar) UNIQUE(foo)
        if other.get_columns().len() != self.get_columns().len() {
            return false;
        }

        // Check if columns are the same, and even in the same order
        if self.spans_columns(&other.get_columns()) {
            if !self.same_partial_index(other) || !self.has_same_column_lengths(other) {
                return false;
            }

            if !self.is_unique() && !self.is_primary() {
                // this is a special case: If the current key is neither primary or unique, any unique or
                // primary key will always have the same effect for the index and there cannot be any constraint
                // overlaps. This means a primary or unique index can always fulfill the requirements of just an
                // index that has no constraints.
                return true;
            }

            other.is_primary() == self.is_primary() && other.is_unique() == self.is_unique()
        } else {
            false
        }
    }

    /// Return whether the two indexes have the same partial index
    fn same_partial_index(&self, other: &Index) -> bool {
        (self.has_option("where")
            && other.has_option("where")
            && self.get_option("where") == other.get_option("where"))
            || (!self.has_option("where") && !other.has_option("where"))
    }

    /// Returns whether the index has the same column lengths as the other
    fn has_same_column_lengths(&self, other: &Index) -> bool {
        let s_lens = self
            .options
            .get("lengths")
            .cloned()
            .unwrap_or_else(|| Value::Array(vec![]));
        let o_lens = other
            .options
            .get("lengths")
            .cloned()
            .unwrap_or_else(|| Value::Array(vec![]));

        let Ok(s_lens) = s_lens.try_into_vec() else {
            return false;
        };
        let Ok(o_lens) = o_lens.try_into_vec() else {
            return false;
        };

        let s_lens = s_lens
            .into_iter()
            .filter(|v| v != &Value::NULL)
            .collect::<Vec<_>>();
        let o_lens = o_lens
            .into_iter()
            .filter(|v| v != &Value::NULL)
            .collect::<Vec<_>>();

        s_lens == o_lens
    }
}

impl_asset!(Index, asset);

pub struct IndexOptions {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub primary: bool,
    pub flags: Vec<String>,
    pub options_lengths: Vec<Option<usize>>,
    pub options_where: Option<String>,
}

impl IndexOptions {
    pub fn new_index(self) -> Index {
        let lengths = Value::Array(
            self.options_lengths
                .iter()
                .map(|v| match v {
                    Some(s) => {
                        if *s == 0 {
                            Value::NULL
                        } else {
                            Value::from(s)
                        }
                    }
                    None => Value::NULL,
                })
                .collect(),
        );

        let mut options = HashMap::new();
        options.insert("lengths".to_string(), lengths);
        if let Some(val) = self.options_where.as_deref() {
            if !val.is_empty() {
                options.insert("where".to_string(), Value::String(val.to_string()));
            }
        }

        Index::new(
            self.name,
            &self.columns,
            self.unique,
            self.primary,
            &self.flags,
            options,
        )
    }
}
