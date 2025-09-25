use crate::platform::DatabasePlatform;
use crate::schema::{Identifier, IntoIdentifier};
use crc::{CRC_32_ISO_HDLC, Crc};
use std::borrow::Cow;

#[derive(Clone, Debug, Default, Eq, IntoIdentifier, PartialEq)]
pub(crate) struct AbstractAsset {
    quoted: bool,
    namespace: Option<String>,
    name: String,
}

/// Generates an identifier from a list of column names obeying a certain string length.
///
/// This is especially important for Oracle, since it does not allow identifiers larger than 30 chars,
/// however building idents automatically for foreign keys, composite keys or such can easily create
/// very long names.
pub(super) fn generate_identifier_name<S: AsRef<str>, U: Into<Option<usize>>>(
    column_names: &[S],
    prefix: &str,
    max_size: U,
) -> String {
    let max_size = max_size.into().unwrap_or(30);
    let hash = column_names
        .iter()
        .map(|name| {
            let hash = Crc::<u32>::new(&CRC_32_ISO_HDLC);
            let mut digest = hash.digest();
            digest.update(name.as_ref().as_bytes());
            format!("{:X}", digest.finalize())
        })
        .collect::<Vec<String>>()
        .join("");

    let mut identifier = format!("{}_{}", prefix, hash);
    identifier.truncate(max_size);

    identifier.to_uppercase()
}

pub trait Asset: IntoIdentifier {
    /// Returns the name of this schema asset.
    fn get_name(&self) -> Cow<'_, str>;

    /// Sets the name of this asset.
    fn set_name(&mut self, name: &str);

    /// Whether the name of this asset is empty
    fn is_empty(&self) -> bool {
        self.get_name().is_empty()
    }

    /// Is this asset in the default namespace?
    fn is_in_default_namespace(&self, default_namespace_name: &str) -> bool {
        self.get_namespace_name()
            .as_ref()
            .map(|v| v.eq(default_namespace_name))
            .unwrap_or(true)
    }

    /// Gets the namespace name of this asset.
    /// If None is returned this means the default namespace is used.
    fn get_namespace_name(&self) -> Option<String>;

    /// Checks if this identifier is quoted.
    fn is_identifier_quoted(&self, identifier: &str) -> bool {
        if identifier.is_empty() {
            false
        } else {
            let c: char = identifier.chars().next().unwrap();
            c == '`' || c == '"' || c == '['
        }
    }

    /// Trim quotes from the identifier.
    fn trim_quotes(&self, identifier: &str) -> String {
        identifier.to_string().replace(['`', '"', '[', ']'], "")
    }

    /// The shortest name is stripped of the default namespace. All other
    /// namespaced elements are returned as full-qualified names.
    fn get_shortest_name(&self, default_namespace_name: &str) -> String;

    /// Checks if this asset's name is quoted.
    fn is_quoted(&self) -> bool;

    /// Gets the quoted representation of this asset but only if it was defined with one. Otherwise
    /// return the plain unquoted value as inserted.
    fn get_quoted_name(&self, platform: &dyn DatabasePlatform) -> String {
        let keywords = platform.create_reserved_keywords_list();
        self.get_name()
            .split('.')
            .map(|v| {
                if self.is_quoted() || keywords.is_keyword(v) {
                    platform.quote_identifier(v)
                } else {
                    v.to_string()
                }
            })
            .collect::<Vec<String>>()
            .join(".")
    }
}

impl<A: Asset + ?Sized> IntoIdentifier for &mut A {
    delegate::delegate! {
        to(**self) {
            fn into_identifier(&self) -> Identifier;
        }
    }
}

impl<A: Asset + ?Sized> Asset for &mut A {
    delegate::delegate! {
        to(**self) {
            fn get_name(&self) -> Cow<'_, str>;
            fn set_name(&mut self, name: &str);
            fn is_empty(&self) -> bool;
            fn is_in_default_namespace(&self, default_namespace_name: &str) -> bool;
            fn get_namespace_name(&self) -> Option<String>;
            fn is_identifier_quoted(&self, identifier: &str) -> bool;
            fn trim_quotes(&self, identifier: &str) -> String;
            fn get_shortest_name(&self, default_namespace_name: &str) -> String;
            fn is_quoted(&self) -> bool;
            fn get_quoted_name(&self, platform: &dyn DatabasePlatform) -> String;
        }
    }
}

impl<A: Asset + ?Sized> IntoIdentifier for Box<A> {
    delegate::delegate! {
        to(**self) {
            fn into_identifier(&self) -> Identifier;
        }
    }
}

impl<A: Asset + ?Sized> Asset for Box<A> {
    delegate::delegate! {
        to(**self) {
            fn get_name(&self) -> Cow<'_, str>;
            fn set_name(&mut self, name: &str);
            fn is_empty(&self) -> bool;
            fn is_in_default_namespace(&self, default_namespace_name: &str) -> bool;
            fn get_namespace_name(&self) -> Option<String>;
            fn is_identifier_quoted(&self, identifier: &str) -> bool;
            fn trim_quotes(&self, identifier: &str) -> String;
            fn get_shortest_name(&self, default_namespace_name: &str) -> String;
            fn is_quoted(&self) -> bool;
            fn get_quoted_name(&self, platform: &dyn DatabasePlatform) -> String;
        }
    }
}

pub(crate) macro impl_asset($t:ident,$e:ident) {
    impl crate::schema::Asset for $t {
        delegate::delegate! {
            to(self.$e) {
                fn get_name(&self) -> Cow<'_, str>;
                fn set_name(&mut self, name: &str);
                fn is_empty(&self) -> bool;
                fn is_in_default_namespace(&self, default_namespace_name: &str) -> bool;
                fn get_namespace_name(&self) -> Option<String>;
                fn is_identifier_quoted(&self, identifier: &str) -> bool;
                fn trim_quotes(&self, identifier: &str) -> String;
                fn get_shortest_name(&self, default_namespace_name: &str) -> String;
                fn is_quoted(&self) -> bool;
                fn get_quoted_name(&self, platform: &dyn DatabasePlatform) -> String;
            }
        }
    }
}

impl Asset for AbstractAsset {
    fn get_name(&self) -> Cow<'_, str> {
        if let Some(namespace) = self.namespace.clone() {
            Cow::Owned(format!("{}.{}", namespace, &self.name))
        } else {
            Cow::Borrowed(&self.name)
        }
    }

    fn set_name(&mut self, name: &str) {
        let name = if self.is_identifier_quoted(name) {
            self.quoted = true;
            self.trim_quotes(name)
        } else {
            self.quoted = false;
            name.to_string()
        };

        if name.contains('.') {
            let parts: Vec<&str> = name.split('.').collect();
            self.namespace = parts.first().map(|s| s.to_string());
            self.name = parts.get(1).map(|s| s.to_string()).unwrap();
        } else {
            self.namespace = None;
            self.name = name
        }
    }

    fn get_namespace_name(&self) -> Option<String> {
        self.namespace.clone()
    }

    fn get_shortest_name(&self, default_namespace_name: &str) -> String {
        if self.is_in_default_namespace(default_namespace_name) {
            self.name.clone()
        } else {
            self.get_name().to_lowercase()
        }
    }

    fn is_quoted(&self) -> bool {
        self.quoted
    }
}
