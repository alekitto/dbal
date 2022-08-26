use crate::platform::DatabasePlatform;
use crc::{Crc, CRC_32_ISO_HDLC};

#[derive(Clone, Default, PartialEq)]
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
pub(super) fn generate_identifier_name(
    column_names: Vec<String>,
    prefix: &str,
    max_size: Option<usize>,
) -> String {
    let max_size = max_size.unwrap_or(30);
    let hash = column_names
        .iter()
        .map(|name| {
            let hash = Crc::<u32>::new(&CRC_32_ISO_HDLC);
            let mut digest = hash.digest();
            digest.update(name.as_bytes());
            format!("{:X}", digest.finalize())
        })
        .collect::<Vec<String>>()
        .join("");

    let mut identifier = format!("{}_{}", prefix, hash);
    identifier.truncate(max_size);

    identifier.to_uppercase()
}

pub trait Asset {
    /// Returns the name of this schema asset.
    fn get_name(&self) -> String;

    /// Sets the name of this asset.
    fn set_name(&mut self, name: String);

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
        identifier
            .to_string()
            .replace('`', "")
            .replace('"', "")
            .replace('[', "")
            .replace(']', "")
    }

    /// The shortest name is stripped of the default namespace. All other
    /// namespaced elements are returned as full-qualified names.
    fn get_shortest_name(&self, default_namespace_name: &str) -> String;

    /// Checks if this asset's name is quoted.
    fn is_quoted(&self) -> bool;

    /// Gets the quoted representation of this asset but only if it was defined with one. Otherwise
    /// return the plain unquoted value as inserted.
    fn get_quoted_name<T: DatabasePlatform + ?Sized>(&self, platform: &T) -> String {
        let keywords = platform.create_reserved_keywords_list();
        self.get_name()
            .split('.')
            .into_iter()
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

impl Asset for AbstractAsset {
    fn get_name(&self) -> String {
        if let Some(namespace) = self.namespace.clone() {
            format!("{}.{}", namespace, &self.name)
        } else {
            self.name.clone()
        }
    }

    fn set_name(&mut self, name: String) {
        let name = if self.is_identifier_quoted(&name) {
            self.quoted = true;
            self.trim_quotes(&name)
        } else {
            self.quoted = false;
            name
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
