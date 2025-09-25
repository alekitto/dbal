use std::collections::BTreeMap;

#[cfg(feature = "mysql")]
mod mariadb_keywords;
#[cfg(feature = "mysql")]
mod mysql80_keywords;
#[cfg(feature = "mysql")]
mod mysql_keywords;
#[cfg(feature = "postgres")]
mod postgresql_keywords;
#[cfg(feature = "sqlite")]
mod sqlite_keywords;

pub trait Keywords {
    /// Returns the name of this keyword list.
    fn get_name(&self) -> &'static str;

    /// Returns the list of keywords.
    fn get_keywords(&self) -> &[&'static str];
}

pub struct KeywordList {
    keyword_map: BTreeMap<String, &'static str>,
}

impl KeywordList {
    pub(crate) fn new(keywords: &'static dyn Keywords) -> Self {
        let mut keyword_map = BTreeMap::new();
        for keyword in keywords.get_keywords() {
            keyword_map.insert(keyword.to_uppercase(), *keyword);
        }

        Self { keyword_map }
    }

    /// Checks if the given word is a keyword of this dialect/vendor platform.
    pub fn is_keyword(&self, word: &str) -> bool {
        self.keyword_map.contains_key(&word.to_uppercase())
    }

    #[cfg(feature = "mysql")]
    pub fn mariadb_keywords() -> Self {
        Self::new(&mariadb_keywords::MARIADB_KEYWORDS)
    }

    #[cfg(feature = "mysql")]
    pub fn mysql_keywords() -> Self {
        Self::new(&mysql_keywords::MYSQL_KEYWORDS)
    }

    #[cfg(feature = "mysql")]
    pub fn mysql80_keywords() -> Self {
        Self::new(&mysql80_keywords::MYSQL80_KEYWORDS)
    }

    #[cfg(feature = "postgres")]
    pub fn postgres_keywords() -> Self {
        Self::new(&postgresql_keywords::POSTGRESQL_KEYWORDS)
    }

    #[cfg(feature = "sqlite")]
    pub fn sqlite_keywords() -> Self {
        Self::new(&sqlite_keywords::SQLITE_KEYWORDS)
    }
}
