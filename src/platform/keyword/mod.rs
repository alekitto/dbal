#[cfg(feature = "mysql")]
mod mariadb_keywords;
#[cfg(feature = "mysql")]
mod mysql80_keywords;
#[cfg(feature = "mysql")]
mod mysql_keywords;

pub trait Keywords {
    /// Returns the name of this keyword list.
    fn get_name(&self) -> &'static str;

    /// Returns the list of keywords.
    fn get_keywords(&self) -> &[&'static str];
}

pub struct KeywordList {
    keywords: &'static dyn Keywords,
}

impl KeywordList {
    pub(crate) fn new(keywords: &'static dyn Keywords) -> Self {
        Self { keywords }
    }

    /// Checks if the given word is a keyword of this dialect/vendor platform.
    pub fn is_keyword(&self, word: &str) -> bool {
        self.keywords.get_keywords().iter().any(|w| (*w).eq(word))
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
}
