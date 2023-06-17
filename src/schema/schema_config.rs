#[derive(Clone, Debug)]
pub struct SchemaConfig {
    pub max_identifier_length: usize,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            max_identifier_length: 63,
        }
    }
}