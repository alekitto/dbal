use super::asset::AbstractAsset;
use crate::schema::asset::Asset;

#[derive(Clone, PartialEq)]
pub struct Identifier {
    asset: AbstractAsset,
}

impl Identifier {
    pub fn new<S: Into<String>>(identifier: S, quote: bool) -> Self {
        let identifier = identifier.into();
        let mut asset = AbstractAsset::default();
        asset.set_name(identifier);

        if quote && !asset.is_quoted() {
            asset.set_name(format!(r#""{}""#, asset.get_name()));
        }

        Self { asset }
    }
}

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        Identifier::new(s, false)
    }
}

impl Asset for Identifier {
    fn get_name(&self) -> String {
        self.asset.get_name()
    }

    fn set_name(&mut self, name: String) {
        self.asset.set_name(name)
    }

    fn get_namespace_name(&self) -> Option<String> {
        self.asset.get_namespace_name()
    }

    fn get_shortest_name(&self, default_namespace_name: &str) -> String {
        self.asset.get_shortest_name(default_namespace_name)
    }

    fn is_quoted(&self) -> bool {
        self.asset.is_quoted()
    }
}
