use super::asset::AbstractAsset;
use crate::schema::asset::{impl_asset, Asset};
use creed_derive::IntoIdentifier;
use std::borrow::{Borrow, Cow};
use std::fmt::Display;

#[derive(Clone, Debug, IntoIdentifier, PartialEq)]
pub struct Identifier {
    asset: AbstractAsset,
}

impl Identifier {
    pub fn new<S: AsRef<str>>(identifier: S, quote: bool) -> Self {
        let identifier = identifier.as_ref();
        let mut asset = AbstractAsset::default();
        asset.set_name(identifier);

        if quote && !asset.is_quoted() {
            asset.set_name(&format!(r#""{}""#, asset.get_name()));
        }

        Self { asset }
    }
}

pub trait IntoIdentifier: Display {
    #[allow(clippy::wrong_self_convention)]
    fn into_identifier(&self) -> Identifier;
}

impl IntoIdentifier for str {
    fn into_identifier(&self) -> Identifier {
        Identifier::from(self)
    }
}

impl IntoIdentifier for Cow<'_, str> {
    fn into_identifier(&self) -> Identifier {
        Identifier::from(self.borrow())
    }
}

impl IntoIdentifier for &str {
    fn into_identifier(&self) -> Identifier {
        Identifier::from(*self)
    }
}

impl IntoIdentifier for String {
    fn into_identifier(&self) -> Identifier {
        Identifier::from(self.as_str())
    }
}

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        Identifier::new(s, false)
    }
}

impl_asset!(Identifier, asset);
