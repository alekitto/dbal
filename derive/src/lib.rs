extern crate proc_macro;
mod into_identifier;

use proc_macro::TokenStream;

#[proc_macro_derive(IntoIdentifier)]
pub fn into_identifier_derive_fn(input: TokenStream) -> TokenStream {
    into_identifier::into_identifier(input)
}
