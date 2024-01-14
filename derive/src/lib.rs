extern crate proc_macro;
mod common;
mod into_identifier;
mod migrate;
mod value;

use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_derive(IntoIdentifier)]
pub fn into_identifier_derive_fn(input: TokenStream) -> TokenStream {
    into_identifier::into_identifier(input)
}

#[proc_macro]
pub fn migrator(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as migrate::MigrateInput);
    match migrate::expand_migrator_from_lit_dir(input) {
        Ok(ts) => ts.into(),
        Err(e) => {
            if let Some(parse_err) = e.downcast_ref::<syn::Error>() {
                parse_err.to_compile_error().into()
            } else {
                let msg = e.to_string();
                quote!(::std::compile_error!(#msg)).into()
            }
        }
    }
}

#[proc_macro]
pub fn value_map(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as value::ValuesMapInput);
    match value::expand_value_map(input) {
        Ok(ts) => ts.into(),
        Err(e) => {
            if let Some(parse_err) = e.downcast_ref::<syn::Error>() {
                parse_err.to_compile_error().into()
            } else {
                let msg = e.to_string();
                quote!(::std::compile_error!(#msg)).into()
            }
        }
    }
}
