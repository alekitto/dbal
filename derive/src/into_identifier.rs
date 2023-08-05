use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

pub(crate) fn into_identifier(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input as DeriveInput);
    let target = input.ident;

    let tokens = quote! {
        impl ::creed::schema::IntoIdentifier for #target {
            fn into_identifier(&self) -> ::creed::schema::Identifier {
                ::creed::schema::Identifier::new(self.get_name(), self.is_quoted())
            }
        }

        impl ::core::fmt::Display for #target {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(&self.get_name())
            }
        }
    };

    tokens.into()
}
