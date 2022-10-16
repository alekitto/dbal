extern crate proc_macro;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(IntoIdentifier)]
pub fn into_identifier_derive_fn(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input as DeriveInput);
    format!(
        r#"
impl ::creed::schema::IntoIdentifier for {0} {{
    fn into_identifier(&self) -> ::creed::schema::Identifier {{
        ::creed::schema::Identifier::new(self.get_name(), self.is_quoted())
    }}
}}

impl ::core::fmt::Display for {0} {{
    fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {{
        f.write_str(&self.get_name())
    }}
}}

impl ::creed::schema::IntoIdentifier for &{0} {{
    fn into_identifier(&self) -> ::creed::schema::Identifier {{
        ::creed::schema::Identifier::new(self.get_name(), self.is_quoted())
    }}
}}
    "#,
        input.ident,
    )
    .parse()
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
