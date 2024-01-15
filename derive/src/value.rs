use crate::common::Result;
use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::Parse;
use syn::punctuated::Punctuated;
use syn::{Expr, Token};

struct ValueMapInput {
    column_name: Expr,
    value: Expr,
    ty: Option<Expr>,
}

impl Parse for ValueMapInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let column_name = input.parse()?;
        let _arrow: Token![=>] = input.parse()?;
        let value = input.parse()?;
        let ty = if input.peek(Token![typeof]) {
            let _sep: Token![typeof] = input.parse()?;
            Some(input.parse()?)
        } else {
            None
        };

        Ok(Self {
            column_name,
            ty,
            value,
        })
    }
}

pub(crate) struct ValuesMapInput {
    inputs: Punctuated<ValueMapInput, Token![,]>,
}

impl Parse for ValuesMapInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let inputs = input.parse_terminated(
            |stream| stream.parse() as syn::Result<ValueMapInput>,
            Token![,],
        )?;

        Ok(ValuesMapInput { inputs })
    }
}

pub(crate) fn expand_value_map(input: ValuesMapInput) -> Result<TokenStream> {
    let len = input.inputs.len();
    let values = input
        .inputs
        .into_iter()
        .map(|val| {
            let key = val.column_name;
            let cur = val.value;
            let ty = if let Some(ty) = val.ty {
                quote! { Some(#ty.into_type().map_err(::creed::error::StdError::from)?) }
            } else {
                quote! { None }
            };

            let value = quote! {
                ::creed::TypedValue {
                    value: #cur.into(),
                    r#type: #ty,
                }
            };

            quote! {
                map.insert( #key, #value.into() );
            }
        })
        .collect::<Vec<_>>();

    let result = quote! {
        {
            #[allow(unused_mut)]
            let mut map = ::std::collections::HashMap::<_, ::creed::TypedValue>::with_capacity(#len);
            #( #values )*
            ::creed::TypedValueMap(map)
        }
    };

    Ok(result)
}
