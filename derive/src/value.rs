use crate::common::Result;
use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::Parse;
use syn::{Error, Expr, Token};

struct ValueMapInput {
    column_name: Expr,
    _arrow: Token![=>],
    ty: Option<Expr>,
    _col: Option<Token![;]>,
    value: Expr,
}

impl Parse for ValueMapInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let column_name = input.parse()?;
        let _arrow = input.parse()?;
        let (ty, _col) = if input.peek2(Token![;]) {
            (Some(input.parse()?), Some(input.parse()?))
        } else {
            (None, None)
        };
        let value = input.parse()?;

        Ok(Self {
            column_name,
            _arrow,
            ty,
            _col,
            value,
        })
    }
}

pub(crate) struct ValuesMapInput {
    inputs: Vec<ValueMapInput>,
}

impl Parse for ValuesMapInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut inputs = vec![];
        while !input.is_empty() {
            inputs.push(input.parse()?);
            if input.peek(Token![,]) {
                let _comma: Token![,] = input.parse()?;
            } else if !input.is_empty() {
                return Err(Error::new(input.span(), r#"expected "," or end of input"#));
            }
        }

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
                quote! { Some(#ty.into_type()?) }
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
            ::creed::TypedValueMap(map).into()
        }
    };

    Ok(result)
}
