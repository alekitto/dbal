use crate::common::{resolve_path, Result};
use proc_macro2::{Ident, TokenStream};
use quote::{quote, ToTokens, TokenStreamExt};
use sha2::{Digest, Sha384};
use std::fs::{metadata, read_dir, read_to_string};
use std::path::Path;
use syn::parse::Parse;
use syn::token::Super;
use syn::{Item, LitStr, Token, VisRestricted, Visibility};

pub(crate) struct MigrateInput {
    pub_token: Option<Token![pub]>,
    ident: Ident,
    _comma: Token![,],
    path: LitStr,
}

impl Parse for MigrateInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let pub_token = if input.peek(Token![pub]) {
            Some(input.parse()?)
        } else {
            None
        };

        Ok(Self {
            pub_token,
            ident: input.parse()?,
            _comma: input.parse()?,
            path: input.parse()?,
        })
    }
}

#[derive(Default)]
struct QuotedMigration {
    version: i64,
    checksum: Vec<u8>,

    has_pre_up: bool,
    has_post_up: bool,
    has_pre_down: bool,
    has_post_down: bool,
}

impl ToTokens for QuotedMigration {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let QuotedMigration {
            version,
            checksum,
            has_pre_up,
            has_post_up,
            has_pre_down,
            has_post_down,
        } = &self;

        let version_mod_name: Ident = syn::parse_str(&format!("version_{}", version)).unwrap();
        let pre_up_ref = if *has_pre_up {
            quote! { Some(&#version_mod_name::pre_up) }
        } else {
            quote! { None }
        };
        let pre_down_ref = if *has_pre_down {
            quote! { Some(&#version_mod_name::pre_down) }
        } else {
            quote! { None }
        };

        let post_up_ref = if *has_post_up {
            quote! { Some(&#version_mod_name::post_up) }
        } else {
            quote! { None }
        };
        let post_down_ref = if *has_post_down {
            quote! { Some(&#version_mod_name::post_down) }
        } else {
            quote! { None }
        };

        let ts = quote! {
            ::creed::migrate::Migration {
                version: #version,
                description: &#version_mod_name::description,
                up: &#version_mod_name::up,
                down: &#version_mod_name::down,
                pre_up: #pre_up_ref,
                pre_down: #pre_down_ref,
                post_up: #post_up_ref,
                post_down: #post_down_ref,
                // this tells the compiler to watch this path for changes
                checksum: ::std::borrow::Cow::Borrowed(&[
                    #(#checksum),*
                ]),
            }
        };

        tokens.append_all(ts);
    }
}

pub(crate) fn expand_migrator_from_lit_dir(migrate_input: MigrateInput) -> Result<TokenStream> {
    let path = resolve_path(migrate_input.path.value(), migrate_input.path.span())?;
    expand_migrator(&path, &migrate_input.ident, &migrate_input.pub_token)
}

pub(crate) fn expand_migrator(
    path: &Path,
    migrator_name: &Ident,
    pub_token: &Option<Token![pub]>,
) -> Result<TokenStream> {
    let mut migrations = Vec::new();
    let mut migrations_mods = Vec::new();

    for entry in read_dir(path)? {
        let entry = entry?;
        if !metadata(entry.path())?.is_file() {
            // not a file; ignore
            continue;
        }

        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();

        if !file_name.starts_with("version_") || !file_name.ends_with(".rs") {
            // not of the format: version_<VERSION>.rs; ignore
            continue;
        }

        let end = file_name.len() - 3;
        let version: i64 = file_name[8..end].parse()?;
        let migration_file = read_to_string(entry.path())?;

        let version_mod_name: Ident = syn::parse_str(&format!("version_{}", version))?;
        let checksum = Vec::from(Sha384::digest(migration_file.as_bytes()).as_slice());

        let mut quoted_migration = QuotedMigration {
            version,
            checksum,
            ..Default::default()
        };

        let mut program = syn::parse_file(&migration_file)?;
        for item in program.items.iter_mut() {
            if let Item::Fn(func) = item {
                func.vis = Visibility::Restricted(VisRestricted {
                    pub_token: Default::default(),
                    paren_token: Default::default(),
                    in_token: None,
                    path: Box::new(syn::Path::from(syn::parse_str::<Super>("super")?)),
                });

                let func_name = func.sig.ident.to_string();
                if func_name == "pre_up" {
                    quoted_migration.has_pre_up = true;
                }
                if func_name == "pre_down" {
                    quoted_migration.has_pre_down = true;
                }
                if func_name == "post_up" {
                    quoted_migration.has_post_up = true;
                }
                if func_name == "post_down" {
                    quoted_migration.has_post_down = true;
                }
            }
        }

        let migration_tokens: TokenStream = quote! {
            mod #version_mod_name {
                #program
            }
        };

        migrations_mods.push(migration_tokens);
        migrations.push(quoted_migration);
    }

    let token_stream: TokenStream = quote! {
        #pub_token const #migrator_name: ::creed::migrate::Migrator = ::creed::migrate::Migrator::new(
            ::std::borrow::Cow::Borrowed(&[
                #(#migrations),*
            ]),
            true,
            true,
        );

        #(#migrations_mods)*
    };

    Ok(token_stream)
}
