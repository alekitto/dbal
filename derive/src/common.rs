use proc_macro2::Span;
use std::env;
use std::path::{Path, PathBuf};

pub(crate) fn resolve_path(path: impl AsRef<Path>, err_span: Span) -> syn::Result<PathBuf> {
    let path = path.as_ref();
    if path.is_absolute() {
        return Err(syn::Error::new(
            err_span,
            "absolute paths will only work on the current machine",
        ));
    }

    // requires `proc_macro::SourceFile::path()` to be stable
    // https://github.com/rust-lang/rust/issues/54725
    if path.is_relative()
        && !path
            .parent()
            .map_or(false, |parent| !parent.as_os_str().is_empty())
    {
        return Err(syn::Error::new(
            err_span,
            "paths relative to the current file's directory are not currently supported",
        ));
    }

    let base_dir = env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(
            err_span,
            "CARGO_MANIFEST_DIR is not set; please use Cargo to build",
        )
    })?;

    Ok(Path::new(&base_dir).join(path))
}

pub(crate) type Error = Box<dyn std::error::Error>;
pub(crate) type Result<T> = std::result::Result<T, Error>;
