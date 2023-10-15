use ::proc_macro2::{TokenStream, TokenTree};
use ::std::str::FromStr;
use ::syn::parse::{Error, Parse, ParseStream};
use ::syn::{DeriveInput, Ident, Path, Token};

#[derive(Clone, Default, Debug)]
pub struct DieselAttribute {
    pub column_name: Option<Ident>,
    pub table_name: Option<Ident>,
    pub table_path: Option<Path>,
}

impl Parse for DieselAttribute {
    fn parse(parse_stream: ParseStream) -> syn::Result<Self> {
        let mut column_name: Option<Ident> = None;
        let mut table_path: Option<Path> = None;

        while !parse_stream.is_empty() {
            if parse_stream.peek(syn::Ident) {
                let arg: syn::Ident = parse_stream.parse()?;
                match &*format!("{arg}") {
                    "table_name" => {
                        let eq: Option<Token![=]> = parse_stream.parse().ok();
                        if eq.is_some() {
                            let path: Option<Path> = parse_stream.parse().ok();
                            if let Some(path) = path {
                                table_path = Some(path);
                            } else {
                                let _: TokenTree = parse_stream.parse()?;
                            }
                        } else {
                            let _: TokenTree = parse_stream.parse()?;
                        }
                    }
                    "column_name" => {
                        let eq: Option<Token![=]> = parse_stream.parse().ok();
                        if eq.is_some() {
                            let lit_str: Option<syn::LitStr> = parse_stream.parse().ok();
                            if let Some(lit_str) = lit_str {
                                let tokens = TokenStream::from_str(&lit_str.value())
                                    .map_err(|err| Error::new_spanned(&lit_str, err))?;
                                let ident =
                                    syn::parse2::<Ident>(tokens).map_err(|err| Error::new_spanned(&lit_str, err))?;
                                column_name = Some(ident);
                            } else {
                                let _: TokenTree = parse_stream.parse()?;
                            }
                        } else {
                            let _: TokenTree = parse_stream.parse()?;
                        }
                    }
                    _ => {
                        let _: TokenTree = parse_stream.parse()?;
                    }
                }
            } else if !parse_stream.is_empty() {
                let _: TokenTree = parse_stream.parse()?;
            }
        }

        let table_name = table_path
            .as_ref()
            .map(|path| path.segments.last().unwrap().ident.clone());

        Ok(Self {
            column_name,
            table_name,
            table_path,
        })
    }
}

impl DieselAttribute {
    pub fn try_parse(ast: &DeriveInput) -> Result<Option<(DieselAttribute, &syn::Attribute)>, Error> {
        for attr in ast.attrs.iter() {
            if attr.path.is_ident("diesel") {
                return Ok(Some((attr.parse_args()?, attr)));
            }
        }
        Ok(None)
    }
}

pub(crate) fn get_primary_key(ast: &syn::DeriveInput) -> syn::Ident {
    ast.attrs
        .iter()
        .find(|attr| {
            if let Some(attr_ident) = attr.path.get_ident() {
                if &*format!("{attr_ident}") == "primary_key" {
                    return true;
                }
            }
            false
        })
        .map(|attr| attr.parse_args().unwrap())
        .unwrap_or_else(|| format_ident!("id"))
}
