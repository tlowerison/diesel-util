use crate::util::{get_primary_key, DieselAttribute};
use itertools::Itertools;
use proc_macro2::TokenStream;
use syn::parse::{Error, Parse, ParseStream};
use syn::parse2;

#[derive(Default)]
struct DbEntityAttribute {
    raw: Option<syn::Type>,
}

impl Parse for DbEntityAttribute {
    fn parse(parse_stream: ParseStream) -> syn::Result<Self> {
        let mut raw: Option<syn::Type> = None;

        let mut first = true;
        while !parse_stream.is_empty() {
            if !first {
                let _comma: Token![,] = parse_stream.parse()?;
                if parse_stream.is_empty() {
                    break;
                }
            }
            let arg: syn::Ident = parse_stream.parse()?;
            let _eq: Token![=] = parse_stream.parse()?;

            match &*format!("{arg}") {
                "raw" => {
                    raw = Some(parse_stream.parse()?);
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        arg,
                        "db_entity attribute expected one of the following arguments: `raw`",
                    ))
                }
            };
            first = false;
        }

        Ok(DbEntityAttribute { raw })
    }
}

impl DbEntityAttribute {
    pub fn try_parse(ast: &syn::DeriveInput) -> Result<Option<(DbEntityAttribute, &syn::Attribute)>, Error> {
        for attr in ast.attrs.iter() {
            if attr.path.is_ident("db_entity") {
                return Ok(Some((attr.parse_args()?, attr)));
            }
        }
        Ok(None)
    }
}

pub fn derive_db_entity(tokens: TokenStream) -> Result<TokenStream, Error> {
    let ast: syn::DeriveInput = parse2(tokens)?;

    let struct_name = ast.ident.clone();

    let db_entity_attribute = DbEntityAttribute::try_parse(&ast)?;
    let diesel_attribute = DieselAttribute::try_parse(&ast)?;

    let fields = match &ast.data {
        syn::Data::Struct(data_struct) => match &data_struct.fields {
            syn::Fields::Named(named_fields) => &named_fields.named,
            syn::Fields::Unnamed(unnamed_fields) => &unnamed_fields.unnamed,
            syn::Fields::Unit => return Err(Error::new_spanned(ast, "DbEntity cannot be derived on unit types")),
        },
        _ => return Err(Error::new_spanned(ast, "DbEntity only supports structs")),
    };

    let (raw, table, id, selection, selection_helper) = match (db_entity_attribute, diesel_attribute) {
        (Some(_), Some((_, diesel_attr))) => {
            return Err(Error::new_spanned(
                diesel_attr,
                "unexpected `diesel` attribute, `db_entity` and `diesel` cannot be used in conjunction",
            ))
        }
        (None, None) => {
            return Err(Error::new_spanned(
                ast,
                "DbEntity derivation requires exactly one of the attributes `db_entity` or `diesel` to be provided",
            ));
        }
        (Some((db_entity_attribute, db_entity_attr)), None) => {
            let raw = db_entity_attribute.raw.ok_or_else(|| {
                Error::new_spanned(db_entity_attr, "unable to extract `raw` arg from `db_entity` attribute")
            })?;
            (
                quote!(#raw),
                quote!(<#raw as ::diesel_util::DbEntity>::Table),
                quote!(<#raw as ::diesel_util::DbEntity>::Id),
                quote!(<#raw as ::diesel_util::DbEntity>::Selection),
                quote!(<#raw as ::diesel_util::DbEntity>::SelectionHelper),
            )
        }
        (None, Some((diesel_attribute, diesel_attr))) => {
            let table_path = diesel_attribute.table_path.ok_or_else(|| {
                Error::new_spanned(
                    diesel_attr,
                    "unable to extract table_name from a diesel(table_name = `...`) attribute",
                )
            })?;
            let primary_key_name = get_primary_key(&ast);
            let id_type = {
                let mut id_type: Option<&syn::Type> = None;
                for field in fields {
                    if *field.ident.as_ref().expect("expected named field") == primary_key_name {
                        id_type = Some(&field.ty);
                        break;
                    }
                }
                id_type.ok_or_else(|| {
                    Error::new_spanned(
                        &ast,
                        "unable to determine the type of the primary key, `{primary_key_name}`",
                    )
                })?
            };

            let selection_column_names = fields
                .iter()
                .map(|field| {
                    let ident = field.ident.as_ref().expect("expected named field");
                    let mut column_name = ident.clone();
                    for attr in &field.attrs {
                        if attr.path.is_ident("diesel") {
                            let diesel_attribute: DieselAttribute = attr.parse_args()?;
                            if let Some(ident) = diesel_attribute.column_name {
                                column_name = ident;
                            }
                        }
                    }
                    Ok(quote!(#table_path::#column_name))
                })
                .collect::<Result<Vec<_>, Error>>()?;

            let selection_column_names_grouped = selection_column_names
                .iter()
                .chunks(12)
                .into_iter()
                .map(|x| {
                    let selection_column_names = x.collect::<Vec<_>>();
                    quote!((#(#selection_column_names,)*))
                })
                .collect::<Vec<_>>();

            (
                quote!(Self),
                quote!(#table_path::table),
                quote!(#id_type),
                quote!((#(#selection_column_names,)*)),
                quote!((#(#selection_column_names_grouped,)*)),
            )
        }
    };

    let tokens = quote! {
        impl ::diesel_util::DbEntity for #struct_name {
            type Raw = #raw;
            type Table = #table;
            type Id = #id;
            type Selection = #selection;
            type SelectionHelper = #selection_helper;
        }
    };

    Ok(tokens)
}
