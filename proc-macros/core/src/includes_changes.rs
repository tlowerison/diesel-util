use crate::util::get_primary_key;
use proc_macro2::TokenStream;
use syn::parse::Error;
use syn::parse2;

pub fn derive_includes_changes(tokens: TokenStream) -> Result<TokenStream, Error> {
    let ast: syn::DeriveInput = parse2(tokens)?;

    let ident = &ast.ident;

    let mut field_names: Vec<_> = match &ast.data {
        syn::Data::Struct(data_struct) => match &data_struct.fields {
            syn::Fields::Named(fields_named) => fields_named
                .named
                .iter()
                .map(|field| field.ident.as_ref().unwrap().clone())
                .collect(),
            syn::Fields::Unnamed(fields_unnamed) => fields_unnamed
                .unnamed
                .iter()
                .map(|field| field.ident.as_ref().unwrap().clone())
                .collect(),
            syn::Fields::Unit => vec![],
        },
        _ => panic!("IncludesChanges can only be derived on struct data types at this time."),
    };

    let primary_key_name = get_primary_key(&ast);

    let mut primary_key_index: isize = -1;
    for (index, field_name) in field_names.iter().enumerate() {
        if *field_name == primary_key_name {
            primary_key_index = index as isize;
            break;
        }
    }

    let primary_key_index = if primary_key_index < 0 {
        panic!(
            "IncludesChanges could not be derived for primary key `{primary_key_name}`, field not found in `{ident}`"
        );
    } else {
        primary_key_index as usize
    };

    field_names.remove(primary_key_index);

    let opt_field_names: Vec<_> = match ast.data {
        syn::Data::Struct(data_struct) => match data_struct.fields {
            syn::Fields::Named(fields_named) => fields_named
                .named
                .into_iter()
                .filter_map(|field| match &field.ty {
                    syn::Type::Path(syn::TypePath { path, .. }) => {
                        if &path.segments[0].ident == "Option" {
                            let ident = field.ident.unwrap();
                            Some(quote!(#ident))
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .collect(),
            syn::Fields::Unnamed(fields_unnamed) => fields_unnamed
                .unnamed
                .into_iter()
                .enumerate()
                .filter_map(|(i, field)| match &field.ty {
                    syn::Type::Path(syn::TypePath { path, .. }) => {
                        if &path.segments[0].ident == "Option" {
                            Some(format!("{i}").parse().unwrap())
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .collect(),
            syn::Fields::Unit => vec![],
        },
        _ => panic!("IncludesChanges can only be derived on struct data types at this time."),
    };

    let expr = if field_names.is_empty() {
        quote! { false }
    } else if opt_field_names.is_empty() {
        quote! { true }
    } else {
        quote! {
            #(
                self.#opt_field_names.is_some()
            )||*
        }
    };

    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let tokens = quote! {
        impl #impl_generics ::diesel_util::IncludesChanges for #ident #ty_generics #where_clause {
            fn includes_changes(&self) -> bool {
                #expr
            }
        }
    };

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() -> Result<(), Error> {
        assert_eq!(
            derive_includes_changes(quote!(
                #[derive(IncludesChanges)]
                pub struct DbFoo {
                    pub id: Uuid,
                    pub a: Option<i32>,
                    pub b: Option<bool>,
                    pub c: Option<String>,
                }
            ))?
            .to_string(),
            quote!(
                impl ::diesel_util::IncludesChanges for DbFoo {
                    fn includes_changes(&self) -> bool {
                        self.a.is_some() || self.b.is_some() || self.c.is_some()
                    }
                }
            )
            .to_string(),
        );

        Ok(())
    }
}
