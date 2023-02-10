use proc_macro2::TokenStream;
use std::str::FromStr;
use syn::parse::Error;
use syn::parse2;
use syn::punctuated::Punctuated;

pub fn derive_db(item: TokenStream) -> Result<TokenStream, Error> {
    let ast: syn::DeriveInput = parse2(item)?;

    let ident = &ast.ident;

    let data_struct = match &ast.data {
        syn::Data::Struct(data_struct) => data_struct,
        _ => {
            return Err(Error::new_spanned(
                ast,
                "Db can only be derived on struct types",
            ))
        }
    };

    let mut db_field_and_index: Option<(usize, &syn::Field)> = None;
    for (index, db_field) in data_struct.fields.iter().enumerate() {
        for attr in &db_field.attrs {
            if attr.path.is_ident("db") {
                if db_field_and_index.is_some() {
                    return Err(Error::new_spanned(
                        attr,
                        "`#[db]` attribute cannot be used more than once",
                    ));
                } else {
                    db_field_and_index = Some((index, db_field))
                }
            }
        }
    }
    let (db_field_index, db_field) = match db_field_and_index {
        Some(db_field_and_index) => db_field_and_index,
        None => {
            return Err(Error::new_spanned(
                ast,
                "Db requires exactly one field to be marked with the `#[db]` attribute",
            ));
        }
    };

    let db_field_ident = match db_field.ident.as_ref() {
        Some(ident) => quote!(#ident),
        None => TokenStream::from_str(&format!("{db_field_index}")).unwrap(),
    };
    let db_field_type = &db_field.ty;

    let (_, ty_generics, _) = ast.generics.split_for_impl();

    let with_tx_connection_lifetime = quote!('with_tx_connection);

    let mut db_trait_generics = ast.generics.clone();
    for param in db_trait_generics.params.iter_mut() {
        if let syn::GenericParam::Type(syn::TypeParam { bounds, ident, .. }) = param {
            let mut has_clone_bound = false;
            let mut has_send_bound = false;
            let mut has_sync_bound = false;
            let is_param_exact_db_field_type =
                if let syn::Type::Path(syn::TypePath { path, .. }) = db_field_type {
                    if let Some(db_field_type_ident) = path.get_ident() {
                        ident == db_field_type_ident
                    } else {
                        false
                    }
                } else {
                    false
                };

            for bound in &*bounds {
                if let syn::TypeParamBound::Trait(syn::TraitBound { path, .. }) = bound {
                    if path.is_ident("Clone") {
                        has_clone_bound = true;
                    }
                    if path.is_ident("Send") {
                        has_send_bound = true;
                    }
                    if path.is_ident("Sync") {
                        has_sync_bound = true;
                    }
                }
            }

            if !has_clone_bound {
                bounds.push(syn::TypeParamBound::Trait(syn::TraitBound {
                    paren_token: None,
                    modifier: syn::TraitBoundModifier::None,
                    lifetimes: None,
                    path: syn::Path {
                        leading_colon: None,
                        segments: Punctuated::from_iter(
                            [syn::PathSegment {
                                ident: format_ident!("Clone"),
                                arguments: syn::PathArguments::None,
                            }]
                            .into_iter(),
                        ),
                    },
                }));
            }
            if !has_send_bound {
                bounds.push(syn::TypeParamBound::Trait(syn::TraitBound {
                    paren_token: None,
                    modifier: syn::TraitBoundModifier::None,
                    lifetimes: None,
                    path: syn::Path {
                        leading_colon: None,
                        segments: Punctuated::from_iter(
                            [syn::PathSegment {
                                ident: format_ident!("Send"),
                                arguments: syn::PathArguments::None,
                            }]
                            .into_iter(),
                        ),
                    },
                }));
            }
            if !has_sync_bound {
                bounds.push(syn::TypeParamBound::Trait(syn::TraitBound {
                    paren_token: None,
                    modifier: syn::TraitBoundModifier::None,
                    lifetimes: None,
                    path: syn::Path {
                        leading_colon: None,
                        segments: Punctuated::from_iter(
                            [syn::PathSegment {
                                ident: format_ident!("Sync"),
                                arguments: syn::PathArguments::None,
                            }]
                            .into_iter(),
                        ),
                    },
                }));
            }
            if is_param_exact_db_field_type {
                bounds.push(syn::TypeParamBound::Trait(syn::TraitBound {
                    paren_token: None,
                    modifier: syn::TraitBoundModifier::None,
                    lifetimes: None,
                    path: syn::Path {
                        leading_colon: None,
                        segments: Punctuated::from_iter(
                            [
                                syn::PathSegment {
                                    ident: format_ident!("diesel_util"),
                                    arguments: syn::PathArguments::None,
                                },
                                syn::PathSegment {
                                    ident: format_ident!("_Db"),
                                    arguments: syn::PathArguments::None,
                                },
                            ]
                            .into_iter(),
                        ),
                    },
                }));
            }
        }
    }

    let (db_impl_generics, _, db_where_clause) = db_trait_generics.split_for_impl();

    let tx_connection_input = format_ident!("tx_connection");
    let tx_connection_constructor = match &data_struct.fields {
        syn::Fields::Named(_) => {
            let fields = data_struct
                .fields
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let field_ident = &field.ident;
                    if i == db_field_index {
                        quote!(#field_ident: #tx_connection_input)
                    } else {
                        quote!(#field_ident: self.#field_ident.clone())
                    }
                })
                .collect::<Vec<_>>();
            quote!(#ident { #(#fields,)* })
        }
        syn::Fields::Unnamed(_) => {
            let fields = data_struct
                .fields
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    let field_ident = TokenStream::from_str(&format!("{i}")).unwrap();
                    if i == db_field_index {
                        quote!(#tx_connection_input)
                    } else {
                        quote!(self.#field_ident.clone())
                    }
                })
                .collect::<Vec<_>>();
            quote!(#ident(#(#fields,)*))
        }
        _ => unreachable!(),
    };

    if ast.generics.params.is_empty() {
        return Err(Error::new_spanned(ast, "Db requires at least one generic which must be used as the exact type value for the field marked with `#[db]`"));
    }

    let mut tx_connection_type_constructor = quote!(#ident<);
    for param in ast.generics.params.iter() {
        if let syn::GenericParam::Type(syn::TypeParam { ident, .. }) = param {
            let is_param_exact_db_field_type =
                if let syn::Type::Path(syn::TypePath { path, .. }) = db_field_type {
                    if let Some(db_field_type_ident) = path.get_ident() {
                        ident == db_field_type_ident
                    } else {
                        false
                    }
                } else {
                    false
                };
            if is_param_exact_db_field_type {
                tx_connection_type_constructor = quote!(#tx_connection_type_constructor <#ident as diesel_util::_Db>::TxConnection<#with_tx_connection_lifetime>,);
            } else {
                tx_connection_type_constructor = quote!(#tx_connection_type_constructor #ident,);
            }
        }
    }
    tx_connection_type_constructor = quote!(#tx_connection_type_constructor>);

    let tokens = quote! {
        #[diesel_util::diesel_util_async_trait]
        impl #db_impl_generics diesel_util::_Db for #ident #ty_generics #db_where_clause {
            type Backend = <#db_field_type as diesel_util::_Db>::Backend;
            type AsyncConnection = <#db_field_type as diesel_util::_Db>::AsyncConnection;
            type Connection<'r> = <#db_field_type as diesel_util::_Db>::Connection<'r> where Self: 'r;
            type TxConnection<#with_tx_connection_lifetime> = #tx_connection_type_constructor;

            async fn query<'a, F, T, E>(&self, f: F) -> Result<T, E>
            where
                F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> diesel_util::scoped_futures::ScopedBoxFuture<'a, 'r, Result<T, E>>
                    + Send
                    + 'a,
                E: std::fmt::Debug + From<diesel_util::diesel::result::Error> + Send + 'a,
                T: Send + 'a
            {
                self.#db_field_ident.query(f).await
            }

            async fn with_tx_connection<'a, F, T, E>(&self, f: F) -> Result<T, E>
            where
                F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> diesel_util::scoped_futures::ScopedBoxFuture<'a, 'r, Result<T, E>>
                    + Send
                    + 'a,
                E: std::fmt::Debug + From<diesel_util::diesel::result::Error> + Send + 'a,
                T: Send + 'a
            {
                self.#db_field_ident.with_tx_connection(f).await
            }

            fn tx_id(&self) -> Option<diesel_util::uuid::Uuid> {
                self.#db_field_ident.tx_id()
            }

            async fn tx_cleanup<F, E>(&self, f: F)
            where
                F: for<'r> diesel_util::TxCleanupFn<'r, Self::AsyncConnection, E>,
                E: Into<diesel_util::TxCleanupError> + 'static
            {
                self.#db_field_ident.tx_cleanup(f).await
            }

            async fn tx<'life0, 'a, T, E, F>(&'life0 self, callback: F) -> Result<T, E>
            where
                F: for<'r> diesel_util::TxFn<'a, Self::TxConnection<'r>, diesel_util::scoped_futures::ScopedBoxFuture<'a, 'r, Result<T, E>>>,
                E: std::fmt::Debug + From<diesel_util::diesel::result::Error> + From<diesel_util::TxCleanupError> + Send + 'a,
                T: Send + 'a,
                'life0: 'a
            {
                self.#db_field_ident.tx(|#tx_connection_input| {
                    let tx_connection = #tx_connection_constructor;
                    callback(tx_connection)
                }).await
            }
        }
    };

    Ok(tokens)
}
