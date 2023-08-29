use proc_macro2::TokenStream;
use proc_macro_util::{find_field_attribute_in_struct, MatchedAttribute};
use syn::parse::Error;

pub fn derive_reassignment(tokens: TokenStream) -> Result<TokenStream, Error> {
    let ast: syn::DeriveInput = syn::parse2(tokens)?;

    let MatchedAttribute {
        field: parent_id_field,
        field_accessor: parent_id_accessor,
        ..
    } = find_field_attribute_in_struct("parent_id", &ast)?;
    let MatchedAttribute {
        field: child_id_field,
        field_accessor: child_id_accessor,
        ..
    } = find_field_attribute_in_struct("child_id", &ast)?;

    let parent_id_ty = &parent_id_field.ty;
    let child_id_ty = &child_id_field.ty;

    let ident = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let tokens = quote! {
        impl #impl_generics ::diesel_util::Reassignment for #ident #ty_generics #where_clause {
            type ParentId = #parent_id_ty;
            type ChildId = #child_id_ty;
            fn parent_id(&self) -> &Self::ParentId {
                &self.#parent_id_accessor
            }
            fn child_id(&self) -> &Self::ChildId {
                &self.#child_id_accessor
            }
        }
    };

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proc_macro_util::pretty;

    #[test]
    fn test_reassignment() -> Result<(), Error> {
        let expected = pretty(&quote!(
            impl ::diesel_util::Reassignment for Struct {
                type ParentId = i32;
                type ChildId = i64;
                fn parent_id(&self) -> &Self::ParentId {
                    &self.parent_id
                }
                fn child_id(&self) -> &Self::ChildId {
                    &self.child_id
                }
            }
        ))?;
        let received = pretty(&derive_reassignment(quote!(
            pub struct Struct {
                #[parent_id]
                pub parent_id: i32,
                #[child_id]
                pub child_id: i64,
            }
        ))?)?;

        println!("{expected}");
        println!("{received}");

        assert_eq!(expected, received);

        Ok(())
    }
}
