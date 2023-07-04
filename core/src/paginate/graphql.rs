#[cfg(feature = "async-graphql-4")]
use async_graphql_4 as async_graphql;
#[cfg(feature = "async-graphql-5")]
use async_graphql_5 as async_graphql;

cfg_if! {
    if #[cfg(any(feature = "async-graphql-4", feature = "async-graphql-5"))] {
        #[derive(Clone, Copy, Debug)]
        pub (crate) struct GraphqlPaginationCountValidator;

        impl async_graphql::CustomValidator<u32> for GraphqlPaginationCountValidator {
            fn check(&self, value: &u32) -> Result<(), String> {
                match crate::paginate::pagination_max_count() {
                    None => Ok(()),
                    Some(max) => match value <= max {
                        true => Ok(()),
                        false => Err(format!("Page count is too large, must be less than or equal to {max}")),
                    },
                }
            }
        }

    }
}
