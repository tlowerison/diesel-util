#![allow(incomplete_features)]
#![feature(
    associated_type_defaults,
    return_position_impl_trait_in_trait,
    specialization,
    trait_alias
)]

#[cfg(not(any(feature = "anyhow", feature = "color-eyre")))]
compile_error!("One of `anyhow` or `color-eyre` features must be enabled.");

#[cfg(all(feature = "anyhow", feature = "color-eyre"))]
compile_error!("Cannot compile with both `anyhow` and `color-eyre` features enabled.");

#[cfg(all(feature = "async-graphql-4", feature = "async-graphql-5"))]
compile_error!("Two versions of the subdependency `async-graphql` were enabled, please only enable one by only using one of the features: `async-graphql-4`, `async-graphql-5`.");

#[cfg(feature = "anyhow")]
pub(crate) use anyhow::Error as InternalError;

#[cfg(feature = "color-eyre")]
pub(crate) use color_eyre::Report as InternalError;

#[macro_use]
extern crate async_backtrace;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate cfg_if;
#[macro_use]
extern crate derivative;
#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;

mod audit;
mod connection;
mod deletable;
mod is_deleted;
mod macros;
mod operations;
mod paginate;
mod schema;

pub use audit::*;
pub use connection::*;
pub use deletable::*;
pub use is_deleted::*;
pub use macros::*;
pub use operations::*;
pub use paginate::*;
pub use schema::*;

cfg_if! {
    if #[cfg(any(feature = "bb8", feature = "deadpool", feature = "mobc"))] {
        mod pool;
        pub use pool::*;
    }
}
