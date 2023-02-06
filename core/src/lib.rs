#![allow(incomplete_features)]
#![feature(associated_type_defaults, specialization, trait_alias)]

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
pub extern crate lazy_static;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;

mod connection;
mod delete;
mod error;
mod macros;
// mod operations;
mod paginate;
mod schema;
mod scoped;

pub use connection::*;
pub use delete::*;
pub use diesel_util_proc_macros::*;
pub use error::*;
pub use macros::*;
// pub use operations::*;
pub use paginate::*;
pub use schema::*;
pub use scoped::*;

cfg_if! {
    if #[cfg(any(feature = "deadpool", feature = "bb8", feature = "mobc"))] {
        mod db_pool;
        pub use db_pool::*;
    }
}
