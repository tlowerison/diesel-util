#![allow(incomplete_features)]
#![feature(associated_type_defaults, specialization, trait_alias)]

#[macro_use]
extern crate async_backtrace;
#[macro_use]
extern crate async_trait;
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
#[macro_use]
extern crate typed_builder;

mod connection;
mod db_pool;
mod delete;
mod error;
mod macros;
mod operations;
mod paginate;
mod schema;

pub use connection::*;
pub use db_pool::*;
pub use delete::*;
pub use diesel_util_proc_macros::*;
pub use error::*;
pub use macros::*;
pub use operations::*;
pub use paginate::*;
pub use schema::*;

pub use anyhow;
pub use async_trait::async_trait as diesel_util_async_trait;
pub use derivative::Derivative as DieselUtilDerivative;
pub use diesel;
pub use paste::paste;
pub use scoped_futures;
pub use serde_json;
pub use tokio;
pub use uuid;
