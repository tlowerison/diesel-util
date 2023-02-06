use crate::connection::{DbConnOwned, DbConnection};
use anyhow::Error;
use diesel::{migration::MigrationConnection, Connection};
use diesel_async::AsyncConnection;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use futures::future::BoxFuture;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use typed_builder::TypedBuilder;

use diesel_async::pooled_connection::PoolableConnection;

#[cfg(feature = "bb8")]
use diesel_async::pooled_connection::bb8::Pool;

#[cfg(feature = "deadpool")]
use diesel_async::pooled_connection::deadpool::Pool;

#[cfg(feature = "mobc")]
use diesel_async::pooled_connection::mobc::Pool;

cfg_if! {
    if #[cfg(any(feature = "bb8", feature = "deadpool"))] {
        pub trait AsyncSyncConnectionBridge: AsyncConnection + PoolableConnection {
            type SyncConnection: Connection
                + MigrationConnection
                + MigrationHarness<<Self as AsyncConnection>::Backend>
                + 'static;
        }
    }
}

cfg_if! {
    if #[cfg(feature = "mobc")] {
        pub trait AsyncSyncConnectionBridge: AsyncConnection + PoolableConnection + mobc::Manager {
            type SyncConnection: Connection
                + MigrationConnection
                + MigrationHarness<<Self as AsyncConnection>::Backend>
                + 'static;
        }
    }
}

/// _DbPool wraps a deadpool diesel connection pool.
/// It is prefixed with a _ to allow convenient type aliasing in applicatins
/// with the name DbPool, e.g.
/// ```
/// type DbPool = diesel_util::_DbPool<AsyncPgConnection>;
/// ```
#[derive(Deref, DerefMut, Derivative)]
#[derivative(Debug)]
pub struct _DbPool<C: AsyncSyncConnectionBridge + 'static>(
    #[derivative(Debug = "ignore")] pub Arc<Pool<C>>,
);

impl<C: AsyncSyncConnectionBridge> Clone for _DbPool<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Clone, Debug, TypedBuilder)]
pub struct DbPoolConfig<F> {
    #[builder(setter(into))]
    pub database_url: String,
    #[builder(setter(into))]
    pub database_migration_url: String,
    pub pool_builder: F,
}

#[cfg(feature = "mysql")]
impl AsyncSyncConnectionBridge for diesel_async::AsyncMysqlConnection {
    type SyncConnection = diesel::mysql::MysqlConnection;
}

#[cfg(feature = "postgres")]
impl AsyncSyncConnectionBridge for diesel_async::AsyncPgConnection {
    type SyncConnection = diesel::pg::PgConnection;
}

impl<C: AsyncSyncConnectionBridge + 'static> _DbPool<C> {
    pub async fn new<F, E>(
        db_pool_config: DbPoolConfig<F>,
        migrations: impl Into<Option<EmbeddedMigrations>>,
    ) -> Result<Self, Error>
    where
        E: Debug + Display + Send + Sync + 'static,
        F: for<'a> Fn(&'a str) -> BoxFuture<'a, Result<Pool<C>, E>>,
    {
        let DbPoolConfig {
            database_url,
            database_migration_url,
            pool_builder,
        } = db_pool_config;
        let migrations = migrations.into();

        info!("connecting to database");

        let db_pool = Self(Arc::new(pool_builder(&database_url).await.map_err(Error::msg)?));
        db_pool.ping().await?;

        info!("connected to database successfully");

        if let Some(migrations) = migrations {
            tokio::task::spawn_blocking(move || {
                info!("connecting to database for migrations");
                let mut migration_conn =
                    <C as AsyncSyncConnectionBridge>::SyncConnection::establish(
                        &database_migration_url,
                    )
                    .unwrap_or_else(|_| panic!("error connecting to db for migrations"));
                info!("running pending database migrations");
                match migration_conn.run_pending_migrations(migrations) {
                    Ok(_) => Ok(()),
                    Err(err) => Err(Error::msg(err.to_string())),
                }
            })
            .await??;

            info!("ran migrations successfully");
        }

        Ok(db_pool)
    }

    pub async fn ping(&self) -> Result<(), anyhow::Error> {
        self.get()
            .await
            .map_err(|err| Error::msg(format!("could not get db pooled connection: {err}")))?
            .ping()
            .await
            .map_err(|err| Error::msg(format!("db ping failed: {err}")))?;
        Ok(())
    }

    pub(crate) async fn get_connection(&self) -> Result<DbConnOwned<C>, diesel::result::Error> {
        let connection = self
            .0
            .get()
            .await
            .map_err(|err| diesel::result::Error::QueryBuilderError(format!("could not get pooled connection to database: {err}").into()))?;
        Ok(DbConnection::from(connection))
    }
}
