use crate::connection::{DbConnOwned, DbConnection};
use anyhow::Error;
use diesel::{migration::MigrationConnection, Connection};
use diesel_async::AsyncConnection;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use std::sync::Arc;
use typed_builder::TypedBuilder;

use diesel_async::pooled_connection::{AsyncDieselConnectionManager, PoolableConnection};

#[cfg(feature = "deadpool")]
use diesel_async::pooled_connection::deadpool::Pool;

#[cfg(feature = "bb8")]
use diesel_async::pooled_connection::bb8::Pool;

/// _DbPool wraps a deadpool diesel connection pool.
/// It is prefixed with a _ to allow convenient type aliasing in applicatins
/// with the name DbPool, e.g.
/// ```
/// type DbPool = diesel_util::_DbPool<AsyncPgConnection>;
/// ```
#[derive(Deref, DerefMut, Derivative)]
#[derivative(Debug)]
pub struct _DbPool<C: PoolableConnection + 'static>(
    #[derivative(Debug = "ignore")] pub Arc<Pool<C>>,
);

impl<C: PoolableConnection> Clone for _DbPool<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Clone, Debug, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
pub struct DbPoolConfig {
    pub database_url: String,
    pub database_migration_url: String,
    pub max_connections: Option<usize>,
}

pub trait AsyncSyncConnectionBridge: AsyncConnection {
    type SyncConnection: Connection
        + MigrationConnection
        + MigrationHarness<<Self as AsyncConnection>::Backend>
        + 'static;
}

#[cfg(feature = "mysql")]
impl AsyncSyncConnectionBridge for diesel_async::AsyncMysqlConnection {
    type SyncConnection = diesel::mysql::MysqlConnection;
}

#[cfg(feature = "postgres")]
impl AsyncSyncConnectionBridge for diesel_async::AsyncPgConnection {
    type SyncConnection = diesel::pg::PgConnection;
}

impl<C: AsyncConnection + PoolableConnection + AsyncSyncConnectionBridge + 'static> _DbPool<C> {
    pub async fn new(
        db_pool_config: DbPoolConfig,
        migrations: impl Into<Option<EmbeddedMigrations>>,
    ) -> Result<Self, Error> {
        let DbPoolConfig {
            database_url,
            database_migration_url,
            max_connections,
        } = db_pool_config;
        let migrations = migrations.into();

        info!("connecting to database");

        let db_pool = Self(Arc::new({
            cfg_if! {
                if #[cfg(feature = "deadpool")] {
                    let mut pool_builder = Pool::builder(AsyncDieselConnectionManager::new(&database_url));
                    if let Some(max_connections) = max_connections {
                        pool_builder = pool_builder.max_size(max_connections);
                    }
                    pool_builder.build().map_err(Error::msg)?
                }
            }
            cfg_if! {
                if #[cfg(feature = "bb8")] {
                    let mut pool_builder = Pool::builder();
                    if let Some(max_connections) = max_connections {
                        pool_builder = pool_builder.max_size(max_connections as u32);
                    }
                    pool_builder.build(AsyncDieselConnectionManager::new(&database_url)).await.map_err(Error::msg)?
                }
            }
        }));
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

    /// expected use:
    /// ```
    /// let db_pool = DbPool::new(...).await?;
    /// let results = a_diesel_table::table.select((a_diesel_table::column)).get_results(lock_conn!(db_pool)).await?;
    /// ```
    pub(crate) async fn get_connection(&self) -> Result<DbConnOwned<C>, diesel::result::Error> {
        let connection = self
            .0
            .get()
            .await
            .map_err(|err| diesel::result::Error::QueryBuilderError(format!("could not get pooled connection to database: {err}").into()))?;
        Ok(DbConnection::from(connection))
    }
}
