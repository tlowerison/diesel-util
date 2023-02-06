use crate::{
    _DbPool, lock_conn, AsyncSyncConnectionBridge, DbError, IsNotDeleted, Page, PageRef, Paginate,
    Paginated, TxCleanupError,
};
use diesel::associations::HasTable;
use diesel::backend::Backend;
use diesel::dsl::SqlTypeOf;
use diesel::expression::{AsExpression, Expression};
use diesel::expression_methods::ExpressionMethods;
use diesel::helper_types as ht;
use diesel::query_builder::*;
use diesel::query_dsl::methods::{FilterDsl, FindDsl};
use diesel::query_source::QuerySource;
use diesel::result::Error;
use diesel::sql_types::SqlType;
use diesel::{Identifiable, Insertable, Table};
use diesel_async::pooled_connection::{deadpool::Object, PoolableConnection};
use diesel_async::{methods::*, AsyncConnection, RunQueryDsl};
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use scoped_futures::{ScopedBoxFuture, ScopedFutureExt};
use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::{fmt::Debug, hash::Hash, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

pub trait Audit: Into<Self::AuditRow> {
    type AuditRow;
    type AuditTable: HasTable;
}

pub trait IncludesChanges {
    fn includes_changes(&self) -> bool;
}

#[derive(AsRef, AsMut, Deref, DerefMut, Derivative)]
#[derivative(Debug)]
pub struct DbConnection<AC, C> {
    #[deref]
    #[deref_mut]
    #[derivative(Debug = "ignore")]
    pub(crate) connection: Arc<RwLock<C>>,
    #[derivative(Debug = "ignore")]
    pub(crate) tx_cleanup: TxCleanup<AC>,
    pub(crate) tx_id: Option<Uuid>,
}

impl<AC, C> Clone for DbConnection<AC, C> {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            tx_cleanup: self.tx_cleanup.clone(),
            tx_id: self.tx_id,
        }
    }
}

pub type DbConnOwned<AC> = DbConnection<AC, Object<AC>>;
pub type DbConnRef<'a, AC> = DbConnection<AC, &'a mut AC>;

impl<C: PoolableConnection + 'static> From<Object<C>> for DbConnOwned<C> {
    fn from(connection: Object<C>) -> Self {
        DbConnection {
            tx_id: None,
            connection: Arc::new(RwLock::new(connection)),
            tx_cleanup: Arc::new(Mutex::new(vec![])),
        }
    }
}

pub trait DieselUtilAsyncConnection =
    AsyncConnection + AsyncSyncConnectionBridge + PoolableConnection;

pub trait TxFn<'a, C, Fut>: FnOnce(C) -> Fut + Send + 'a {
    fn call_tx_fn(self, connection: C) -> Fut;
}

impl<'a, C, F, Fut> TxFn<'a, C, Fut> for F
where
    C: Send,
    F: FnOnce(C) -> Fut + Send + 'a,
{
    fn call_tx_fn(self, connection: C) -> Fut {
        (self)(connection)
    }
}

pub trait TxCleanupFn<'r, AC: 'r, E> =
    FnOnce(&'r DbConnRef<'r, AC>) -> BoxFuture<'r, Result<(), E>> + Send + Sync + 'static;

pub type TxCleanup<AC> = Arc<Mutex<Vec<Box<dyn for<'r> TxCleanupFn<'r, AC, TxCleanupError>>>>>;

impl<'a, AC, C> From<DbConnection<AC, C>> for Cow<'a, DbConnection<AC, C>> {
    fn from(value: DbConnection<AC, C>) -> Self {
        Cow::Owned(value)
    }
}

impl<'a, AC, C> From<&'a DbConnection<AC, C>> for Cow<'a, DbConnection<AC, C>> {
    fn from(value: &'a DbConnection<AC, C>) -> Self {
        Cow::Borrowed(value)
    }
}

pub type InsertStmt<Table, T> =
    InsertStatement<Table, BatchInsert<Vec<<T as Insertable<Table>>::Values>, Table, (), false>>;

/// _Db represents a shared reference to a mutable async db connection
/// It abstracts over the case where the connection is owned vs. a mutable reference.
/// The main goal of this abstraction is to defer locking the access to the
/// connection until the latest point possible, allowing other code paths (excepting
/// for connections in transactions) to access the connection as well.
/// At the moment, Db is passed in by value instead of reference into the transaction
/// provided transaction wrapper so you'll need to use `&db_conn` instead of just `db_conn`.
///
/// The trait is prefixed with _ to imply that applications using it will likely want to export
/// their own trait alias for it with the appropriate backend specified -- otherwise generic
/// functions taking a reference to a type implementing _Db will likely have a rough time
/// actually executing any sql times (in terms of parsing through the compile errors).
#[async_trait]
pub trait _Db: Clone + Debug + Send + Sync + Sized {
    type Backend: Backend;
    type AsyncConnection: AsyncConnection<Backend = Self::Backend> + 'static;
    type Connection: DerefMut<Target = Self::AsyncConnection> + Send + Sync;
    type TxConnection<'r>: _Db<Backend = Self::Backend, AsyncConnection = Self::AsyncConnection>;

    async fn connection(&self) -> Result<Cow<'_, Arc<RwLock<Self::Connection>>>, Error>;

    fn tx_id(&self) -> Option<Uuid>;

    async fn tx_cleanup<F, E>(&self, f: F)
    where
        F: for<'r> TxCleanupFn<'r, Self::AsyncConnection, E>,
        E: Into<TxCleanupError> + 'static;

    async fn tx<'s, 'a, T, E, F>(&'s self, callback: F) -> Result<T, E>
    where
        F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
        's: 'a;

    async fn raw_tx<'a, T, E, F>(&self, callback: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a;

    #[framed]
    #[instrument(err(Debug), skip(self))]
    async fn get<'query, R, T, Pk, F, I>(&self, ids: I) -> Result<Vec<R>, Error>
    where
        Pk: AsExpression<SqlTypeOf<T::PrimaryKey>>,
        I: Debug + IntoIterator<Item = Pk> + Send,
        <I as IntoIterator>::IntoIter: Send + ExactSizeIterator,
        R: Send + HasTable<Table = T>,
        T: Table,
        T::PrimaryKey: Expression + ExpressionMethods,
        <T::PrimaryKey as Expression>::SqlType: SqlType,
        T: FilterDsl<ht::EqAny<<T as Table>::PrimaryKey, I>, Output = F>,
        F: IsNotDeleted<'query, Self::AsyncConnection, R, R>,
    {
        let ids = ids.into_iter();
        if ids.len() == 0 {
            return Ok(vec![]);
        }
        // in block to prevent additional constraints on things requiring Send / Sync because they "might be used again" after the return
        let query = {
            R::table()
                .filter(R::table().primary_key().eq_any(ids))
                .is_not_deleted()
        };
        Ok(query.get_results(lock_conn!(self)).await?)
    }

    #[framed]
    #[instrument(err(Debug), skip(self))]
    async fn get_by_column<'query, R, U, Q, C>(
        &self,
        c: C,
        values: impl IntoIterator<Item = U> + Debug + Send,
    ) -> Result<Vec<R>, Error>
    where
        U: AsExpression<SqlTypeOf<C>>,
        C: Debug + Expression + ExpressionMethods + Send,
        SqlTypeOf<C>: SqlType,
        R: Send + HasTable,
        <R as HasTable>::Table: Table + IsNotDeleted<'query, Self::AsyncConnection, R, R>,
        <<R as HasTable>::Table as IsNotDeleted<'query, Self::AsyncConnection, R, R>>::IsNotDeletedFilter:
            FilterDsl<ht::EqAny<C, Vec<U>>, Output = Q>,
        Q: Send + LoadQuery<'query, Self::AsyncConnection, R> + 'query,
    {
        // in block to prevent additional constraints on things requiring Send / Sync because they "might be used again" after the return
        let query = {
            R::table()
                .is_not_deleted()
                .filter(c.eq_any(values.into_iter().collect::<Vec<_>>()))
        };
        Ok(query.get_results(lock_conn!(self)).await?)
    }

    #[framed]
    #[instrument(err(Debug), skip(self))]
    async fn get_page<'query, R, P, F>(&self, page: P) -> Result<Vec<R>, Error>
    where
        P: Borrow<Page> + Debug + Send,
        R: Send + HasTable,
        <R as HasTable>::Table:
            Table + IsNotDeleted<'query, Self::AsyncConnection, R, R, IsNotDeletedFilter = F>,
        F: Paginate + Send,
        Paginated<<F as AsQuery>::Query>:
            Send + LoadQuery<'query, Self::AsyncConnection, R> + 'query,
    {
        if page.borrow().is_empty() {
            return Ok(vec![]);
        }
        Ok(R::table()
            .is_not_deleted()
            .paginate(page)
            .get_results(lock_conn!(self))
            .await?)
    }

    #[framed]
    #[instrument(err(Debug), skip(self))]
    async fn get_pages<'query, R, P, I, F>(&self, pages: I) -> Result<Vec<R>, Error>
    where
        P: Debug + for<'a> PageRef<'a> + Send,
        I: Debug + IntoIterator<Item = P> + Send,
        <I as IntoIterator>::IntoIter: Send,
        R: Send + HasTable,
        <R as HasTable>::Table:
            Table + IsNotDeleted<'query, Self::AsyncConnection, R, R, IsNotDeletedFilter = F>,
        F: Paginate + Send,
        Paginated<<F as AsQuery>::Query>:
            Send + LoadQuery<'query, Self::AsyncConnection, R> + 'query,
    {
        Ok(R::table()
            .is_not_deleted()
            .multipaginate(pages.into_iter())
            .get_results(lock_conn!(self))
            .await?)
    }

    #[framed]
    #[instrument(err(Debug), skip(self, values))]
    async fn insert<R, I, U>(&self, values: I) -> Result<Vec<R>, DbError>
    where
        U: HasTable + Send,
        <U as HasTable>::Table: Table + QueryId + Send,
        <<U as HasTable>::Table as QuerySource>::FromClause: Send,

        I: IntoIterator<Item = U> + Send,
        Vec<U>: Insertable<<U as HasTable>::Table>,
        <Vec<U> as Insertable<<U as HasTable>::Table>>::Values: Send,
        R: Send,
        InsertStatement<
            <U as HasTable>::Table,
            <Vec<U> as Insertable<<U as HasTable>::Table>>::Values,
        >: for<'query> LoadQuery<'query, Self::AsyncConnection, R>,

        R: Audit + Clone + Send,
        <R as Audit>::AuditRow: Send,
        Vec<<R as Audit>::AuditRow>: Insertable<<<R as Audit>::AuditTable as HasTable>::Table>
            + UndecoratedInsertRecord<<<R as Audit>::AuditTable as HasTable>::Table>,
        <<R as Audit>::AuditTable as HasTable>::Table: Table + QueryId + Send,
        <<<R as Audit>::AuditTable as HasTable>::Table as QuerySource>::FromClause: Send,
        InsertStatement<
            <<R as Audit>::AuditTable as HasTable>::Table,
            <Vec<<R as Audit>::AuditRow> as Insertable<
                <<R as Audit>::AuditTable as HasTable>::Table,
            >>::Values,
        >: for<'query> LoadQuery<'query, Self::AsyncConnection, <R as Audit>::AuditRow>,
        <Vec<<R as Audit>::AuditRow> as Insertable<
            <<R as Audit>::AuditTable as HasTable>::Table,
        >>::Values: Send,
    {
        self.raw_tx(move |db_conn| {
            async move {
                let values = values.into_iter().collect::<Vec<_>>();
                if values.is_empty() {
                    return Ok(vec![]);
                }

                let all_inserted = diesel::insert_into(U::table())
                    .values(values)
                    .get_results::<R>(db_conn)
                    .await?;

                let audit_posts: Vec<<R as Audit>::AuditRow> = all_inserted
                    .clone()
                    .into_iter()
                    .map(|inserted| inserted.into())
                    .collect();

                // TODO: replace usage of `get_result` with `execute` instead of get_result - currently `ExecuteDsl: Sized` cannot be computed for some reason
                diesel::insert_into(<<R as Audit>::AuditTable as HasTable>::table())
                    .values(audit_posts)
                    .get_result::<<R as Audit>::AuditRow>(db_conn)
                    .await?;

                Ok(all_inserted)
            }
            .scope_boxed()
        })
        .await
    }

    #[framed]
    #[instrument(err(Debug), skip(self, value))]
    async fn insert_one<R, I>(&self, value: I) -> Result<R, DbError>
    where
        I: HasTable + Insertable<<I as HasTable>::Table> + Send,
        <I as Insertable<<I as HasTable>::Table>>::Values: Send,
        <I as HasTable>::Table: Table + QueryId + Send,
        <<I as HasTable>::Table as QuerySource>::FromClause: Send,
        InsertStatement<<I as HasTable>::Table, <I as Insertable<<I as HasTable>::Table>>::Values>:
            for<'query> LoadQuery<'query, Self::AsyncConnection, R>,

        R: Audit + Clone + Send,
        <R as Audit>::AuditRow: Send,
        <R as Audit>::AuditRow: Insertable<<<R as Audit>::AuditTable as HasTable>::Table>
            + UndecoratedInsertRecord<<<R as Audit>::AuditTable as HasTable>::Table>,
        <<R as Audit>::AuditTable as HasTable>::Table: Table + QueryId + Send,
        <<<R as Audit>::AuditTable as HasTable>::Table as QuerySource>::FromClause: Send,
        InsertStatement<
            <<R as Audit>::AuditTable as HasTable>::Table,
            <<R as Audit>::AuditRow as Insertable<<<R as Audit>::AuditTable as HasTable>::Table>>::Values,
        >: for<'query> LoadQuery<'query, Self::AsyncConnection, <R as Audit>::AuditRow>,
        <<R as Audit>::AuditRow as Insertable<<<R as Audit>::AuditTable as HasTable>::Table>>::Values: Send,
    {
        self.raw_tx(move |db_conn| {
            async move {
                let inserted = diesel::insert_into(I::table())
                    .values(value)
                    .get_result::<R>(db_conn)
                    .await?;

                let audit_post: <R as Audit>::AuditRow = inserted.clone().into();

                // TODO: replace usage of `get_result` with `execute` instead of get_result - currently `ExecuteDsl: Sized` cannot be computed for some reason
                diesel::insert_into(<<R as Audit>::AuditTable as HasTable>::table())
                    .values(audit_post)
                    .get_result::<<R as Audit>::AuditRow>(db_conn)
                    .await?;

                Ok(inserted)
            }
            .scope_boxed()
        })
        .await
    }

    #[framed]
    #[instrument(err(Debug), skip(self, values))]
    async fn insert_without_audit<R, I, U>(&self, values: I) -> Result<Vec<R>, DbError>
    where
        U: HasTable + Send,
        <U as HasTable>::Table: Table + QueryId + Send,
        <<U as HasTable>::Table as QuerySource>::FromClause: Send,

        R: Send,

        I: IntoIterator<Item = U> + Send,
        Vec<U>: Insertable<<U as HasTable>::Table>,
        <Vec<U> as Insertable<<U as HasTable>::Table>>::Values: Send,
        InsertStatement<
            <U as HasTable>::Table,
            <Vec<U> as Insertable<<U as HasTable>::Table>>::Values,
        >: for<'query> LoadQuery<'query, Self::AsyncConnection, R>,
    {
        let values = values.into_iter().collect::<Vec<_>>();
        if values.is_empty() {
            return Ok(vec![]);
        }
        Ok(diesel::insert_into(U::table())
            .values(values)
            .get_results::<R>(lock_conn!(self))
            .await?)
    }

    #[framed]
    #[instrument(err(Debug), skip(self, value))]
    async fn insert_one_without_audit<R, I>(&self, value: I) -> Result<R, DbError>
    where
        I: HasTable + Insertable<<I as HasTable>::Table> + Send,
        <I as Insertable<<I as HasTable>::Table>>::Values: Send,

        R: Send,

        <I as HasTable>::Table: Table + QueryId + Send,
        <<I as HasTable>::Table as QuerySource>::FromClause: Send,
        InsertStatement<<I as HasTable>::Table, <I as Insertable<<I as HasTable>::Table>>::Values>:
            for<'query> LoadQuery<'query, Self::AsyncConnection, R>,
    {
        Ok(diesel::insert_into(I::table())
            .values(value)
            .get_result::<R>(lock_conn!(self))
            .await?)
    }

    #[framed]
    #[instrument(err(Debug), skip(self, patches))]
    async fn update<R, I, U, T, Pk, F>(&self, patches: I) -> Result<Vec<R>, DbError>
    where
        U: AsChangeset<Target = T> + HasTable<Table = T> + IncludesChanges + Send + Sync,
        for<'a> &'a U: HasTable<Table = T> + Identifiable<Id = &'a Pk> + IntoUpdateTarget,
        <U as AsChangeset>::Changeset: Send,
        for<'a> <&'a U as IntoUpdateTarget>::WhereClause: Send,

        ht::Find<T, Pk>: IntoUpdateTarget,
        <ht::Find<T, Pk> as IntoUpdateTarget>::WhereClause: Send,
        for<'a> ht::Update<ht::Find<T, Pk>, U>:
            AsQuery + LoadQuery<'a, Self::AsyncConnection, R> + Send,

        Pk: AsExpression<SqlTypeOf<T::PrimaryKey>> + Clone + Hash + Eq + Send + Sync,

        T: FindDsl<Pk> + Table + Send,
        ht::Find<T, Pk>: HasTable<Table = T> + Send,
        <T as QuerySource>::FromClause: Send,

        I: IntoIterator<Item = U> + Send,
        R: Send,
        for<'a> &'a R: Identifiable<Id = &'a Pk>,

        // Audit bounds
        R: Audit + Clone,
        <R as Audit>::AuditRow: Send,
        Vec<<R as Audit>::AuditRow>: Insertable<<<R as Audit>::AuditTable as HasTable>::Table>
            + UndecoratedInsertRecord<<<R as Audit>::AuditTable as HasTable>::Table>,
        <<R as Audit>::AuditTable as HasTable>::Table: Table + QueryId + Send,
        <<<R as Audit>::AuditTable as HasTable>::Table as QuerySource>::FromClause: Send,
        <Vec<<R as Audit>::AuditRow> as Insertable<
            <<R as Audit>::AuditTable as HasTable>::Table,
        >>::Values: Send,
        InsertStatement<
            <<R as Audit>::AuditTable as HasTable>::Table,
            <Vec<<R as Audit>::AuditRow> as Insertable<
                <<R as Audit>::AuditTable as HasTable>::Table,
            >>::Values,
        >: for<'a> LoadQuery<'a, Self::AsyncConnection, <R as Audit>::AuditRow>,

        T: Table,
        T::PrimaryKey: Expression + ExpressionMethods,
        <T::PrimaryKey as Expression>::SqlType: SqlType,
        T: FilterDsl<ht::EqAny<<T as Table>::PrimaryKey, Vec<Pk>>, Output = F>,
        F: for<'a> IsNotDeleted<'a, Self::AsyncConnection, R, R>,
    {
        let patches = patches.into_iter().collect::<Vec<U>>();
        let ids = patches
            .iter()
            .map(|patch| patch.id().clone())
            .collect::<Vec<_>>();

        self.raw_tx(move |db_conn| {
            async move {
                let no_change_patch_ids = patches
                    .iter()
                    .filter_map(|patch| {
                        if !patch.includes_changes() {
                            Some(patch.id().to_owned())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                let num_changed_patches = ids.len() - no_change_patch_ids.len();
                if num_changed_patches == 0 {
                    return Ok(vec![]);
                }
                // TODO: switch to parallel requests in the future -- requires DbConn to be cloneable
                let mut all_updated = Vec::with_capacity(num_changed_patches);
                for patch in patches.into_iter().filter(|patch| patch.includes_changes()) {
                    let record = diesel::update(U::table().find(patch.id().to_owned()))
                        .set(patch)
                        .get_result::<R>(db_conn)
                        .await?;
                    all_updated.push(record);
                }

                let audit_posts: Vec<<R as Audit>::AuditRow> = all_updated
                    .clone()
                    .into_iter()
                    .map(|record| record.into())
                    .collect();

                // TODO: replace usage of `get_result` with `execute` instead of get_result - currently `ExecuteDsl: Sized` cannot be computed for some reason
                diesel::insert_into(<<R as Audit>::AuditTable as HasTable>::table())
                    .values(audit_posts)
                    .get_result::<<R as Audit>::AuditRow>(db_conn)
                    .await?;

                let filter = FilterDsl::filter(
                    U::table(),
                    U::table().primary_key().eq_any(no_change_patch_ids),
                )
                .is_not_deleted();
                let unchanged_records = filter.get_results::<R>(&mut *db_conn).await?;

                let mut all_records = unchanged_records
                    .into_iter()
                    .chain(all_updated.into_iter())
                    .map(|record| (record.id().clone(), record))
                    .collect::<HashMap<_, _>>();

                Ok(ids
                    .iter()
                    .map(|id| all_records.remove(id).unwrap())
                    .collect::<Vec<_>>())
            }
            .scope_boxed()
        })
        .await
    }
}

#[async_trait]
impl<C> _Db for DbConnOwned<C>
where
    C: AsyncConnection + PoolableConnection + Sync + 'static,
{
    type Backend = <C as AsyncConnection>::Backend;
    type AsyncConnection = C;
    type Connection = Object<C>;
    type TxConnection<'r> = Cow<'r, DbConnRef<'r, Self::AsyncConnection>>;

    async fn connection(&self) -> Result<Cow<'_, Arc<RwLock<Self::Connection>>>, Error> {
        Ok(Cow::Borrowed(self.deref()))
    }

    fn tx_id(&self) -> Option<Uuid> {
        self.tx_id
    }

    #[framed]
    async fn tx_cleanup<F, E>(&self, f: F)
    where
        F: for<'r> TxCleanupFn<'r, Self::AsyncConnection, E>,
        E: Into<TxCleanupError> + 'static,
    {
        let mut tx_cleanup = self.tx_cleanup.lock().await;
        tx_cleanup.push(Box::new(|x| f(x).map_err(Into::into).boxed()));
    }

    #[framed]
    async fn tx<'s, 'a, T, E, F>(&'s self, callback: F) -> Result<T, E>
    where
        F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
        's: 'a,
    {
        let tx_cleanup = <Self as AsRef<TxCleanup<Self::AsyncConnection>>>::as_ref(self).clone();
        lock_conn!(self)
            .transaction(move |connection| {
                async move {
                    let db_connection = DbConnection {
                        tx_id: Some(Uuid::new_v4()),
                        connection: Arc::new(RwLock::new(connection)),
                        tx_cleanup: tx_cleanup.clone(),
                    };
                    let value = callback.call_tx_fn(Cow::Borrowed(&db_connection)).await?;
                    let mut tx_cleanup = tx_cleanup.lock().await;
                    for tx_cleanup_fn in tx_cleanup.drain(..) {
                        tx_cleanup_fn(&db_connection).await?;
                    }
                    Ok::<T, E>(value)
                }
                .scope_boxed()
            })
            .await
    }

    #[framed]
    async fn raw_tx<'a, T, E, F>(&self, callback: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
    {
        let tx_cleanup = <Self as AsRef<TxCleanup<Self::AsyncConnection>>>::as_ref(self).clone();
        self.connection()
            .await?
            .write()
            .await
            .deref_mut()
            .transaction(move |connection| {
                async move {
                    let value = callback(connection).await?;
                    let db_connection = DbConnection {
                        tx_id: Some(Uuid::new_v4()),
                        connection: Arc::new(RwLock::new(connection.as_mut())),
                        tx_cleanup: tx_cleanup.clone(),
                    };
                    let mut tx_cleanup = tx_cleanup.lock().await;
                    for tx_cleanup_fn in tx_cleanup.drain(..) {
                        tx_cleanup_fn(&db_connection).await?;
                    }
                    Ok(value)
                }
                .scope_boxed()
            })
            .await
    }
}

#[async_trait]
impl<'d, C> _Db for DbConnRef<'d, C>
where
    C: AsyncConnection + PoolableConnection + Sync + 'static,
{
    type Backend = <C as AsyncConnection>::Backend;
    type AsyncConnection = C;
    type Connection = &'d mut C;
    type TxConnection<'r> = Cow<'r, DbConnRef<'r, Self::AsyncConnection>>;

    async fn connection(&self) -> Result<Cow<'_, Arc<RwLock<Self::Connection>>>, Error> {
        Ok(Cow::Borrowed(&self.connection))
    }

    fn tx_id(&self) -> Option<Uuid> {
        self.tx_id
    }

    #[framed]
    async fn tx_cleanup<F, E>(&self, f: F)
    where
        F: for<'r> TxCleanupFn<'r, Self::AsyncConnection, E>,
        E: Into<TxCleanupError> + 'static,
    {
        let mut tx_cleanup = self.tx_cleanup.lock().await;
        tx_cleanup.push(Box::new(|x| f(x).map_err(Into::into).boxed()));
    }

    async fn tx<'s, 'a, T, E, F>(&'s self, callback: F) -> Result<T, E>
    where
        F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
        's: 'a,
    {
        let tx_id = self.tx_id;
        let tx_cleanup = <Self as AsRef<TxCleanup<Self::AsyncConnection>>>::as_ref(self).clone();
        lock_conn!(self)
            .transaction(move |connection| {
                let db_connection = DbConnection {
                    connection: Arc::new(RwLock::new(connection)),
                    tx_cleanup: tx_cleanup.clone(),
                    tx_id,
                };
                callback.call_tx_fn(Cow::Owned(db_connection)).scope_boxed()
            })
            .await
    }

    #[framed]
    async fn raw_tx<'a, T, E, F>(&self, callback: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
    {
        lock_conn!(self)
            .transaction(move |connection| async move { callback(connection).await }.scope_boxed())
            .await
    }
}

#[async_trait]
impl<C> _Db for _DbPool<C>
where
    C: AsyncConnection + AsyncSyncConnectionBridge + PoolableConnection + Sync + 'static,
{
    type Backend = <C as AsyncConnection>::Backend;
    type AsyncConnection = C;
    type Connection = Object<C>;
    type TxConnection<'r> = Cow<'r, DbConnRef<'r, Self::AsyncConnection>>;

    async fn connection(&self) -> Result<Cow<'_, Arc<RwLock<Self::Connection>>>, Error> {
        self.get_connection()
            .await
            .map(|db_connection| Cow::Owned(db_connection.connection))
            .map_err(|err| {
                Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::UnableToSendCommand,
                    Box::new(format!("{err}")),
                )
            })
    }

    fn tx_id(&self) -> Option<Uuid> {
        None
    }

    async fn tx_cleanup<F, E>(&self, _: F)
    where
        F: for<'r> TxCleanupFn<'r, Self::AsyncConnection, E>,
        E: Into<TxCleanupError> + 'static,
    {
    }

    #[framed]
    async fn tx<'s, 'a, T, E, F>(&'s self, callback: F) -> Result<T, E>
    where
        F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
        's: 'a,
    {
        self.raw_tx(|async_connection| {
            async move {
                let tx_cleanup = TxCleanup::default();
                let db_connection = DbConnection {
                    tx_id: Some(Uuid::new_v4()),
                    connection: Arc::new(RwLock::new(async_connection)),
                    tx_cleanup: tx_cleanup.clone(),
                };
                let value = callback.call_tx_fn(Cow::Borrowed(&db_connection)).await?;
                let mut tx_cleanup = tx_cleanup.lock().await;
                for tx_cleanup_fn in tx_cleanup.drain(..) {
                    tx_cleanup_fn(&db_connection).await?;
                }
                Ok::<T, E>(value)
            }
            .scope_boxed()
        })
        .await
    }

    #[framed]
    async fn raw_tx<'a, T, E, F>(&self, callback: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
    {
        self.connection()
            .await?
            .write()
            .await
            .deref_mut()
            .transaction(move |async_connection| {
                async move {
                    let value = callback(async_connection).await?;

                    let tx_cleanup = TxCleanup::default();
                    let db_connection = DbConnection {
                        tx_id: Some(Uuid::new_v4()),
                        connection: Arc::new(RwLock::new(async_connection.as_mut())),
                        tx_cleanup: tx_cleanup.clone(),
                    };
                    let mut tx_cleanup = tx_cleanup.lock().await;
                    for tx_cleanup_fn in tx_cleanup.drain(..) {
                        tx_cleanup_fn(&db_connection).await?;
                    }
                    Ok(value)
                }
                .scope_boxed()
            })
            .await
    }
}

#[async_trait]
impl<'d, D: _Db + Clone> _Db for Cow<'d, D> {
    type Backend = <D::AsyncConnection as AsyncConnection>::Backend;
    type AsyncConnection = D::AsyncConnection;
    type Connection = D::Connection;
    type TxConnection<'r> = D::TxConnection<'r>;

    async fn connection(&self) -> Result<Cow<'_, Arc<RwLock<Self::Connection>>>, Error> {
        (**self).connection().await
    }

    fn tx_id(&self) -> Option<Uuid> {
        (**self).tx_id()
    }

    #[framed]
    async fn tx_cleanup<F, E>(&self, f: F)
    where
        F: for<'r> TxCleanupFn<'r, Self::AsyncConnection, E>,
        E: Into<TxCleanupError> + 'static,
    {
        (**self).tx_cleanup(f).await
    }

    async fn tx<'s, 'a, T, E, F>(&'s self, callback: F) -> Result<T, E>
    where
        F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
        's: 'a,
    {
        (**self).tx(callback).await
    }

    async fn raw_tx<'a, T, E, F>(&self, callback: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
    {
        (**self).raw_tx(callback).await
    }
}

#[async_trait]
impl<'d, D: _Db + Clone> _Db for &'d D {
    type Backend = <D::AsyncConnection as AsyncConnection>::Backend;
    type AsyncConnection = D::AsyncConnection;
    type Connection = D::Connection;
    type TxConnection<'r> = D::TxConnection<'r>;

    async fn connection(&self) -> Result<Cow<'_, Arc<RwLock<Self::Connection>>>, Error> {
        (**self).connection().await
    }

    fn tx_id(&self) -> Option<Uuid> {
        (**self).tx_id()
    }

    #[framed]
    async fn tx_cleanup<F, E>(&self, f: F)
    where
        F: for<'r> TxCleanupFn<'r, Self::AsyncConnection, E>,
        E: Into<TxCleanupError> + 'static,
    {
        (**self).tx_cleanup(f).await
    }

    async fn tx<'s, 'a, T, E, F>(&'s self, callback: F) -> Result<T, E>
    where
        F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
        's: 'a,
    {
        (**self).tx(callback).await
    }

    async fn raw_tx<'a, T, E, F>(&self, callback: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
    {
        (**self).raw_tx(callback).await
    }
}
