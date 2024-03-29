use crate::*;
use ::async_backtrace::{backtrace, Location};
use ::diesel::associations::HasTable;
use ::diesel::backend::Backend;
use ::diesel::deserialize::FromSqlRow;
use ::diesel::dsl::SqlTypeOf;
use ::diesel::expression::{AsExpression, Expression, QueryMetadata, ValidGrouping};
use ::diesel::expression_methods::ExpressionMethods;
use ::diesel::helper_types as ht;
use ::diesel::query_builder::*;
use ::diesel::query_dsl::methods::{FilterDsl, FindDsl};
use ::diesel::query_dsl::select_dsl::SelectDsl;
use ::diesel::query_source::QuerySource;
use ::diesel::result::Error;
use ::diesel::serialize::ToSql;
use ::diesel::sql_types::{BigInt, SqlType};
use ::diesel::SelectableExpression;
use ::diesel::{Identifiable, Insertable, Table};
use ::diesel_async::{methods::*, AsyncConnection, RunQueryDsl};
use ::futures::future::{ready, BoxFuture, FutureExt, TryFutureExt};
use ::page_util::*;
use ::scoped_futures::{ScopedBoxFuture, ScopedFutureExt};
use ::std::borrow::{Borrow, Cow};
use ::std::collections::HashMap;
use ::std::fmt::Display;
use ::std::ops::Deref;
use ::std::{fmt::Debug, hash::Hash, sync::Arc};
use ::tokio::sync::{Mutex, RwLock};
use ::uuid::Uuid;

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

pub type DbConnRef<'a, C> = DbConnection<C, &'a mut C>;

#[derive(Derivative, thiserror::Error)]
#[derivative(Debug)]
#[error("{source}")]
pub struct TxCleanupError {
    pub source: InternalError,
    #[derivative(Debug = "ignore")]
    pub backtrace: Option<Box<[Location]>>,
}

impl TxCleanupError {
    #[framed]
    pub fn new<S: Display + Debug + Send + Sync + 'static>(msg: S) -> Self {
        Self {
            source: InternalError::msg(msg),
            backtrace: backtrace(),
        }
    }
}

impl From<InternalError> for TxCleanupError {
    #[framed]
    fn from(source: InternalError) -> Self {
        Self {
            source,
            backtrace: backtrace(),
        }
    }
}

impl From<TxCleanupError> for Error {
    fn from(_value: TxCleanupError) -> Self {
        cfg_if! {
            if #[cfg(feature = "tracing")] {
                error!("transaction cleanup error occurred within a transaction whose error type is diesel::result::Error, converting error to diesel::result::Error::RollbackTransaction");
                error!("original error: {_value}");
            }
        }
        Self::RollbackTransaction
    }
}

cfg_if! {
    if #[cfg(feature = "bb8")] {
        use diesel_async::pooled_connection::bb8::PooledConnection;

        pub type DbConnOwned<'r, C> = DbConnection<C, PooledConnection<'r, C>>;

        impl<'a, C: PoolableConnection + 'static> From<PooledConnection<'a, C>> for DbConnOwned<'a, C> {
            fn from(connection: PooledConnection<'a, C>) -> Self {
                DbConnection {
                    tx_id: None,
                    connection: Arc::new(RwLock::new(connection)),
                    tx_cleanup: Arc::new(Mutex::new(vec![])),
                }
            }
        }
    }
}

cfg_if! {
    if #[cfg(feature = "deadpool")] {
        use diesel_async::pooled_connection::deadpool::Object;

        type PooledConnection<'a, C> = Object<C>;
        pub type DbConnOwned<'r, C> = DbConnection<C, Object<C>>;

        impl<'a, C: PoolableConnection + 'static> From<PooledConnection<'a, C>> for DbConnOwned<'a, C> {
            fn from(connection: PooledConnection<'a, C>) -> Self {
                DbConnection {
                    tx_id: None,
                    connection: Arc::new(RwLock::new(connection)),
                    tx_cleanup: Arc::new(Mutex::new(vec![])),
                }
            }
        }

    }
}

cfg_if! {
    if #[cfg(feature = "mobc")] {
        use diesel_async::pooled_connection::AsyncDieselConnectionManager;
        use mobc::Connection;

        type PooledConnection<'a, C> = Connection<AsyncDieselConnectionManager<C>>;
        pub type DbConnOwned<'r, C> = DbConnection<C, Connection<AsyncDieselConnectionManager<C>>>;

        impl<'a, C: PoolableConnection + 'static> From<PooledConnection<'a, C>> for DbConnOwned<'a, C> {
            fn from(connection: PooledConnection<'a, C>) -> Self {
                DbConnection {
                    tx_id: None,
                    connection: Arc::new(RwLock::new(connection)),
                    tx_cleanup: Arc::new(Mutex::new(vec![])),
                }
            }
        }
    }
}

cfg_if! {
    if #[cfg(any(feature = "bb8", feature = "deadpool", feature = "mobc"))] {
        use diesel_async::pooled_connection::PoolableConnection;
        use std::ops::DerefMut;
    }
}

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

impl<AC, C> From<DbConnection<AC, C>> for Cow<'_, DbConnection<AC, C>> {
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
/// provided transaction wrapper so you'll need to use `&conn` instead of just `conn`.
///
/// The trait is prefixed with _ to imply that applications using it will likely want to export
/// their own trait alias for it with the appropriate backend specified -- otherwise generic
/// functions taking a reference to a type implementing _Db will likely have a rough time
/// actually executing any sql times (in terms of parsing through the compile errors).
#[async_trait]
pub trait _Db: Clone + Debug + Send + Sync + Sized {
    type Backend: Backend + Send + 'static;
    type AsyncConnection: AsyncConnection<Backend = Self::Backend> + 'static;
    type Connection<'r>: Deref<Target = Self::AsyncConnection> + Send + Sync
    where
        Self: 'r;
    type TxConnection<'r>: _Db<Backend = Self::Backend, AsyncConnection = Self::AsyncConnection>;

    async fn query<'a, F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>> + Send + 'a,
        E: Debug + From<Error> + Send + 'a,
        T: Send + 'a;

    async fn with_tx_connection<'a, F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>> + Send + 'a,
        E: Debug + From<Error> + Send + 'a,
        T: Send + 'a;

    fn tx_id(&self) -> Option<Uuid>;

    async fn tx_cleanup<F, E>(&self, f: F)
    where
        F: for<'r> TxCleanupFn<'r, Self::AsyncConnection, E>,
        E: Into<TxCleanupError> + 'static;

    async fn tx<'life0, 'a, T, E, F>(&'life0 self, callback: F) -> Result<T, E>
    where
        F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
        'life0: 'a;

    async fn raw_tx<'a, T, E, F>(&self, callback: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>> + Send + 'a,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
    {
        self.with_tx_connection(callback).await
    }

    #[framed]
    fn get<'life0, 'async_trait, 'query, R, T, Pk, F, I, S>(
        &'life0 self,
        ids: I,
        selection: S,
    ) -> BoxFuture<'async_trait, Result<Vec<R>, Error>>
    where
        Pk: AsExpression<SqlTypeOf<T::PrimaryKey>>,
        I: Debug + IntoIterator<Item = Pk> + Send,
        <I as IntoIterator>::IntoIter: Send + ExactSizeIterator,

        R: Send + HasTable<Table = T>,
        T: Table + SelectDsl<S>,
        ht::Select<T, S>: FilterDsl<ht::EqAny<<T as Table>::PrimaryKey, I>, Output = F>,

        T::PrimaryKey: Expression + ExpressionMethods,
        <T::PrimaryKey as Expression>::SqlType: SqlType,

        F: IsNotDeleted<'query, Self::AsyncConnection, R, R>,
        <F as IsNotDeleted<'query, Self::AsyncConnection, R, R>>::IsNotDeletedFilter:
            LoadQuery<'query, Self::AsyncConnection, R> + Send,

        S: SelectableExpression<T> + Send + ValidGrouping<()> + 'query,

        'life0: 'async_trait,
        'query: 'async_trait,
        R: 'async_trait,
        T: 'async_trait,
        Pk: 'async_trait,
        F: 'async_trait,
        I: 'async_trait,
        S: 'async_trait,
        Self: 'life0,
    {
        let ids = ids.into_iter();
        if ids.len() == 0 {
            return Box::pin(ready(Ok(vec![])));
        }
        let query = R::table()
            .select(selection)
            .filter(R::table().primary_key().eq_any(ids))
            .is_not_deleted();
        self.query(move |conn| Box::pin(query.get_results(conn)))
    }

    #[framed]
    fn get_by_column<'life0, 'async_trait, 'query, R, U, Q, C, S>(
        &'life0 self,
        c: C,
        values: impl IntoIterator<Item = U> + Debug + Send,
        selection: S,
    ) -> BoxFuture<'async_trait, Result<Vec<R>, Error>>
    where
        U: AsExpression<SqlTypeOf<C>>,
        C: Debug + Expression + ExpressionMethods + Send,
        SqlTypeOf<C>: SqlType,

        R: Send + HasTable,
        <R as HasTable>::Table: Table + SelectDsl<S>,
        ht::Select<<R as HasTable>::Table, S>: FilterDsl<ht::EqAny<C, Vec<U>>, Output = Q>,

        Q: IsNotDeleted<'query, Self::AsyncConnection, R, R>,
        <Q as IsNotDeleted<'query, Self::AsyncConnection, R, R>>::IsNotDeletedFilter:
            LoadQuery<'query, Self::AsyncConnection, R> + Send,

        S: SelectableExpression<<R as HasTable>::Table> + Send + ValidGrouping<()> + 'query,

        'life0: 'async_trait,
        'query: 'async_trait,
        R: 'async_trait,
        U: 'async_trait,
        Q: 'async_trait,
        C: 'async_trait,
        S: 'async_trait,
        Self: 'life0,
    {
        let query = R::table()
            .select(selection)
            .filter(c.eq_any(values.into_iter().collect::<Vec<_>>()))
            .is_not_deleted();
        self.query(move |conn| Box::pin(query.get_results(conn)))
    }

    #[framed]
    fn get_page<'life0, 'async_trait, 'query, R, P, F, S>(
        &'life0 self,
        page: P,
        selection: S,
    ) -> BoxFuture<'async_trait, Result<Vec<R>, Error>>
    where
        P: Borrow<DbPage<R::Table>> + Debug + Send,

        R: Send + HasTable + 'static,
        <R as HasTable>::Table: Table + SelectDsl<S>,

        ht::Select<<R as HasTable>::Table, S>:
            IsNotDeleted<'query, Self::AsyncConnection, R, R, IsNotDeletedFilter = F>,

        F: Paginate<Self::Backend, R::Table> + Send,
        for<'q> PaginatedQuery<
            R::Table,
            <F::Output as Paginated<Self::Backend>>::Q0<'q>,
            <F::Output as Paginated<Self::Backend>>::Q2<'q>,
        >: LoadQuery<'query, Self::AsyncConnection, Paged<R>> + Send,
        Self::Backend: QueryMetadata<<F::Output as Query>::SqlType>,
        Paged<R>: FromSqlRow<<F::Output as Query>::SqlType, Self::Backend>,
        i64: ToSql<BigInt, Self::Backend>,

        S: SelectableExpression<R::Table> + Send + ValidGrouping<()> + 'query,

        'life0: 'async_trait,
        'query: 'async_trait,
        R: 'async_trait,
        P: 'async_trait,
        F: 'async_trait,
        S: 'async_trait,
        Self: 'life0,
    {
        if page.borrow().is_empty() {
            return Box::pin(ready(Ok(vec![])));
        }
        let page = page.borrow().clone();
        let query = R::table().select(selection).is_not_deleted().paginate(page);
        self.query(move |conn| Box::pin(query.get_results::<Paged<R>>(conn)))
            .map_ok(|results| results.into_iter().map(|(r, _, _)| r).collect())
            .boxed()
    }

    #[allow(clippy::type_complexity)]
    #[framed]
    fn get_pages<'life0, 'async_trait, 'query, R, P, I, F, S>(
        &'life0 self,
        pages: I,
        selection: S,
    ) -> BoxFuture<'async_trait, Result<HashMap<DbPage<R::Table>, Vec<R>>, Error>>
    where
        P: Debug + for<'a> DbPageRef<'a, R::Table> + Send,
        I: Debug + IntoIterator<Item = P> + Send,
        <I as IntoIterator>::IntoIter: Send,

        R: Clone + Send + HasTable + 'static,
        <R as HasTable>::Table: Table + SelectDsl<S>,

        ht::Select<R::Table, S>: IsNotDeleted<'query, Self::AsyncConnection, R, R, IsNotDeletedFilter = F>,

        F: Paginate<Self::Backend, R::Table> + Send,
        for<'q> PaginatedQuery<
            R::Table,
            <F::Output as Paginated<Self::Backend>>::Q0<'q>,
            <F::Output as Paginated<Self::Backend>>::Q2<'q>,
        >: LoadQuery<'query, Self::AsyncConnection, Paged<R>> + Send,
        Self::Backend: QueryMetadata<<F::Output as Query>::SqlType>,
        Paged<R>: FromSqlRow<<F::Output as Query>::SqlType, Self::Backend>,
        i64: ToSql<BigInt, Self::Backend>,

        S: SelectableExpression<<R as HasTable>::Table> + Send + ValidGrouping<()> + 'query,

        'life0: 'async_trait,
        'query: 'async_trait,
        R: 'async_trait,
        P: 'async_trait,
        I: 'async_trait,
        F: 'async_trait,
        S: 'async_trait,
        Self: 'life0,
    {
        let pages = pages.into_iter().map(|x| x.page_ref().clone()).collect::<Vec<_>>();

        let query = R::table()
            .select(selection)
            .is_not_deleted()
            .multipaginate(pages.iter());
        self.query(move |conn| Box::pin(query.get_results::<Paged<R>>(conn)))
            .map_ok(move |results| split_multipaginated_results(results, pages))
            .boxed()
    }

    #[framed]
    fn insert<'life0, 'async_trait, 'query, 'v, R, V, I, Op, S>(
        &'life0 self,
        values: I,
        selection: S,
    ) -> BoxFuture<'async_trait, Result<Vec<R>, Error>>
    where
        V: HasTable + Send,
        <V as HasTable>::Table: Table + QueryId + Send + 'query,
        <<V as HasTable>::Table as QuerySource>::FromClause: Send,

        Vec<V>: Insertable<<V as HasTable>::Table>,
        <Vec<V> as Insertable<<V as HasTable>::Table>>::Values: Send + 'query,
        R: Send,
        InsertStatement<<V as HasTable>::Table, <Vec<V> as Insertable<<V as HasTable>::Table>>::Values>:
            Into<InsertStatement<<V as HasTable>::Table, <Vec<V> as Insertable<<V as HasTable>::Table>>::Values, Op>>,
        InsertStatement<
            <V as HasTable>::Table,
            <Vec<V> as Insertable<<V as HasTable>::Table>>::Values,
            Op,
            ReturningClause<S>,
        >: LoadQuery<'query, Self::AsyncConnection, R>,

        I: IntoIterator<Item = V> + Send,

        R: MaybeAudit<'query, Self::AsyncConnection>,

        Op: Send + 'query,
        S: SelectableExpression<<V as HasTable>::Table> + Send + ValidGrouping<()> + 'query,
        <S as ValidGrouping<()>>::IsAggregate: diesel::expression::MixedAggregates<
            diesel::expression::is_aggregate::No,
            Output = diesel::expression::is_aggregate::No,
        >,

        'v: 'async_trait + 'life0,
        'life0: 'async_trait,
        Self: 'life0,
        R: 'async_trait,
        V: 'async_trait,
        I: 'async_trait + 'v,
        S: 'async_trait,
    {
        self.raw_tx(move |conn| {
            let values = values.into_iter().collect::<Vec<_>>();
            if values.is_empty() {
                return Box::pin(ready(Ok(vec![])));
            }

            async move {
                let all_inserted = diesel::insert_into(V::table())
                    .values(values)
                    .into()
                    .returning(selection)
                    .get_results::<R>(conn)
                    .await?;

                R::maybe_insert_audit_records(conn, &all_inserted).await?;

                Ok(all_inserted)
            }
            .scope_boxed()
        })
        .boxed()
    }

    #[framed]
    fn update<'life0, 'async_trait, 'query, 'v, R, V, I, T, Pk, F, S>(
        &'life0 self,
        patches: I,
        selection: S,
    ) -> BoxFuture<'async_trait, Result<Vec<R>, Error>>
    where
        V: AsChangeset<Target = T> + HasTable<Table = T> + IncludesChanges + Send + Sync,
        for<'a> &'a V: HasTable<Table = T> + Identifiable<Id = &'a Pk> + IntoUpdateTarget,
        <V as AsChangeset>::Changeset: Send + 'query,
        for<'a> <&'a V as IntoUpdateTarget>::WhereClause: Send,

        ht::Find<T, Pk>: IntoUpdateTarget,
        <ht::Find<T, Pk> as IntoUpdateTarget>::WhereClause: Send + 'query,
        UpdateStatement<
            <ht::Find<T, Pk> as HasTable>::Table,
            <ht::Find<T, Pk> as IntoUpdateTarget>::WhereClause,
            <V as AsChangeset>::Changeset,
        >: AsQuery,
        UpdateStatement<
            <ht::Find<T, Pk> as HasTable>::Table,
            <ht::Find<T, Pk> as IntoUpdateTarget>::WhereClause,
            <V as AsChangeset>::Changeset,
            ReturningClause<S>,
        >: AsQuery + LoadQuery<'query, Self::AsyncConnection, R> + Send,

        Pk: AsExpression<SqlTypeOf<<T as Table>::PrimaryKey>> + Clone + Debug + Hash + Eq + Send + Sync,

        T: FindDsl<Pk> + SelectDsl<S> + Send + Sync + Table + 'query,
        ht::Find<T, Pk>: HasTable<Table = T> + Send,
        <T as QuerySource>::FromClause: Send,

        <<T as Table>::PrimaryKey as Expression>::SqlType: Send,

        I: IntoIterator<Item = V> + Send,
        R: Send + Debug,
        for<'a> &'a R: Identifiable<Id = &'a Pk>,

        R: MaybeAudit<'query, Self::AsyncConnection>,

        <T as Table>::PrimaryKey: Expression + ExpressionMethods + Send + Sync,
        <<T as Table>::PrimaryKey as Expression>::SqlType: SqlType,
        ht::Select<T, S>: FilterDsl<ht::EqAny<<T as Table>::PrimaryKey, Vec<Pk>>, Output = F> + Send,
        F: IsNotDeleted<'query, Self::AsyncConnection, R, R>,

        // Selection bounds
        S: Clone + SelectableExpression<T> + Send + ValidGrouping<()> + Sync + 'query,
        <S as ValidGrouping<()>>::IsAggregate: diesel::expression::MixedAggregates<
            diesel::expression::is_aggregate::No,
            Output = diesel::expression::is_aggregate::No,
        >,

        'life0: 'async_trait,
        'query: 'async_trait,
        'v: 'async_trait + 'life0,
        R: 'async_trait,
        V: 'async_trait,
        I: 'async_trait + 'v,
        T: 'async_trait,
        Pk: 'async_trait,
        F: 'async_trait,
        S: 'async_trait,
    {
        let patches = patches.into_iter().collect::<Vec<V>>();
        let ids = patches.iter().map(|patch| patch.id().clone()).collect::<Vec<_>>();

        let no_change_patch_ids = patches
            .iter()
            .filter_map(
                |patch| {
                    if !patch.includes_changes() {
                        Some(patch.id().to_owned())
                    } else {
                        None
                    }
                },
            )
            .collect::<Vec<_>>();

        self.raw_tx(move |conn| {
            async move {
                if ids.is_empty() {
                    return Ok(vec![]);
                }

                let num_changed_patches = ids.len() - no_change_patch_ids.len();
                let mut all_updated = Vec::with_capacity(num_changed_patches);

                if num_changed_patches > 0 {
                    for patch in patches.into_iter().filter(|patch| patch.includes_changes()) {
                        let record = diesel::update(V::table().find(patch.id().to_owned()))
                            .set(patch)
                            .returning(selection.clone())
                            .get_result::<R>(conn)
                            .await?;
                        all_updated.push(record);
                    }
                    R::maybe_insert_audit_records(conn, &all_updated).await?;
                }

                let unchanged_records = V::table()
                    .select(selection)
                    .filter(V::table().primary_key().eq_any(no_change_patch_ids))
                    .is_not_deleted()
                    .get_results::<R>(&mut *conn)
                    .await?;

                let mut all_records = unchanged_records
                    .into_iter()
                    .chain(all_updated.into_iter())
                    .map(|record| (record.id().clone(), record))
                    .collect::<HashMap<_, _>>();

                Ok(ids.iter().filter_map(|id| all_records.remove(id)).collect::<Vec<_>>())
            }
            .scope_boxed()
        })
        .boxed()
    }
}

#[async_trait]
impl<'d, D: _Db + Clone> _Db for Cow<'d, D> {
    type Backend = D::Backend;
    type AsyncConnection = D::AsyncConnection;
    type Connection<'r> = D::Connection<'r> where Self: 'r;
    type TxConnection<'r> = D::TxConnection<'r>;

    async fn query<'a, F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>> + Send + 'a,
        E: Debug + From<Error> + Send + 'a,
        T: Send + 'a,
    {
        (**self).query(f).await
    }

    async fn with_tx_connection<'a, F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>> + Send + 'a,
        E: Debug + From<Error> + Send + 'a,
        T: Send + 'a,
    {
        (**self).with_tx_connection(f).await
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

    async fn tx<'life0, 'a, T, E, F>(&'life0 self, callback: F) -> Result<T, E>
    where
        F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
        'life0: 'a,
    {
        (**self).tx(callback).await
    }
}

#[async_trait]
impl<'d, D: _Db + Clone> _Db for &'d D {
    type Backend = D::Backend;
    type AsyncConnection = D::AsyncConnection;
    type Connection<'r> = D::Connection<'r> where Self: 'r;
    type TxConnection<'r> = D::TxConnection<'r>;

    async fn query<'a, F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>> + Send + 'a,
        E: Debug + From<Error> + Send + 'a,
        T: Send + 'a,
    {
        (**self).query(f).await
    }

    async fn with_tx_connection<'a, F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>> + Send + 'a,
        E: Debug + From<Error> + Send + 'a,
        T: Send + 'a,
    {
        (**self).with_tx_connection(f).await
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

    async fn tx<'life0, 'a, T, E, F>(&'life0 self, callback: F) -> Result<T, E>
    where
        F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
        E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
        T: Send + 'a,
        'life0: 'a,
    {
        (**self).tx(callback).await
    }
}

cfg_if! {
    if #[cfg(any(feature = "bb8", feature = "deadpool", feature = "mobc"))] {
        #[async_trait]
        impl<'d, C> _Db for DbConnOwned<'d, C>
        where
            C: AsyncConnection + PoolableConnection + Sync + 'static,
            <C as AsyncConnection>::Backend: Send,
        {
            type Backend = <C as AsyncConnection>::Backend;
            type AsyncConnection = C;
            type Connection<'r> = PooledConnection<'r, C> where Self: 'r;
            type TxConnection<'r> = Cow<'r, DbConnRef<'r, Self::AsyncConnection>>;

            async fn query<'a, F, T, E>(&self, f: F) -> Result<T, E>
            where
                F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
                    + Send
                    + 'a,
                E: Debug + From<Error> + Send + 'a,
                T: Send + 'a
            {
                let mut connection = self.connection.write().await;
                f(connection.deref_mut().deref_mut()).await
            }

            async fn with_tx_connection<'a, F, T, E>(&self, f: F) -> Result<T, E>
            where
                F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
                    + Send
                    + 'a,
                E: Debug + From<Error> + Send + 'a,
                T: Send + 'a
            {
                let mut connection = self.connection.write().await;
                let connection = connection.deref_mut().deref_mut();
                connection.transaction(f).await
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
            async fn tx<'life0, 'a, T, E, F>(&'life0 self, callback: F) -> Result<T, E>
            where
                F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
                E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
                T: Send + 'a,
                'life0: 'a,
            {
                let tx_cleanup = <Self as AsRef<TxCleanup<Self::AsyncConnection>>>::as_ref(self).clone();
                self.with_tx_connection(move |mut conn| async move {
                    let db_connection = DbConnection {
                        tx_id: Some(Uuid::new_v4()),
                        connection: Arc::new(RwLock::new(conn.deref_mut())),
                        tx_cleanup: tx_cleanup.clone(),
                    };
                    let value = callback.call_tx_fn(Cow::Borrowed(&db_connection)).await?;
                    let mut tx_cleanup = tx_cleanup.lock().await;
                    for tx_cleanup_fn in tx_cleanup.drain(..) {
                        tx_cleanup_fn(&db_connection).await?;
                    }
                    Ok::<T, E>(value)
                }.scope_boxed()).await
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
                #[allow(unused_mut)]
                self.with_tx_connection(move |mut conn| async move {
                    let value = callback(conn).await?;

                    #[cfg(feature = "bb8")]
                    let connection = Arc::new(RwLock::new(conn.deref_mut()));

                    #[cfg(feature = "deadpool")]
                    let connection = Arc::new(RwLock::new(conn));

                    #[cfg(feature = "mobc")]
                    let connection = Arc::new(RwLock::new(conn.deref_mut()));

                    let db_connection = DbConnection {
                        tx_id: Some(Uuid::new_v4()),
                        tx_cleanup: tx_cleanup.clone(),
                        connection,
                    };
                    let mut tx_cleanup = tx_cleanup.lock().await;
                    for tx_cleanup_fn in tx_cleanup.drain(..) {
                        tx_cleanup_fn(&db_connection).await?;
                    }
                    Ok(value)
                }.scope_boxed()).await
            }
        }

        #[async_trait]
        impl<'d, C> _Db for DbConnection<C, &'d mut C>
        where
            C: AsyncConnection + PoolableConnection + Sync + 'static,
            <C as AsyncConnection>::Backend: Send,
        {
            type Backend = <C as AsyncConnection>::Backend;
            type AsyncConnection = C;
            type Connection<'r> = &'d mut C where Self: 'r;
            type TxConnection<'r> = Cow<'r, DbConnRef<'r, Self::AsyncConnection>>;

            async fn query<'a, F, T, E>(&self, f: F) -> Result<T, E>
            where
                F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
                    + Send
                    + 'a,
                E: Debug + From<Error> + Send + 'a,
                T: Send + 'a
            {
                let mut connection = self.connection.write().await;
                f(connection.deref_mut().deref_mut()).await
            }

            async fn with_tx_connection<'a, F, T, E>(&self, f: F) -> Result<T, E>
            where
                F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
                    + Send
                    + 'a,
                E: Debug + From<Error> + Send + 'a,
                T: Send + 'a
            {
                let mut connection = self.connection.write().await;
                let connection = connection.deref_mut().deref_mut();
                connection.transaction(f).await
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

            async fn tx<'life0, 'a, T, E, F>(&'life0 self, callback: F) -> Result<T, E>
            where
                F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
                E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
                T: Send + 'a,
                'life0: 'a,
            {
                let tx_id = self.tx_id;
                let tx_cleanup = <Self as AsRef<TxCleanup<Self::AsyncConnection>>>::as_ref(self).clone();
                self.with_tx_connection(move |conn| {
                    let db_connection = DbConnection {
                        connection: Arc::new(RwLock::new(conn)),
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
                self.with_tx_connection(move |conn| callback(conn).scope_boxed()).await
            }
        }

        #[async_trait]
        impl<C> _Db for crate::Pool<C>
        where
            C: AsyncPoolableConnection + Sync + 'static,
            <C as AsyncConnection>::Backend: Send,
        {
            type Backend = <C as AsyncConnection>::Backend;
            type AsyncConnection = C;
            type Connection<'r> = PooledConnection<'r, C>;
            type TxConnection<'r> = Cow<'r, DbConnRef<'r, Self::AsyncConnection>>;

            async fn query<'a, F, T, E>(&self, f: F) -> Result<T, E>
            where
                F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
                    + Send
                    + 'a,
                E: Debug + From<Error> + Send + 'a,
                T: Send + 'a
            {
                let connection = self.get_connection().await?;
                let mut connection = connection.write().await;
                f(connection.deref_mut().deref_mut()).await
            }

            async fn with_tx_connection<'a, F, T, E>(&self, f: F) -> Result<T, E>
            where
                F: for<'r> FnOnce(&'r mut Self::AsyncConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
                    + Send
                    + 'a,
                E: Debug + From<Error> + Send + 'a,
                T: Send + 'a
            {
                let connection = self.get_connection().await?;
                let mut connection = connection.write().await;
                let connection = connection.deref_mut().deref_mut();
                connection.transaction(f).await
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
            async fn tx<'life0, 'a, T, E, F>(&'life0 self, callback: F) -> Result<T, E>
            where
                F: for<'r> TxFn<'a, Self::TxConnection<'r>, ScopedBoxFuture<'a, 'r, Result<T, E>>>,
                E: Debug + From<Error> + From<TxCleanupError> + Send + 'a,
                T: Send + 'a,
                'life0: 'a,
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
                #[allow(unused_mut)]
                self.with_tx_connection(move |mut conn| async move {
                    let value = callback(conn).await?;

                    let tx_cleanup = TxCleanup::default();

                    #[cfg(feature = "bb8")]
                    let connection = Arc::new(RwLock::new(conn.deref_mut()));

                    #[cfg(feature = "deadpool")]
                    let connection = Arc::new(RwLock::new(conn));

                    #[cfg(feature = "mobc")]
                    let connection = Arc::new(RwLock::new(conn.deref_mut()));

                    let db_connection = DbConnection {
                        tx_id: Some(Uuid::new_v4()),
                        tx_cleanup: tx_cleanup.clone(),
                        connection,
                    };
                    let mut tx_cleanup = tx_cleanup.lock().await;
                    for tx_cleanup_fn in tx_cleanup.drain(..) {
                        tx_cleanup_fn(&db_connection).await?;
                    }
                    Ok(value)
                }.scope_boxed()).await
            }
        }
    }
}
