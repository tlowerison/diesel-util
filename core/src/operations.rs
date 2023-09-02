use crate::*;
use diesel::associations::HasTable;
use diesel::deserialize::FromSqlRow;
use diesel::dsl::SqlTypeOf;
use diesel::expression::{AsExpression, Expression, QueryMetadata, ValidGrouping};
use diesel::expression_methods::ExpressionMethods;
use diesel::helper_types as ht;
use diesel::query_dsl::methods::{FilterDsl, FindDsl, SelectDsl};
use diesel::query_source::QuerySource;
use diesel::result::Error;
use diesel::serialize::ToSql;
use diesel::sql_types::{BigInt, SqlType};
use diesel::SelectableExpression;
use diesel::{query_builder::*, Identifiable};
use diesel::{Insertable, Table};
use diesel_async::methods::*;
use either::Either;
use futures::future::FutureExt;
use scoped_futures::ScopedFutureExt;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

/// wrapper error type returned from DbEntity methods
/// solely exists to facilitate error conversion when
/// converting between the raw db representation and
/// the entity representation
#[derive(Debug, Display, Error, IsVariant, PartialEq, Unwrap)]
pub enum DbEntityError<E> {
    Db(Error),
    /// In the cases where [`DbEntity::Raw`] does not implement [`Into<Self>`]
    /// and instead only implements [`TryInto<Self>`], this variant captures
    /// any errors that occurred during conversion. Conversion types are
    /// useful when you have a more useful structure to keep your db entities
    /// in which doesn't map exactly to a diesel table instance, e.g. nesting
    /// structs or having types which are coupled i.e. if one is an Some
    /// then the other must be Some. In the cases where the assumptions made
    /// by the conversion type during conversion are not held up at runtime,
    /// i.e. data has been modified in the database in a way which breaks
    /// these assumptions, then implementing `TryInto` rather than `Into`
    /// for converting between the types is obviously a better result,
    /// allowing you to avoid a panic.
    Conversion(E),
}

impl<E> From<Error> for DbEntityError<E> {
    fn from(value: Error) -> Self {
        Self::Db(value)
    }
}

impl<E> DbEntityError<E> {
    pub fn conversion(value: E) -> Self {
        Self::Conversion(value)
    }
}

impl From<DbEntityError<std::convert::Infallible>> for diesel::result::Error {
    fn from(value: DbEntityError<std::convert::Infallible>) -> Self {
        value.unwrap_db()
    }
}

pub trait DbEntity: Debug + Sized + Send + 'static {
    /// an equivalent representation of this entity which has diesel trait implementations
    type Raw: Debug + HasTable<Table = Self::Table> + TryInto<Self> + Send + 'static;

    /// the table this entity represents
    type Table: Table + QueryId + SelectDsl<Self::Selection> + Send + Sync;

    /// the type of this entity's table's primary key
    /// note that this type should be equivalent diesel sql_type representation of
    /// the type of the primary key field in Self::Raw
    type Id: AsExpression<SqlTypeOf<<Self::Table as Table>::PrimaryKey>> + Clone + Debug + Eq + Hash + Send + Sync
    where
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType;

    /// represents the selection of columns returned from all queries
    type Selection: Clone + SelectableExpression<Self::Table> + Send + ValidGrouping<()> + Sync;
    /// a form of [`Self::Selection`] which is guaranteed to implement Default
    /// exists because Default is only implemented for tuple types of 12 items or less
    type SelectionHelper: Default + SelectionInto<Self::Selection> + Send;

    fn selection() -> Self::Selection {
        Self::SelectionHelper::default().selection_into()
    }
}

#[async_trait]
pub trait DbGet: DbEntity {
    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn get<'query, D, F>(
        db: &D,
        ids: impl IntoIterator<Item = Self::Id> + Debug + Send,
    ) -> Result<Vec<Self>, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug,

        // Id bounds
        for<'a> &'a Self::Raw: Identifiable<Id = &'a Self::Id>,
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

        ht::Select<Self::Table, Self::Selection>:
            FilterDsl<ht::EqAny<<Self::Table as Table>::PrimaryKey, Vec<Self::Id>>, Output = F>,

        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

        F: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>,
        <F as IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>>::IsNotDeletedFilter:
            LoadQuery<'query, D::AsyncConnection, Self::Raw> + Send,
    {
        let ids = ids.into_iter().collect::<Vec<_>>();

        if ids.is_empty() {
            return Ok(vec![]);
        }

        let result: Result<Vec<Self::Raw>, _> = db.get(ids, Self::selection()).await;
        match result {
            Ok(records) => Ok(records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(DbEntityError::conversion)?),
            Err(err) => {
                #[cfg(feature = "tracing")]
                error!(target: module_path!(), error = %err);
                Err(err.into())
            }
        }
    }

    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn get_one<'query, D, F>(
        db: &D,
        id: Self::Id,
    ) -> Result<Self, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug,

        // Id bounds
        for<'a> &'a Self::Raw: Identifiable<Id = &'a Self::Id>,
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

        ht::Select<Self::Table, Self::Selection>:
            FilterDsl<ht::EqAny<<Self::Table as Table>::PrimaryKey, [Self::Id; 1]>, Output = F>,

        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

        F: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>,
        <F as IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>>::IsNotDeletedFilter:
            LoadQuery<'query, D::AsyncConnection, Self::Raw> + Send,
    {
        let result: Result<Vec<Self::Raw>, _> = db.get([id], Self::selection()).await;
        match result {
            Ok(records) => Ok(records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbEntityError::conversion)?
                .pop()
                .ok_or(Error::NotFound)?),
            Err(err) => {
                #[cfg(feature = "tracing")]
                error!(target: module_path!(), error = %err);
                Err(err.into())
            }
        }
    }

    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn get_by_column<'query, D, C, U, Q>(
        db: &D,
        column: C,
        values: impl Debug + IntoIterator<Item = U> + Send,
    ) -> Result<Vec<Self>, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug,

        // Id bounds
        U: AsExpression<SqlTypeOf<C>> + Debug + Send,
        C: Debug + Expression + ExpressionMethods + Send,
        SqlTypeOf<C>: SqlType,

        ht::Select<Self::Table, Self::Selection>: FilterDsl<ht::EqAny<C, Vec<U>>, Output = Q>,

        Q: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>,
        <Q as IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>>::IsNotDeletedFilter:
            LoadQuery<'query, D::AsyncConnection, Self::Raw> + Send,
    {
        let values = values.into_iter().collect::<Vec<_>>();

        if values.is_empty() {
            return Ok(vec![]);
        }

        let result: Result<Vec<Self::Raw>, _> = db.get_by_column(column, values, Self::selection()).await;
        match result {
            Ok(records) => Ok(records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(DbEntityError::conversion)?),
            Err(err) => {
                #[cfg(feature = "tracing")]
                error!(target: module_path!(), error = %err);
                Err(err.into())
            }
        }
    }

    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn get_page<'query, D, P, F>(
        db: &D,
        page: P,
    ) -> Result<Vec<Self>, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug,

        // DbPage bounds
        P: Borrow<DbPage<Self::Table>> + Debug + Send,

        // Query bounds
        ht::Select<Self::Table, Self::Selection>:
            IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw, IsNotDeletedFilter = F>,

        F: Paginate<D::Backend, Self::Table> + Send,
        for<'q> PaginatedQuery<
            Self::Table,
            <F::Output as Paginated<D::Backend>>::Q0<'q>,
            <F::Output as Paginated<D::Backend>>::Q2<'q>,
        >: LoadQuery<'query, D::AsyncConnection, Paged<Self::Raw>> + Send,
        D::Backend: QueryMetadata<<F::Output as Query>::SqlType>,
        Paged<Self::Raw>: FromSqlRow<<F::Output as Query>::SqlType, D::Backend>,
        i64: ToSql<BigInt, D::Backend>,

        Self::Selection: 'query,
    {
        if page.borrow().is_empty() {
            return Ok(vec![]);
        }

        let result: Result<Vec<Self::Raw>, _> = db.get_page(page, Self::selection()).await;
        match result {
            Ok(records) => Ok(records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(DbEntityError::conversion)?),
            Err(err) => {
                #[cfg(feature = "tracing")]
                error!(target: module_path!(), error = %err);
                Err(err.into())
            }
        }
    }

    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn get_pages<'query, D, P, F>(
        db: &D,
        pages: impl Debug + IntoIterator<Item = P> + Send,
    ) -> Result<HashMap<DbPage<Self::Table>, Vec<Self>>, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug,

        // DbPage bounds
        P: Borrow<DbPage<Self::Table>> + Debug + for<'a> DbPageRef<'a, Self::Table> + Send,

        // Query bounds
        ht::Select<Self::Table, Self::Selection>:
            IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw, IsNotDeletedFilter = F>,
        <F as AsQuery>::Query: Clone + QueryFragment<D::Backend> + QueryId + Send + 'query,

        Self::Raw: Clone,
        F: Paginate<D::Backend, Self::Table> + Send,
        for<'q> PaginatedQuery<
            Self::Table,
            <F::Output as Paginated<D::Backend>>::Q0<'q>,
            <F::Output as Paginated<D::Backend>>::Q2<'q>,
        >: LoadQuery<'query, D::AsyncConnection, Paged<Self::Raw>> + Send,
        D::Backend: QueryMetadata<<F::Output as Query>::SqlType>,
        Paged<Self::Raw>: FromSqlRow<<F::Output as Query>::SqlType, D::Backend>,
        i64: ToSql<BigInt, D::Backend>,

        Self::Selection: 'query,
    {
        let pages = pages.into_iter().collect::<Vec<_>>();

        if pages.iter().all(|page| page.borrow().is_empty()) {
            return Ok(Default::default());
        }

        let raw_records = match db.get_pages(pages, Self::selection()).await {
            Ok(records) => records,
            Err(err) => {
                #[cfg(feature = "tracing")]
                error!(target: module_path!(), error = %err);
                return Err(err.into());
            }
        };
        let mut records = HashMap::<DbPage<Self::Table>, Vec<Self>>::with_capacity(raw_records.len());
        for (page, raw_records) in raw_records {
            records.insert(
                page,
                raw_records
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DbEntityError::conversion)?,
            );
        }
        Ok(records)
    }
}

#[async_trait]
pub trait DbInsert: DbEntity {
    type PostHelper<'v>: Debug + Into<Self::Post<'v>> + Send = Self::Post<'v>;
    type Post<'v>: Debug + HasTable<Table = Self::Table> + Send;

    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn insert<'query, 'v, D, Op>(
        db: &D,
        posts: impl Debug + IntoIterator<Item = Self::PostHelper<'v>> + Send + 'v,
    ) -> Result<Vec<Self>, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db + 'query,
        'v: 'query,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug + Send,

        <Self::Table as QuerySource>::FromClause: Send,

        // Insertable bounds
        Vec<Self::Post<'v>>: Insertable<Self::Table>,
        <Vec<Self::Post<'v>> as Insertable<Self::Table>>::Values: Send + 'query,
        Self::Raw: Send,

        // Insert bounds
        InsertStatement<Self::Table, <Vec<Self::Post<'v>> as Insertable<Self::Table>>::Values>:
            Into<InsertStatement<Self::Table, <Vec<Self::Post<'v>> as Insertable<Self::Table>>::Values, Op>>,
        InsertStatement<
            Self::Table,
            <Vec<Self::Post<'v>> as Insertable<Self::Table>>::Values,
            Op,
            ReturningClause<Self::Selection>,
        >: LoadQuery<'query, D::AsyncConnection, Self::Raw>,

        // Audit bounds
        Self::Raw: MaybeAudit<'query, D::AsyncConnection>,

        // Selection bounds
        Op: Send + 'query,
        Self::Selection: SelectableExpression<Self::Table> + Send + ValidGrouping<()>,
        <Self::Selection as ValidGrouping<()>>::IsAggregate: diesel::expression::MixedAggregates<
            diesel::expression::is_aggregate::No,
            Output = diesel::expression::is_aggregate::No,
        >,
    {
        let db_post_helpers = posts.into_iter().collect::<Vec<_>>();

        if db_post_helpers.is_empty() {
            return Ok(vec![]);
        }

        let db_posts = db_post_helpers.into_iter().map(Self::PostHelper::into);

        db.insert(db_posts, Self::selection())
            .map(|result| match result {
                Ok(records) => Ok(records),
                Err(err) => {
                    let err = err;
                    #[cfg(feature = "tracing")]
                    error!(target: module_path!(), error = %err);
                    Err(err)
                }
            })
            .await
            .map_err(DbEntityError::from)
            .and_then(|records| {
                records
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()
                    .map_err(DbEntityError::conversion)
            })
    }

    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn insert_one<'query, 'v, D, Op>(
        db: &D,
        post: Self::PostHelper<'v>,
    ) -> Result<Self, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db + 'query,
        'v: 'query,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug + Send,

        <Self::Table as QuerySource>::FromClause: Send,

        // Insertable bounds
        Vec<Self::Post<'v>>: Insertable<Self::Table>,
        <Vec<Self::Post<'v>> as Insertable<Self::Table>>::Values: Send + 'query,
        Self::Raw: Send,

        // Insert bounds
        InsertStatement<Self::Table, <Vec<Self::Post<'v>> as Insertable<Self::Table>>::Values>:
            Into<InsertStatement<Self::Table, <Vec<Self::Post<'v>> as Insertable<Self::Table>>::Values, Op>>,
        InsertStatement<
            Self::Table,
            <Vec<Self::Post<'v>> as Insertable<Self::Table>>::Values,
            Op,
            ReturningClause<Self::Selection>,
        >: LoadQuery<'query, D::AsyncConnection, Self::Raw> + QueryFragment<D::Backend>,

        // Audit bounds
        Self::Raw: MaybeAudit<'query, D::AsyncConnection>,

        // Selection bounds
        Op: Send + 'query,
        Self::Selection: SelectableExpression<Self::Table> + Send + ValidGrouping<()>,
        ReturningClause<Self::Selection>: QueryFragment<D::Backend>,
        <Self::Selection as ValidGrouping<()>>::IsAggregate: diesel::expression::MixedAggregates<
            diesel::expression::is_aggregate::No,
            Output = diesel::expression::is_aggregate::No,
        >,
    {
        db.insert([post.into()], Self::selection())
            .map(|result| match result {
                Ok(record) => Ok(record),
                Err(err) => {
                    let err = err;
                    #[cfg(feature = "tracing")]
                    error!(target: module_path!(), error = %err);
                    Err(err)
                }
            })
            .await
            .map_err(DbEntityError::from)
            .and_then(|mut records| {
                records
                    .pop()
                    .ok_or_else(|| diesel::result::Error::NotFound)?
                    .try_into()
                    .map_err(DbEntityError::conversion)
            })
    }
}

#[async_trait]
pub trait DbUpdate: DbEntity {
    /// conversion helper type if needed, defaults to [`Self::Patch`]
    type PatchHelper<'v>: Debug + Into<Self::Patch<'v>> + Send = Self::Patch<'v>;
    type Patch<'v>: AsChangeset<Target = Self::Table>
        + Debug
        + HasTable<Table = Self::Table>
        + IncludesChanges
        + Send
        + Sync;

    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn update<'query, 'v, D, F>(
        db: &D,
        patches: impl Debug + IntoIterator<Item = Self::PatchHelper<'v>> + Send + 'v,
    ) -> Result<Vec<Self>, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db + 'query,
        'v: 'query,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug,

        // Id bounds
        for<'a> &'a Self::Raw: Identifiable<Id = &'a Self::Id>,
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods + Send + Sync,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

        // Changeset bounds
        <Self::Patch<'v> as AsChangeset>::Changeset: Send,
        for<'a> &'a Self::Patch<'v>: HasTable<Table = Self::Table> + Identifiable<Id = &'a Self::Id> + IntoUpdateTarget,
        for<'a> <&'a Self::Patch<'v> as IntoUpdateTarget>::WhereClause: Send,
        <Self::Table as QuerySource>::FromClause: Send,

        // UpdateStatement bounds
        Self::Table: FindDsl<Self::Id>,
        ht::Find<Self::Table, Self::Id>: IntoUpdateTarget,
        <ht::Find<Self::Table, Self::Id> as IntoUpdateTarget>::WhereClause: Send + 'query,
        UpdateStatement<
            <ht::Find<Self::Table, Self::Id> as HasTable>::Table,
            <ht::Find<Self::Table, Self::Id> as IntoUpdateTarget>::WhereClause,
            <Self::Patch<'v> as AsChangeset>::Changeset,
        >: AsQuery,
        UpdateStatement<
            <ht::Find<Self::Table, Self::Id> as HasTable>::Table,
            <ht::Find<Self::Table, Self::Id> as IntoUpdateTarget>::WhereClause,
            <Self::Patch<'v> as AsChangeset>::Changeset,
            ReturningClause<Self::Selection>,
        >: AsQuery + LoadQuery<'query, D::AsyncConnection, Self::Raw> + Send,

        Self::Table: FindDsl<Self::Id> + SelectDsl<Self::Selection> + Send + Sync + Table + 'query,
        ht::Find<Self::Table, Self::Id>: HasTable<Table = Self::Table> + Send,
        <Self::Table as QuerySource>::FromClause: Send,

        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: Send,

        Self::Raw: MaybeAudit<'query, D::AsyncConnection> + Send,

        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods + Send + Sync,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,
        ht::Select<Self::Table, Self::Selection>:
            FilterDsl<ht::EqAny<<Self::Table as Table>::PrimaryKey, Vec<Self::Id>>, Output = F> + Send,
        F: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>,

        // Selection bounds
        Self::Selection: Clone + SelectableExpression<Self::Table> + Send + ValidGrouping<()> + Sync + 'query,
        <Self::Selection as ValidGrouping<()>>::IsAggregate: diesel::expression::MixedAggregates<
            diesel::expression::is_aggregate::No,
            Output = diesel::expression::is_aggregate::No,
        >,
    {
        let db_patch_helpers = patches.into_iter().collect::<Vec<_>>();

        if db_patch_helpers.is_empty() {
            return Ok(vec![]);
        }

        db.update(
            db_patch_helpers.into_iter().map(Self::PatchHelper::into),
            Self::selection(),
        )
        .map(|result| match result {
            Ok(records) => Ok(records),
            Err(err) => {
                let err = err;
                #[cfg(feature = "tracing")]
                error!(target: module_path!(), error = %err);
                Err(err)
            }
        })
        .await
        .map_err(DbEntityError::from)
        .and_then(|records| {
            records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(DbEntityError::conversion)
        })
    }

    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn update_one<'query, 'v, D, F>(
        db: &D,
        patch: Self::PatchHelper<'v>,
    ) -> Result<Self, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db + 'query,
        'v: 'query,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug,

        // Id bounds
        for<'a> &'a Self::Raw: Identifiable<Id = &'a Self::Id>,
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods + Send + Sync,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

        // Changeset bounds
        <Self::Patch<'v> as AsChangeset>::Changeset: Send,
        for<'a> &'a Self::Patch<'v>: HasTable<Table = Self::Table> + Identifiable<Id = &'a Self::Id> + IntoUpdateTarget,
        for<'a> <&'a Self::Patch<'v> as IntoUpdateTarget>::WhereClause: Send,
        <Self::Table as QuerySource>::FromClause: Send,

        // UpdateStatement bounds
        Self::Table: FindDsl<Self::Id>,
        ht::Find<Self::Table, Self::Id>: IntoUpdateTarget,
        <ht::Find<Self::Table, Self::Id> as IntoUpdateTarget>::WhereClause: Send + 'query,
        UpdateStatement<
            <ht::Find<Self::Table, Self::Id> as HasTable>::Table,
            <ht::Find<Self::Table, Self::Id> as IntoUpdateTarget>::WhereClause,
            <Self::Patch<'v> as AsChangeset>::Changeset,
        >: AsQuery,
        UpdateStatement<
            <ht::Find<Self::Table, Self::Id> as HasTable>::Table,
            <ht::Find<Self::Table, Self::Id> as IntoUpdateTarget>::WhereClause,
            <Self::Patch<'v> as AsChangeset>::Changeset,
            ReturningClause<Self::Selection>,
        >: AsQuery + LoadQuery<'query, D::AsyncConnection, Self::Raw> + Send,

        Self::Table: FindDsl<Self::Id> + SelectDsl<Self::Selection> + Send + Sync + Table + 'query,
        ht::Find<Self::Table, Self::Id>: HasTable<Table = Self::Table> + Send,
        <Self::Table as QuerySource>::FromClause: Send,

        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: Send,

        Self::Raw: MaybeAudit<'query, D::AsyncConnection> + Send,

        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods + Send + Sync,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,
        ht::Select<Self::Table, Self::Selection>:
            FilterDsl<ht::EqAny<<Self::Table as Table>::PrimaryKey, Vec<Self::Id>>, Output = F> + Send,
        F: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>,

        // Selection bounds
        Self::Selection: Clone + SelectableExpression<Self::Table> + Send + ValidGrouping<()> + Sync + 'query,
        <Self::Selection as ValidGrouping<()>>::IsAggregate: diesel::expression::MixedAggregates<
            diesel::expression::is_aggregate::No,
            Output = diesel::expression::is_aggregate::No,
        >,
    {
        db.update([patch.into()], Self::selection())
            .map(|result| match result {
                Ok(mut records) => Ok(records.pop().ok_or(diesel::result::Error::NotFound)?),
                Err(err) => {
                    let err = err;
                    #[cfg(feature = "tracing")]
                    error!(target: module_path!(), error = %err);
                    Err(err)
                }
            })
            .await
            .map_err(DbEntityError::from)
            .and_then(|record| record.try_into().map_err(DbEntityError::conversion))
    }
}

#[async_trait]
pub trait DbDelete: DbEntity {
    type DeletedAt;
    type DeletePatch<'v>;

    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn delete<'query, 'v, D, I>(
        db: &D,
        ids: I,
    ) -> Result<Vec<Self>, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db + 'query,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug,

        I: Debug + Send,

        // Id bounds
        for<'a> &'a Self::Raw: Identifiable<Id = &'a Self::Id>,
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

        Self::Raw: Deletable<
            'query,
            D::AsyncConnection,
            Self::Table,
            I,
            Self::Id,
            Self::DeletedAt,
            Self::DeletePatch<'v>,
            Self::Selection,
        >,
    {
        db.raw_tx(move |conn| {
            async move {
                match Self::Raw::maybe_soft_delete(conn, ids, Self::selection()).await {
                    Either::Left(ids) => Self::Raw::hard_delete(conn, ids, Self::selection()).await,
                    Either::Right(result) => result,
                }
            }
            .scope_boxed()
        })
        .map(|result| match result {
            Ok(records) => Ok(records),
            Err(err) => {
                let err = err;
                #[cfg(feature = "tracing")]
                error!(target: module_path!(), error = %err);
                Err(err)
            }
        })
        .await
        .map_err(DbEntityError::from)
        .and_then(|records| {
            records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(DbEntityError::conversion)
        })
    }

    #[framed]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", not(feature = "tracing-ret")),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db))
    )]
    #[cfg_attr(
        all(feature = "tracing", not(feature = "tracing-args"), feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip_all, ret)
    )]
    #[cfg_attr(
        all(feature = "tracing", feature = "tracing-args", feature = "tracing-ret"),
        instrument(err(Debug), fields(Self = std::any::type_name::<Self>()), skip(db), ret)
    )]
    async fn delete_one<'query, 'v, D, I>(
        db: &D,
        id: Self::Id,
    ) -> Result<Self, DbEntityError<<Self::Raw as TryInto<Self>>::Error>>
    where
        D: _Db + 'query,
        Self::Raw: TryInto<Self>,
        <Self::Raw as TryInto<Self>>::Error: Debug,

        // Id bounds
        for<'a> &'a Self::Raw: Identifiable<Id = &'a Self::Id>,
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

        Self::Raw: Deletable<
            'query,
            D::AsyncConnection,
            Self::Table,
            [Self::Id; 1],
            Self::Id,
            Self::DeletedAt,
            Self::DeletePatch<'v>,
            Self::Selection,
        >,
    {
        db.raw_tx(move |conn| {
            async move {
                match Self::Raw::maybe_soft_delete(conn, [id], Self::selection()).await {
                    Either::Left(ids) => Self::Raw::hard_delete(conn, ids, Self::selection()).await,
                    Either::Right(result) => result,
                }
            }
            .scope_boxed()
        })
        .map(|result| match result {
            Ok(records) => Ok(records),
            Err(err) => {
                let err = err;
                #[cfg(feature = "tracing")]
                error!(target: module_path!(), error = %err);
                Err(err)
            }
        })
        .await
        .map_err(DbEntityError::from)
        .and_then(|mut records| {
            TryInto::try_into(records.pop().expect("expected a deleted record")).map_err(DbEntityError::conversion)
        })
    }
}

impl<T: DbEntity> DbGet for T {}

/// utility trait for converting from tuples of tuples into flattened tuples
pub trait SelectionInto<T> {
    fn selection_into(self) -> T;
}

macro_rules! impl_selection_into {
    ($( ($(($($ident:ident,)*),)*) )*) => {
        $(
            impl<$($($ident,)*)*> SelectionInto<($($($ident,)*)*)> for ($(($($ident,)*),)*) {
                fn selection_into(self) -> ($($($ident,)*)*) {
                    #[allow(non_snake_case)]
                    let ($(($($ident,)*),)*) = self;
                    ($($($ident,)*)*)
                }
            }
        )*
    };
}

// implement in multiples of 12 (largest number of items in a tuple which implement Debug, Default, etc.)
// goes up to 64
impl_selection_into!(((A1,),)((A1, B1,),)((A1, B1, C1,),)((A1, B1, C1, D1,),)((
    A1, B1, C1, D1, E1,
),)((A1, B1, C1, D1, E1, F1,),)((A1, B1, C1, D1, E1, F1, G1,),)((
    A1, B1, C1, D1, E1, F1, G1, H1,
),)((A1, B1, C1, D1, E1, F1, G1, H1, I1,),)((
    A1, B1, C1, D1, E1, F1, G1, H1, I1, J1,
),)((A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1,),)((
    A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,
),)((A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,), (A2,),)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2,),
)((A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,), (A2, B2, C2,),)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5, G5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5, G5, H5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5, G5, H5, I5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5, G5, H5, I5, J5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5, G5, H5, I5, J5, K5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5, G5, H5, I5, J5, K5, L5,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5, G5, H5, I5, J5, K5, L5,),
    (A6,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5, G5, H5, I5, J5, K5, L5,),
    (A6, B6,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5, G5, H5, I5, J5, K5, L5,),
    (A6, B6, C6,),
)(
    (A1, B1, C1, D1, E1, F1, G1, H1, I1, J1, K1, L1,),
    (A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, L2,),
    (A3, B3, C3, D3, E3, F3, G3, H3, I3, J3, K3, L3,),
    (A4, B4, C4, D4, E4, F4, G4, H4, I4, J4, K4, L4,),
    (A5, B5, C5, D5, E5, F5, G5, H5, I5, J5, K5, L5,),
    (A6, B6, C6, D6,),
));
