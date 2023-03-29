use crate::*;
use ::diesel::associations::HasTable;
use ::diesel::backend::Backend;
use ::diesel::dsl::SqlTypeOf;
use ::diesel::expression::{AsExpression, Expression, ValidGrouping};
use ::diesel::expression_methods::ExpressionMethods;
use ::diesel::helper_types as ht;
use ::diesel::query_builder::ReturningClause;
use ::diesel::query_builder::*;
use ::diesel::query_dsl::methods::FilterDsl;
use ::diesel::query_dsl::methods::FindDsl;
use ::diesel::query_dsl::select_dsl::SelectDsl;
use ::diesel::query_source::QuerySource;
use ::diesel::result::Error;
use ::diesel::sql_types::{Nullable, SqlType, Timestamp};
use ::diesel::Identifiable;
use ::diesel::SelectableExpression;
use ::diesel::{Column, Table};
use ::diesel_async::methods::*;
use ::diesel_async::{AsyncConnection, RunQueryDsl};
use ::either::Either;
use ::futures::future::{ready, BoxFuture, FutureExt};
use ::std::collections::HashMap;
use ::std::fmt::Debug;
use ::std::hash::Hash;

pub trait Deletable<'query, C, Tab, I, Id, DeletedAt, DeletePatch, Selection>: Sized {
    fn hard_delete<'life0, 'async_trait>(
        conn: &'life0 mut C,
        input: I,
        selection: Selection,
    ) -> BoxFuture<'async_trait, Result<Vec<Self>, Error>>
    where
        'life0: 'async_trait,
        'query: 'async_trait,
        Self: 'async_trait;

    fn maybe_soft_delete<'life0, 'async_trait>(
        conn: &'life0 mut C,
        input: I,
        selection: Selection,
    ) -> BoxFuture<'async_trait, Either<I, Result<Vec<Self>, Error>>>
    where
        'life0: 'async_trait,
        'query: 'async_trait,
        Self: 'async_trait;
}

pub trait SoftDeletableColumn {
    type Column: Column<SqlType = Nullable<Self::SqlType>> + Default + ExpressionMethods;
    type SqlType: SqlType;
}

pub trait SoftDeletable {
    type DeletedAt: SoftDeletableColumn;
}

impl<T> SoftDeletableColumn for (T, Nullable<Timestamp>)
where
    T: Column<SqlType = Nullable<Timestamp>> + Default + ExpressionMethods,
{
    type Column = T;
    type SqlType = Timestamp;
}

#[cfg(feature = "diesel-postgres")]
impl<T> SoftDeletableColumn for (T, Nullable<diesel::pg::sql_types::Timestamptz>)
where
    T: Column<SqlType = Nullable<diesel::pg::sql_types::Timestamptz>> + Default + ExpressionMethods,
{
    type Column = T;
    type SqlType = diesel::pg::sql_types::Timestamptz;
}

impl<'query, DB, C, Tab, I, Id, F, DeletedAt, DeletePatch, Selection, T>
    Deletable<'query, C, Tab, I, Id, DeletedAt, DeletePatch, Selection> for T
where
    T: HasTable<Table = Tab> + MaybeAudit<'query, C> + Send,

    DB: Backend,
    C: AsyncConnection<Backend = DB> + 'static,

    // Id bounds
    I: IntoIterator + Send + Sized + 'query,
    I::Item: Debug + Send + Sync + Into<Id>,
    Id: AsExpression<SqlTypeOf<Tab::PrimaryKey>>,

    // table bounds
    Tab: Table + QueryId + Send,
    Tab::PrimaryKey: Expression + ExpressionMethods,
    <Tab::PrimaryKey as Expression>::SqlType: SqlType,

    // delete bounds
    Tab: FilterDsl<ht::EqAny<Tab::PrimaryKey, Vec<Id>>, Output = F>,
    <Tab as QuerySource>::FromClause: Send,
    F: HasTable<Table = Tab> + IntoUpdateTarget + Send + 'query,
    <F as IntoUpdateTarget>::WhereClause: Send,
    DeleteStatement<F::Table, F::WhereClause, ReturningClause<Selection>>:
        LoadQuery<'query, C, T> + QueryFragment<DB> + QueryId + Send + 'query,

    // Selection bounds
    Selection: SelectableExpression<Tab> + Send + ValidGrouping<()>,
{
    fn hard_delete<'life0, 'async_trait>(
        conn: &'life0 mut C,
        input: I,
        selection: Selection,
    ) -> BoxFuture<'async_trait, Result<Vec<Self>, Error>>
    where
        'query: 'async_trait,
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        async move {
            let query = Self::table().filter(
                Self::table()
                    .primary_key()
                    .eq_any(input.into_iter().map(Into::<Id>::into).collect::<Vec<_>>()),
            );
            let records = diesel::delete(query).returning(selection).get_results(conn).await?;

            Self::maybe_insert_audit_records(conn, &records).await?;

            Ok(records)
        }
        .boxed()
    }

    #[allow(unused_variables)]
    default fn maybe_soft_delete<'life0, 'async_trait>(
        _: &'life0 mut C,
        input: I,
        _: Selection,
    ) -> BoxFuture<'async_trait, Either<I, Result<Vec<Self>, Error>>>
    where
        'query: 'async_trait,
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(ready(Either::Left(input)))
    }
}

impl<'query, DB, C, Tab, I, Id, F, DeletedAt, DeletePatch, Selection, T>
    Deletable<'query, C, Tab, I, Id, DeletedAt, DeletePatch, Selection> for T
where
    T: HasTable<Table = Tab> + MaybeAudit<'query, C> + Send,

    DB: Backend,
    C: AsyncConnection<Backend = DB> + 'static,

    // Id bounds
    I: IntoIterator + Send + Sized + 'query,
    I::Item: Debug + Send + Sync + Into<Id>,
    Id: AsExpression<SqlTypeOf<Tab::PrimaryKey>>,

    // table bounds
    Tab: Table + QueryId + Send,
    Tab::PrimaryKey: Expression + ExpressionMethods,
    <Tab::PrimaryKey as Expression>::SqlType: SqlType,

    // delete bounds
    Tab: FilterDsl<ht::EqAny<Tab::PrimaryKey, Vec<Id>>, Output = F>,
    <Tab as QuerySource>::FromClause: Send,
    F: HasTable<Table = Tab> + IntoUpdateTarget + Send + 'query,
    <F as IntoUpdateTarget>::WhereClause: Send,
    DeleteStatement<F::Table, F::WhereClause, ReturningClause<Selection>>:
        LoadQuery<'query, C, T> + QueryFragment<DB> + QueryId + Send + 'query,

    // Selection bounds
    Selection: SelectableExpression<Tab> + Send + ValidGrouping<()>,

    // specialization
    Id: Clone + Eq + Hash + Send + Sync + 'query,

    for<'a> &'a T: Identifiable<Id = &'a Id>,

    for<'a> &'a DeletePatch: HasTable<Table = Tab> + Identifiable<Id = &'a Id> + IntoUpdateTarget,
    for<'a> <&'a DeletePatch as IntoUpdateTarget>::WhereClause: Send,

    I::Item: Into<DeletePatch>,
    DeletePatch: AsChangeset<Target = Tab> + Debug + HasTable<Table = Tab> + IncludesChanges + Send + Sync + 'query,
    DeletePatch::Changeset: Send,

    <Tab as QuerySource>::FromClause: Send,

    // UpdateStatement bounds
    Tab: FindDsl<Id>,
    ht::Find<Tab, Id>: HasTable<Table = Tab> + IntoUpdateTarget + Send,
    <ht::Find<Tab, Id> as IntoUpdateTarget>::WhereClause: Send,
    UpdateStatement<
        <ht::Find<Tab, Id> as HasTable>::Table,
        <ht::Find<Tab, Id> as IntoUpdateTarget>::WhereClause,
        <DeletePatch as AsChangeset>::Changeset,
    >: AsQuery,
    UpdateStatement<
        <ht::Find<Tab, Id> as HasTable>::Table,
        <ht::Find<Tab, Id> as IntoUpdateTarget>::WhereClause,
        <DeletePatch as AsChangeset>::Changeset,
        ReturningClause<Selection>,
    >: AsQuery + LoadQuery<'query, C, T> + Send,

    // Filter bounds for records whose changesets do not include any changes
    F: IsNotDeleted<'query, C, T, T>,
    <F as IsNotDeleted<'query, C, T, T>>::IsNotDeletedFilter: SelectDsl<Selection>,
    for<'a> ht::Select<<F as IsNotDeleted<'a, C, T, T>>::IsNotDeletedFilter, Selection>: LoadQuery<'a, C, T> + Send,

    // Selection bounds
    Selection: Clone + Sync,
    <Selection as ValidGrouping<()>>::IsAggregate: diesel::expression::MixedAggregates<
        diesel::expression::is_aggregate::No,
        Output = diesel::expression::is_aggregate::No,
    >,
{
    default fn maybe_soft_delete<'life0, 'async_trait>(
        conn: &'life0 mut C,
        input: I,
        selection: Selection,
    ) -> BoxFuture<'async_trait, Either<I, Result<Vec<Self>, Error>>>
    where
        'query: 'async_trait,
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let result = (move || async move {
                let patches = input.into_iter().map(Into::into).collect::<Vec<DeletePatch>>();
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

                let num_changed_patches = ids.len() - no_change_patch_ids.len();
                if num_changed_patches == 0 {
                    return Ok(vec![]);
                }
                let mut all_updated = Vec::with_capacity(num_changed_patches);
                for patch in patches.into_iter().filter(|patch| patch.includes_changes()) {
                    let record = diesel::update(Self::table().find(patch.id().to_owned()))
                        .set(patch)
                        .returning(selection.clone())
                        .get_result::<Self>(conn)
                        .await?;
                    all_updated.push(record);
                }

                Self::maybe_insert_audit_records(conn, &all_updated).await?;

                let filter = FilterDsl::filter(Self::table(), Self::table().primary_key().eq_any(no_change_patch_ids))
                    .is_not_deleted();
                let unchanged_records = filter.select(selection).get_results::<Self>(&mut *conn).await?;

                let mut all_records = unchanged_records
                    .into_iter()
                    .chain(all_updated.into_iter())
                    .map(|record| (record.id().to_owned(), record))
                    .collect::<HashMap<_, _>>();

                Ok(ids.iter().map(|id| all_records.remove(id).unwrap()).collect::<Vec<_>>())
            })()
            .await;
            Either::Right(result)
        })
    }
}
