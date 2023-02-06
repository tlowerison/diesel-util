use crate::*;
use diesel::associations::HasTable;
use diesel::dsl::SqlTypeOf;
use diesel::expression::{AsExpression, Expression};
use diesel::expression_methods::ExpressionMethods;
use diesel::helper_types as ht;
use diesel::query_dsl::methods::{FilterDsl, FindDsl};
use diesel::query_source::QuerySource;
use diesel::sql_types::SqlType;
use diesel::{query_builder::*, Identifiable};
use diesel::{Insertable, Table};
use diesel_async::methods::*;
use futures::future::FutureExt;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::hash::Hash;

pub trait DbEntity: Sized + Send + 'static {
    type Raw: Audit + Clone + HasTable<Table = Self::Table> + TryInto<Self> + Send + 'static;
    type Table: Table + QueryId + Send;

    type Id: AsExpression<SqlTypeOf<<Self::Table as Table>::PrimaryKey>>
    where
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType;
}

#[async_trait]
pub trait DbGet<D: _Db>: DbEntity {
    #[framed]
    #[instrument(skip(db, ids))]
    async fn get<'query, F>(db: &D, ids: impl IntoIterator<Item = Self::Id> + Send) -> Result<Vec<Self>, DbError>
    where
        // Error bounds
        <Self::Raw as TryInto<Self>>::Error: Into<DbError>,

        // Id bounds
        Self::Id: Debug + Send,
        for<'a> &'a Self::Raw: Identifiable<Id = &'a Self::Id>,
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

        Self::Table: FilterDsl<ht::EqAny<<Self::Table as Table>::PrimaryKey, Vec<Self::Id>>, Output = F>,
        F: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>,
    {
        let ids = ids.into_iter().collect::<Vec<_>>();
        tracing::Span::current().record("ids", &*format!("{ids:?}"));

        if ids.is_empty() {
            return Ok(vec![]);
        }

        let result: Result<Vec<Self::Raw>, _> = db.get(ids).await;
        match result {
            Ok(records) => Ok(records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(Into::into)?),
            Err(err) => {
                error!(target: module_path!(), error = %err);
                Err(err.into())
            }
        }
    }

    #[framed]
    #[instrument(skip(db))]
    async fn get_one<'query, F>(db: &D, id: Self::Id) -> Result<Self, DbError>
    where
        // Error bounds
        <Self::Raw as TryInto<Self>>::Error: Into<DbError>,

        // Id bounds
        Self::Id: Debug + Send,
        for<'a> &'a Self::Raw: Identifiable<Id = &'a Self::Id>,
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

        Self::Table: FilterDsl<ht::EqAny<<Self::Table as Table>::PrimaryKey, [Self::Id; 1]>, Output = F>,
        F: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>,
    {
        let result: Result<Vec<Self::Raw>, _> = db.get([id]).await;
        match result {
            Ok(records) => Ok(records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()
                .map_err(Into::into)?
                .pop()
                .ok_or_else(|| DbError::bad_request("record not found"))?),
            Err(err) => {
                error!(target: module_path!(), error = %err);
                Err(err.into())
            }
        }
    }

    #[framed]
    #[instrument(skip(db, values))]
    async fn get_by_column<'query, C, U, Q>(
        db: &D,
        column: C,
        values: impl IntoIterator<Item = U> + Send,
    ) -> Result<Vec<Self>, DbError>
    where
        // Error bounds
        <Self::Raw as TryInto<Self>>::Error: Into<DbError>,

        // Id bounds
        U: AsExpression<SqlTypeOf<C>> + Debug + Send,
        C: Debug + Expression + ExpressionMethods + Send + 'query,
        SqlTypeOf<C>: SqlType,

        Self::Table: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>,
        <Self::Table as IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>>::IsNotDeletedFilter:
            FilterDsl<ht::EqAny<C, Vec<U>>, Output = Q>,
        Q: Send + LoadQuery<'query, D::AsyncConnection, Self::Raw> + 'query,
    {
        let values = values.into_iter().collect::<Vec<_>>();
        tracing::Span::current().record("values", &*format!("{values:?}"));

        let result: Result<Vec<Self::Raw>, _> = db.get_by_column(column, values).await;
        match result {
            Ok(records) => Ok(records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(Into::into)?),
            Err(err) => {
                error!(target: module_path!(), error = %err);
                Err(err.into())
            }
        }
    }

    #[framed]
    #[instrument(skip(db))]
    async fn get_page<'query, P, F>(db: &D, page: P) -> Result<Vec<Self>, DbError>
    where
        // Error bounds
        <Self::Raw as TryInto<Self>>::Error: Into<DbError>,

        // Page bounds
        P: Borrow<Page> + Debug + Send,

        // Query bounds
        Self::Table: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw, IsNotDeletedFilter = F>,
        F: Paginate + Send,
        Paginated<<F as AsQuery>::Query>: Send + LoadQuery<'query, D::AsyncConnection, Self::Raw> + 'query,
    {
        if page.borrow().is_empty() {
            return Ok(vec![]);
        }
        let result: Result<Vec<Self::Raw>, _> = db.get_page(page).await;
        match result {
            Ok(records) => Ok(records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(Into::into)?),
            Err(err) => {
                error!(target: module_path!(), error = %err);
                Err(err.into())
            }
        }
    }

    #[framed]
    #[instrument(skip(db, pages))]
    async fn get_pages<'query, P, F>(db: &D, pages: impl IntoIterator<Item = P> + Send) -> Result<Vec<Self>, DbError>
    where
        // Error bounds
        <Self::Raw as TryInto<Self>>::Error: Into<DbError>,

        // Page bounds
        P: Borrow<Page> + Debug + for<'a> PageRef<'a> + Send,

        // Query bounds
        Self::Table: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw, IsNotDeletedFilter = F>,
        F: Paginate + Send,
        Paginated<<F as AsQuery>::Query>: Send + LoadQuery<'query, D::AsyncConnection, Self::Raw> + 'query,
    {
        let pages = pages.into_iter().collect::<Vec<_>>();
        tracing::Span::current().record("pages", &*format!("{pages:?}"));

        if pages.iter().all(|page| page.borrow().is_empty()) {
            return Ok(vec![]);
        }

        let result: Result<Vec<Self::Raw>, _> = db.get_pages(pages).await;
        match result {
            Ok(records) => Ok(records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(Into::into)?),
            Err(err) => {
                error!(target: module_path!(), error = %err);
                Err(err.into())
            }
        }
    }
}

#[async_trait]
pub trait DbInsert<D: _Db>: DbEntity {
    type PostHelper<'a>: Debug + Into<Self::Post<'a>> + Send = Self::Post<'a>;
    type Post<'a>: Debug + HasTable<Table = Self::Table> + Send;

    #[framed]
    #[instrument(skip(db, posts))]
    async fn insert<'p>(db: &D, posts: impl IntoIterator<Item = Self::PostHelper<'p>> + Send + 'p) -> Result<Vec<Self>, DbError>
    where
    // Error bounds
        <Self::Raw as TryInto<Self>>::Error: Into<DbError>,

    // Insertable bounds
        Vec<Self::Post<'p>>: Insertable<Self::Table> + Send,
        <Vec<Self::Post<'p>> as Insertable<Self::Table>>::Values: Send,
        <Self::Table as QuerySource>::FromClause: Send,

    // Insert bounds
        for<'query> InsertStatement<Self::Table, <Vec<Self::Post<'p>> as Insertable<Self::Table>>::Values>: LoadQuery<'query, D::AsyncConnection, Self::Raw>,
        for<'query> InsertStatement<<<Self::Raw as Audit>::AuditTable as HasTable>::Table, <Vec<<Self::Raw as Audit>::AuditRow> as Insertable<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>>::Values>: LoadQuery<'query, D::AsyncConnection, <Self::Raw as Audit>::AuditRow>,

    // Audit bounds
        <Self::Raw as Audit>::AuditRow: Send,
        Vec<<Self::Raw as Audit>::AuditRow>: Insertable<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>
            + UndecoratedInsertRecord<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>,
        <<Self::Raw as Audit>::AuditTable as HasTable>::Table: Table + QueryId + Send,
        <<<Self::Raw as Audit>::AuditTable as HasTable>::Table as QuerySource>::FromClause: Send,
        <Vec<<Self::Raw as Audit>::AuditRow> as Insertable<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>>::Values: Send,
    {
        let db_post_helpers = posts.into_iter().collect::<Vec<_>>();
        tracing::Span::current().record("posts", &*format!("{db_post_helpers:?}"));

        if db_post_helpers.is_empty() {
            return Ok(vec![]);
        }

        db.insert(db_post_helpers.into_iter().map(Self::PostHelper::into))
            .map(|result| match result {
                Ok(records) => Ok(records
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()
                    .map_err(Into::into)?),
                Err(err) => {
                    let err = err;
                    error!(target: module_path!(), error = %err);
                    Err(err)
                }
            })
            .await
    }
}

#[async_trait]
pub trait DbSoftDelete<D: _Db>: DbEntity {
    type DeletePatchHelper<'a>: Debug + Into<Self::DeletePatch<'a>> + Send = Self::DeletePatch<'a>;
    type DeletePatch<'a>: AsChangeset<Target = Self::Table>
        + Debug
        + HasTable<Table = Self::Table>
        + IncludesChanges
        + Send
        + Sync;

    #[framed]
    #[instrument(skip(db, delete_patches))]
    async fn delete<'p, F>(db: &D, delete_patches: impl IntoIterator<Item = Self::DeletePatchHelper<'p>> + Send + 'p) -> Result<Vec<Self>, DbError>
    where
    // Error bounds
        <Self::Raw as TryInto<Self>>::Error: Into<DbError>,

    // Id bounds
        Self::Id: Clone + Hash + Eq + Send + Sync,
        for<'a> &'a Self::Raw: Identifiable<Id = &'a Self::Id>,
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

    // Changeset bounds
        <Self::DeletePatch<'p> as AsChangeset>::Changeset: Send,
        for<'a> &'a Self::DeletePatch<'p>: HasTable<Table = Self::Table> + Identifiable<Id = &'a Self::Id> + IntoUpdateTarget,
        for<'a> <&'a Self::DeletePatch<'p> as IntoUpdateTarget>::WhereClause: Send,
        <Self::Table as QuerySource>::FromClause: Send,

    // UpdateStatement bounds
        Self::Table: FindDsl<Self::Id>,
        ht::Find<Self::Table, Self::Id>: HasTable<Table = Self::Table> + IntoUpdateTarget + Send,
        <ht::Find<Self::Table, Self::Id> as IntoUpdateTarget>::WhereClause: Send,
        for<'query> ht::Update<ht::Find<Self::Table, Self::Id>, Self::DeletePatch<'p>>: AsQuery + LoadQuery<'query, D::AsyncConnection, Self::Raw> + Send,

    // Filter bounds for records whose changesets do not include any changes
        Self::Table: FilterDsl<ht::EqAny<<Self::Table as Table>::PrimaryKey, Vec<Self::Id>>, Output = F>,
        for<'query> F: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>,

    // Audit bounds
        Self::Raw: Audit + Clone,
        <Self::Raw as Audit>::AuditRow: Send,
        Vec<<Self::Raw as Audit>::AuditRow>: Insertable<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>
            + UndecoratedInsertRecord<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>,
        <<Self::Raw as Audit>::AuditTable as HasTable>::Table: Table + QueryId + Send,
        <<<Self::Raw as Audit>::AuditTable as HasTable>::Table as QuerySource>::FromClause: Send,
        <Vec<<Self::Raw as Audit>::AuditRow> as Insertable<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>>::Values: Send,
        for<'query> InsertStatement<
            <<Self::Raw as Audit>::AuditTable as HasTable>::Table,
            <Vec<<Self::Raw as Audit>::AuditRow> as Insertable<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>>::Values,
        >: LoadQuery<'query, D::AsyncConnection, <Self::Raw as Audit>::AuditRow>,
    {
        let db_delete_patch_helpers = delete_patches.into_iter().collect::<Vec<_>>();
        tracing::Span::current().record("delete_patches", &*format!("{db_delete_patch_helpers:?}"));

        if db_delete_patch_helpers.is_empty() {
            return Ok(vec![]);
        }

        db.update(db_delete_patch_helpers.into_iter().map(Self::DeletePatchHelper::into))
            .map(|result| match result {
                Ok(records) => Ok(records
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()
                    .map_err(Into::into)?),
                Err(err) => {
                    let err = err;
                    error!(target: module_path!(), error = %err);
                    Err(err)
                }
            })
            .await
    }
}

#[async_trait]
pub trait DbUpdate<D: _Db>: DbEntity {
    type PatchHelper<'a>: Debug + Into<Self::Patch<'a>> + Send = Self::Patch<'a>;
    type Patch<'a>: AsChangeset<Target = Self::Table>
        + Debug
        + HasTable<Table = Self::Table>
        + IncludesChanges
        + Send
        + Sync;

    #[framed]
    #[instrument(skip(db, patches))]
    async fn update<'p, F>(db: &D, patches: impl IntoIterator<Item = Self::PatchHelper<'p>> + Send + 'p) -> Result<Vec<Self>, DbError>
    where
    // Error bounds
        <Self::Raw as TryInto<Self>>::Error: Into<DbError>,

    // Id bounds
        Self::Id: Clone + Hash + Eq + Send + Sync,
        for<'a> &'a Self::Raw: Identifiable<Id = &'a Self::Id>,
        <Self::Table as Table>::PrimaryKey: Expression + ExpressionMethods,
        <<Self::Table as Table>::PrimaryKey as Expression>::SqlType: SqlType,

    // Changeset bounds
        <Self::Patch<'p> as AsChangeset>::Changeset: Send,
        for<'a> &'a Self::Patch<'p>: HasTable<Table = Self::Table> + Identifiable<Id = &'a Self::Id> + IntoUpdateTarget,
        for<'a> <&'a Self::Patch<'p> as IntoUpdateTarget>::WhereClause: Send,
        <Self::Table as QuerySource>::FromClause: Send,

    // UpdateStatement bounds
        Self::Table: FindDsl<Self::Id>,
        ht::Find<Self::Table, Self::Id>: HasTable<Table = Self::Table> + IntoUpdateTarget + Send,
        <ht::Find<Self::Table, Self::Id> as IntoUpdateTarget>::WhereClause: Send,
        for<'query> ht::Update<ht::Find<Self::Table, Self::Id>, Self::Patch<'p>>: AsQuery + LoadQuery<'query, D::AsyncConnection, Self::Raw> + Send,

    // Filter bounds for records whose changesets do not include any changes
        Self::Table: FilterDsl<ht::EqAny<<Self::Table as Table>::PrimaryKey, Vec<Self::Id>>, Output = F>,
        for<'query> F: IsNotDeleted<'query, D::AsyncConnection, Self::Raw, Self::Raw>,

    // Audit bounds
        Self::Raw: Audit + Clone,
        <Self::Raw as Audit>::AuditRow: Send,
        Vec<<Self::Raw as Audit>::AuditRow>: Insertable<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>
            + UndecoratedInsertRecord<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>,
        <<Self::Raw as Audit>::AuditTable as HasTable>::Table: Table + QueryId + Send,
        <<<Self::Raw as Audit>::AuditTable as HasTable>::Table as QuerySource>::FromClause: Send,
        <Vec<<Self::Raw as Audit>::AuditRow> as Insertable<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>>::Values: Send,
        for<'query> InsertStatement<
            <<Self::Raw as Audit>::AuditTable as HasTable>::Table,
            <Vec<<Self::Raw as Audit>::AuditRow> as Insertable<<<Self::Raw as Audit>::AuditTable as HasTable>::Table>>::Values,
        >: LoadQuery<'query, D::AsyncConnection, <Self::Raw as Audit>::AuditRow>,
    {
        let db_patch_helpers = patches.into_iter().collect::<Vec<_>>();
        tracing::Span::current().record("patches", &*format!("{db_patch_helpers:?}"));

        if db_patch_helpers.is_empty() {
            return Ok(vec![]);
        }

        db.update(db_patch_helpers.into_iter().map(Self::PatchHelper::into))
            .map(|result| match result {
                Ok(records) => Ok(records
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()
                    .map_err(Into::into)?),
                Err(err) => {
                    let err = err;
                    error!(target: module_path!(), error = %err);
                    Err(err)
                }
            })
            .await
    }
}

/// DbEntity is automatically implemented for any type which implements Audit and HasTable
impl<T, Tab, Id> DbEntity for T
where
    T: Audit + Clone + HasTable<Table = Tab> + Send + 'static,
    Tab: Table + QueryId + Send,

    Id: AsExpression<SqlTypeOf<Tab::PrimaryKey>>,
    for<'a> &'a T: Identifiable<Id = &'a Id>,
    Tab::PrimaryKey: Expression + ExpressionMethods,
    <Tab::PrimaryKey as Expression>::SqlType: SqlType,
{
    type Raw = T;
    type Table = Tab;
    type Id = Id;
}

impl<T: DbEntity, D: _Db> DbGet<D> for T {}
