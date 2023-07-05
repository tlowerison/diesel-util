use crate::paginate::{AsPage, Page, PageExt, PageRef};
use crate::paginate::{PageCursor, PageOffset};
use crate::{ColumnCursorComparisonExpression, ColumnOrderByExpression, PageSplit};
use diesel::backend::Backend;
use diesel::helper_types::{Asc, Desc};
use diesel::query_builder::*;
use diesel::query_dsl::methods::{FilterDsl, OrderDsl};
use diesel::serialize::ToSql;
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel::Column;
use diesel::{prelude::*, FromSqlRow};
use dyn_clone::clone_box;
use either::Either::*;
use itertools::intersperse;
use itertools::Itertools;
use std::hash::Hash;
use std::marker::PhantomData;
use uuid::Uuid;

static PAGE_ID_COLUMN_NAME: &str = "page_id";
static OFFSET_COLUMN_NAME: &str = "offset";
static OFFSET_SUBQUERY1_ALIAS: &str = "q1";
static OFFSET_SUBQUERY2_ALIAS: &str = "q2";

#[derive(Debug, Clone)]
pub struct PaginatedQuery<QS: ?Sized, Q0, Q2, P = (), PO = ()> {
    query: Q0,
    cursor_queries: Vec<Q2>,
    /// must include PageOffset so that its `left` and `right` fields
    /// can be referenced in `QueryFragment::walk_ast` (the values
    /// cannot be created within the scope of the method)
    pages: Option<Vec<Page<QS>>>,
    partition: Option<P>,
    partition_order: Option<PO>,
}

#[derive(Clone, Debug, FromSqlRow)]
pub struct PaginatedResult<T> {
    pub result: T,
    pub row_number: Option<i64>,
    pub page_id: Option<Uuid>,
}

#[derive(Clone, Copy, Debug, Deref, DerefMut, From)]
pub struct PaginatedQueryWrapper<T, DB>(
    #[deref]
    #[deref_mut]
    pub T,
    PhantomData<DB>,
);

pub trait Paginate<DB: Backend, QS: ?Sized>: AsQuery + Clone + Send + Sized {
    type Output: Paginated<DB>;

    fn paginate<P: AsPage<QS>>(self, page: P) -> PaginatedQueryWrapper<Self::Output, DB>;

    fn multipaginate<P, I>(self, pages: I) -> PaginatedQueryWrapper<Self::Output, DB>
    where
        P: for<'a> PageRef<'a, QS>,
        I: Iterator<Item = P>;
}

impl<DB, QS, Q0, Q1, Q2, SqlType> Paginate<DB, QS> for Q0
where
    DB: Backend,
    QS: diesel::QuerySource,
    Q0: Clone
        + Query<SqlType = SqlType>
        + QueryFragment<DB>
        + QueryId
        + Send
        + FilterDsl<Box<dyn ColumnCursorComparisonExpression<QS>>, Output = Q1>,
    Q1: Send + OrderDsl<Box<dyn ColumnOrderByExpression<QS>>, Output = Q2>,
    Q2: Query<SqlType = SqlType> + QueryFragment<DB> + Send,
    SqlType: 'static,
{
    type Output = PaginatedQuery<QS, Q0, Q2, (), ()>;

    fn paginate<P>(self, page: P) -> PaginatedQueryWrapper<Self::Output, DB>
    where
        P: AsPage<QS>,
    {
        let query = self.as_query();
        let pages = page.as_page().map(|page| vec![page.clone()]);

        let mut cursor_queries = Vec::<Q2>::default();
        if let Some(pages) = pages.as_ref() {
            let PageSplit { cursor_indices, .. } = Page::split(pages);

            let page_cursors: Vec<&PageCursor<QS>> = cursor_indices
                .into_iter()
                .map(|i| pages[i].as_cursor().unwrap())
                .collect_vec();

            for page_cursor in page_cursors.iter() {
                let query: Q0 = query.clone();
                let query: Q1 = query.filter(clone_box(&*page_cursor.column_cursor_comparison_expression));
                let query: Q2 = query.order(clone_box(&*page_cursor.column_order_by_expression));
                cursor_queries.push(query);
            }
        }

        PaginatedQueryWrapper(
            PaginatedQuery::<QS, Q0, Q2, (), ()> {
                query,
                cursor_queries,
                pages,
                partition: None,
                partition_order: None,
            },
            Default::default(),
        )
    }

    fn multipaginate<P, I>(self, pages: I) -> PaginatedQueryWrapper<Self::Output, DB>
    where
        P: for<'a> PageRef<'a, QS>,
        I: Iterator<Item = P>,
    {
        let query = self.as_query();

        let pages = Page::merge(pages.map(|page| page.page_ref().clone()))
            .into_iter()
            .map(Into::into)
            .collect_vec();

        let PageSplit { cursor_indices, .. } = Page::split(&pages);

        let page_cursors: Vec<&PageCursor<QS>> = cursor_indices
            .into_iter()
            .map(|i| pages[i].as_cursor().unwrap())
            .collect_vec();

        let mut cursor_queries = Vec::<Q2>::default();
        for page_cursor in page_cursors.iter() {
            let query: Q0 = query.clone();
            let query: Q1 = query.filter(clone_box(&*page_cursor.column_cursor_comparison_expression));
            let query: Q2 = query.order(clone_box(&*page_cursor.column_order_by_expression));
            cursor_queries.push(query);
        }

        PaginatedQueryWrapper(
            PaginatedQuery::<QS, Q0, Q2, (), ()> {
                query,
                cursor_queries,
                pages: Some(pages),
                partition: None,
                partition_order: None,
            },
            Default::default(),
        )
    }
}

#[allow(opaque_hidden_inferred_bound)]
pub trait Paginated<DB: Backend>:
    Query<SqlType = (Self::InternalSqlType, Nullable<BigInt>, Nullable<Text>)> + Send + Sized
{
    type QuerySource: diesel::QuerySource;
    type Q0<'q>: Clone
        + Query<SqlType = Self::InternalSqlType>
        + QueryFragment<DB>
        + QueryId
        + Send
        + FilterDsl<Box<dyn ColumnCursorComparisonExpression<Self::QuerySource>>, Output = Self::Q1<'q>>
        + 'q
    where
        Self: 'q;
    type Q1<'q>: Send + OrderDsl<Box<dyn ColumnOrderByExpression<Self::QuerySource>>, Output = Self::Q2<'q>>
    where
        Self: 'q;
    type Q2<'q>: Send + Query<SqlType = Self::InternalSqlType> + QueryFragment<DB>
    where
        Self: 'q;
    type P: Partition + Send;
    type PO: PartitionOrder<DB> + Send;
    type QueryId: 'static;
    type InternalSqlType: 'static;

    type Partitioned<'q, Expr: Partition + Send + 'q>: Paginated<
            DB,
            Q0<'q> = Self::Q0<'q>,
            Q1<'q> = Self::Q1<'q>,
            Q2<'q> = Self::Q2<'q>,
            P = Expr,
            PO = Self::PO,
            QueryId = Self::QueryId,
            SqlType = Self::SqlType,
            InternalSqlType = Self::InternalSqlType,
        > + 'q
    where
        Self: 'q;

    type PartitionOrdered<'q, Expr: PartitionOrder<DB> + Send + 'q>: Paginated<
            DB,
            Q0<'q> = Self::Q0<'q>,
            Q1<'q> = Self::Q1<'q>,
            Q2<'q> = Self::Q2<'q>,
            P = Self::P,
            PO = Expr,
            QueryId = Self::QueryId,
            SqlType = Self::SqlType,
            InternalSqlType = Self::InternalSqlType,
        > + 'q
    where
        Self: 'q;

    fn partition<'q, Expr: Partition + Send + 'q>(self, expr: Expr) -> Self::Partitioned<'q, Expr>
    where
        Self: 'q;
    fn partition_order<'q, Expr: PartitionOrder<DB> + Send + 'q>(self, expr: Expr) -> Self::PartitionOrdered<'q, Expr>
    where
        Self: 'q;

    fn get_query(&self) -> &Self::Q0<'_>;
    fn get_cursor_queries(&self) -> &[Self::Q2<'_>];
    fn get_partition(&self) -> Option<&Self::P>;
    fn get_partition_order(&self) -> Option<&Self::PO>;
    fn get_pages(&self) -> Option<&[Page<Self::QuerySource>]>;

    fn map_query<'q, F0, F2, NewQ0, NewQ1, NewQ2>(
        self,
        f0: F0,
        f2: F2,
    ) -> impl Paginated<
        DB,
        Q0<'q> = NewQ0,
        Q1<'q> = NewQ1,
        Q2<'q> = NewQ2,
        P = Self::P,
        PO = Self::PO,
        QueryId = NewQ0::QueryId,
        SqlType = Self::SqlType,
        InternalSqlType = Self::InternalSqlType,
    > + 'q
    where
        Self: 'q,
        F0: FnOnce(Self::Q0<'q>) -> NewQ0,
        F2: FnMut(Self::Q2<'q>) -> NewQ2,
        NewQ0: Clone
            + Query<SqlType = Self::InternalSqlType>
            + QueryId
            + QueryFragment<DB>
            + Send
            + FilterDsl<Box<dyn ColumnCursorComparisonExpression<Self::QuerySource>>, Output = NewQ1>
            + 'q,
        NewQ1: Send + OrderDsl<Box<dyn ColumnOrderByExpression<Self::QuerySource>>, Output = NewQ2> + 'q,
        NewQ2: Query<SqlType = Self::InternalSqlType> + QueryFragment<DB> + QueryId + Send + 'q;
}

#[allow(opaque_hidden_inferred_bound)]
impl<DB, QS, Q0, Q1, Q2, P, PO> Paginated<DB> for PaginatedQuery<QS, Q0, Q2, P, PO>
where
    DB: Backend,
    QS: diesel::QuerySource,
    Q0: Clone
        + Query
        + QueryFragment<DB>
        + QueryId
        + Send
        + FilterDsl<Box<dyn ColumnCursorComparisonExpression<QS>>, Output = Q1>,
    Q0::SqlType: 'static,
    Q1: Send + OrderDsl<Box<dyn ColumnOrderByExpression<QS>>, Output = Q2>,
    Q2: Query<SqlType = Q0::SqlType> + QueryFragment<DB> + Send,
    P: Partition + Send,
    PO: PartitionOrder<DB> + Send,
{
    type QuerySource = QS;
    type Q0<'q> = Q0 where Self: 'q;
    type Q1<'q> = Q1 where Self: 'q;
    type Q2<'q> = Q2 where Self: 'q;
    type P = P;
    type PO = PO;
    type QueryId = Q0::QueryId;
    type InternalSqlType = Q0::SqlType;

    type Partitioned<'q, Expr: Partition + Send + 'q> = PaginatedQuery<QS, Q0, Q2, Expr, PO> where Self: 'q;
    type PartitionOrdered<'q, Expr: PartitionOrder<DB> + Send + 'q> = PaginatedQuery<QS, Q0, Q2, P, Expr> where Self: 'q;

    fn partition<'q, Expr: Partition + Send + 'q>(self, expr: Expr) -> Self::Partitioned<'q, Expr>
    where
        Self: 'q,
    {
        PaginatedQuery {
            query: self.query,
            cursor_queries: self.cursor_queries,
            pages: self.pages,
            partition: Some(expr),
            partition_order: self.partition_order,
        }
    }

    fn partition_order<'q, Expr: PartitionOrder<DB> + Send + 'q>(self, expr: Expr) -> Self::PartitionOrdered<'q, Expr>
    where
        Self: 'q,
    {
        PaginatedQuery {
            query: self.query,
            cursor_queries: self.cursor_queries,
            pages: self.pages,
            partition: self.partition,
            partition_order: Some(expr),
        }
    }

    fn get_query(&self) -> &Self::Q0<'_> {
        &self.query
    }
    fn get_cursor_queries(&self) -> &[Self::Q2<'_>] {
        &self.cursor_queries
    }
    fn get_partition(&self) -> Option<&Self::P> {
        self.partition.as_ref()
    }
    fn get_partition_order(&self) -> Option<&Self::PO> {
        self.partition_order.as_ref()
    }
    fn get_pages(&self) -> Option<&[Page<Self::QuerySource>]> {
        self.pages.as_deref()
    }

    fn map_query<'q, F0, F2, NewQ0, NewQ1, NewQ2>(
        self,
        f0: F0,
        f2: F2,
    ) -> impl Paginated<
        DB,
        Q0<'q> = NewQ0,
        Q1<'q> = NewQ1,
        Q2<'q> = NewQ2,
        P = Self::P,
        PO = Self::PO,
        QueryId = NewQ0::QueryId,
        SqlType = Self::SqlType,
        InternalSqlType = Self::InternalSqlType,
    > + 'q
    where
        Self: 'q,
        F0: FnOnce(Self::Q0<'q>) -> NewQ0,
        F2: FnMut(Self::Q2<'q>) -> NewQ2,
        NewQ0: Clone
            + Query<SqlType = Self::InternalSqlType>
            + QueryId
            + QueryFragment<DB>
            + Send
            + FilterDsl<Box<dyn ColumnCursorComparisonExpression<Self::QuerySource>>, Output = NewQ1>
            + 'q,
        NewQ1: Send + OrderDsl<Box<dyn ColumnOrderByExpression<Self::QuerySource>>, Output = NewQ2> + 'q,
        NewQ2: Query<SqlType = Self::InternalSqlType> + QueryFragment<DB> + Send + 'q,
    {
        PaginatedQuery::<QS, NewQ0, NewQ2, Self::P, Self::PO> {
            query: f0(self.query),
            cursor_queries: self.cursor_queries.into_iter().map(f2).collect_vec(),
            pages: self.pages,
            partition: self.partition,
            partition_order: self.partition_order,
        }
    }
}

#[allow(opaque_hidden_inferred_bound)]
impl<DB: Backend + Send, P: Paginated<DB>> Paginated<DB> for PaginatedQueryWrapper<P, DB> {
    type QuerySource = P::QuerySource;
    type Q0<'q> = P::Q0<'q> where Self: 'q;
    type Q1<'q> = P::Q1<'q> where Self: 'q;
    type Q2<'q> = P::Q2<'q> where Self: 'q;
    type P = P::P;
    type PO = P::PO;
    type QueryId = P::QueryId;
    type InternalSqlType = P::InternalSqlType;

    type Partitioned<'q, Expr: Partition + Send + 'q> = PaginatedQueryWrapper<P::Partitioned<'q, Expr>, DB> where Self: 'q;
    type PartitionOrdered<'q, Expr: PartitionOrder<DB> + Send + 'q> = PaginatedQueryWrapper<P::PartitionOrdered<'q, Expr>, DB> where Self: 'q;

    fn partition<'q, Expr: Partition + Send + 'q>(self, expr: Expr) -> Self::Partitioned<'q, Expr>
    where
        Self: 'q,
    {
        PaginatedQueryWrapper(self.0.partition(expr), Default::default())
    }
    fn partition_order<'q, Expr: PartitionOrder<DB> + Send + 'q>(self, expr: Expr) -> Self::PartitionOrdered<'q, Expr>
    where
        Self: 'q,
    {
        PaginatedQueryWrapper(self.0.partition_order(expr), Default::default())
    }

    fn get_query(&self) -> &Self::Q0<'_> {
        self.0.get_query()
    }
    fn get_cursor_queries(&self) -> &[Self::Q2<'_>] {
        self.0.get_cursor_queries()
    }
    fn get_partition(&self) -> Option<&Self::P> {
        self.0.get_partition()
    }
    fn get_partition_order(&self) -> Option<&Self::PO> {
        self.0.get_partition_order()
    }
    fn get_pages(&self) -> Option<&[Page<Self::QuerySource>]> {
        self.0.get_pages()
    }

    fn map_query<'q, F0, F2, NewQ0, NewQ1, NewQ2>(
        self,
        f0: F0,
        f2: F2,
    ) -> impl Paginated<
        DB,
        Q0<'q> = NewQ0,
        Q1<'q> = NewQ1,
        Q2<'q> = NewQ2,
        P = Self::P,
        PO = Self::PO,
        QueryId = NewQ0::QueryId,
        SqlType = Self::SqlType,
        InternalSqlType = Self::InternalSqlType,
    > + 'q
    where
        Self: 'q,
        F0: FnOnce(Self::Q0<'q>) -> NewQ0,
        F2: FnMut(Self::Q2<'q>) -> NewQ2,
        NewQ0: Clone
            + Query<SqlType = Self::InternalSqlType>
            + QueryId
            + QueryFragment<DB>
            + Send
            + FilterDsl<Box<dyn ColumnCursorComparisonExpression<Self::QuerySource>>, Output = NewQ1>
            + 'q,
        NewQ1: Send + OrderDsl<Box<dyn ColumnOrderByExpression<Self::QuerySource>>, Output = NewQ2> + 'q,
        NewQ2: Query<SqlType = Self::InternalSqlType> + QueryFragment<DB> + QueryId + Send + 'q,
    {
        PaginatedQueryWrapper(self.0.map_query(f0, f2), Default::default())
    }
}

pub trait Partition {
    fn encode(&self) -> Result<String, diesel::result::Error>;
}

pub trait PartitionOrder<DB>
where
    DB: Backend,
{
    fn encode<'a, 'b>(ast_pass: AstPass<'a, 'b, DB>)
    where
        DB::QueryBuilder: 'a,
        <DB as Backend>::BindCollector<'a>: 'a,
        DB::MetadataLookup: 'a,
        'b: 'a;
}

impl Partition for () {
    fn encode(&self) -> Result<String, diesel::result::Error> {
        Ok("".into())
    }
}

impl<DB> PartitionOrder<DB> for ()
where
    DB: Backend,
{
    fn encode<'a, 'b>(mut ast_pass: AstPass<'a, 'b, DB>)
    where
        DB::QueryBuilder: 'a,
        <DB as Backend>::BindCollector<'a>: 'a,
        DB::MetadataLookup: 'a,
        'b: 'a,
    {
        ast_pass.push_sql(" 1 ");
    }
}

#[allow(unused_parens)]
impl<DB, T> PartitionOrder<DB> for Asc<T>
where
    DB: Backend,
    T: Column,
{
    fn encode<'a, 'b>(mut ast_pass: AstPass<'a, 'b, DB>)
    where
        DB::QueryBuilder: 'a,
        <DB as Backend>::BindCollector<'a>: 'a,
        DB::MetadataLookup: 'a,
        'b: 'a,
    {
        ast_pass.push_sql(" ");
        ast_pass.push_sql(OFFSET_SUBQUERY1_ALIAS);
        ast_pass.push_sql(".");
        ast_pass.push_sql(<T as Column>::NAME.split('.').last().unwrap());
        ast_pass.push_sql(" asc ");
    }
}

#[allow(unused_parens)]
impl<DB, T> PartitionOrder<DB> for Desc<T>
where
    DB: Backend,
    T: Column,
{
    fn encode<'a, 'b>(mut ast_pass: AstPass<'a, 'b, DB>)
    where
        DB::QueryBuilder: 'a,
        <DB as Backend>::BindCollector<'a>: 'a,
        DB::MetadataLookup: 'a,
        'b: 'a,
    {
        ast_pass.push_sql(" ");
        ast_pass.push_sql(OFFSET_SUBQUERY1_ALIAS);
        ast_pass.push_sql(".");
        ast_pass.push_sql(<T as Column>::NAME.split('.').last().unwrap());
        ast_pass.push_sql(" desc ");
    }
}

#[macro_export]
macro_rules! intersperse_statement {
    ($separator:stmt; $stmt:stmt; $($stmts:stmt;)+) => {
        $stmt
        $separator
        $crate::intersperse_statement!($separator; $($stmts;)*);
    };
    ($separator:stmt; $stmt:stmt;) => {
        $stmt
    };
}

macro_rules! partition {
    ($($len:literal: $($gen:ident)+),*$(,)?) => {
        $(
            #[allow(unused_parens)]
            impl<$($gen),+> Partition for ($($gen,)+)
            where
                $($gen: Column),+
            {
                fn encode(&self) -> Result<String, diesel::result::Error> {
                    let unique_min_column_names = [$(
                        format!("{OFFSET_SUBQUERY1_ALIAS}.{}", <$gen as Column>::NAME.split(".").last().unwrap()),
                    )+]
                        .into_iter()
                        .unique()
                        .collect::<Vec<_>>();
                    if unique_min_column_names.len() < $len {
                        return Err(diesel::result::Error::QueryBuilderError("could not encode group by clause as a row number partition for pagination because the column names included in the group by clause have identical names".into()));
                    }
                    Ok(unique_min_column_names.join(", "))
                }
            }

            #[allow(unused_parens)]
            impl<DB, $($gen),+> PartitionOrder<DB> for ($($gen,)+)
            where
                DB: Backend,
                $($gen: PartitionOrder<DB>,)+
            {
                fn encode<'a, 'b>(mut ast_pass: AstPass<'a, 'b, DB>)
                where
                    DB::QueryBuilder: 'a,
                    <DB as Backend>::BindCollector<'a>: 'a,
                    DB::MetadataLookup: 'a,
                    'b: 'a,
                {
                    $crate::intersperse_statement!(
                        ast_pass.push_sql(", ");
                        $(<$gen as PartitionOrder<DB>>::encode(ast_pass.reborrow());)+
                    );
                }
            }
        )*
    };
}

impl<QS, Q0, Q2, P, PO, DB> QueryFragment<DB> for PaginatedQuery<QS, Q0, Q2, P, PO>
where
    DB: Backend,
    Q0: QueryFragment<DB>,
    Q2: QueryFragment<DB>,
    P: Partition,
    PO: PartitionOrder<DB>,
    i64: ToSql<BigInt, DB>,
{
    fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        pass.unsafe_to_cache_prepared();

        let query = &self.query;

        let pages = match self.pages.as_ref() {
            Some(pages) => pages,
            None => return query.walk_ast(pass),
        };
        if pages.is_empty() {
            return Err(diesel::result::Error::QueryBuilderError(
                "no pages specified for a paginated query".into(),
            ));
        }

        let PageSplit {
            cursor_indices,
            offset_indices,
        } = Page::split(pages);

        let has_cursor_pages = !cursor_indices.is_empty();
        let has_offset_pages = !offset_indices.is_empty();

        if has_cursor_pages {
            let cursor_queries = &self.cursor_queries;
            let page_cursors: Vec<&PageCursor<QS>> = cursor_indices
                .into_iter()
                .map(|i| pages[i].as_cursor().unwrap())
                .collect_vec();

            pass.push_sql("with ");
            for (i, cursor_query) in cursor_queries.iter().enumerate() {
                pass.push_sql(&format!("query{i} as ("));
                cursor_query.walk_ast(pass.reborrow())?;
                pass.push_sql(")");
                if i < cursor_queries.len() - 1 {
                    pass.push_sql(", ");
                }
            }

            for item in intersperse(page_cursors.into_iter().enumerate().map(Left), Right(())) {
                match item {
                    Left((i, page_cursor)) => {
                        if let Some(partition) = self.partition.as_ref() {
                            pass.push_sql("(select *, row_number() over (partition by ");
                            pass.push_sql(&partition.encode()?);
                            if self.partition_order.is_some() {
                                pass.push_sql(" order by ");
                                PO::encode(pass.reborrow());
                            }
                            pass.push_sql(") as ");
                            pass.push_sql(OFFSET_COLUMN_NAME);
                        } else {
                            pass.push_sql("(select *, null as ");
                            pass.push_sql(OFFSET_COLUMN_NAME);
                        }

                        pass.push_sql(", ");
                        pass.push_sql(&format!("'{}' as ", page_cursor.id));
                        pass.push_sql(PAGE_ID_COLUMN_NAME);

                        static SUBQUERY_NAME: &str = "q";
                        pass.push_sql(&format!(" from query{i} as "));
                        pass.push_sql(SUBQUERY_NAME);

                        match self.partition.is_some() {
                            false => {
                                pass.push_sql(" limit ");
                                pass.push_bind_param::<BigInt, _>(&page_cursor.count)?;
                            }
                            true => {
                                pass.push_sql(" where ");
                                pass.push_sql(SUBQUERY_NAME);
                                pass.push_sql(".");
                                pass.push_sql(OFFSET_COLUMN_NAME);
                                pass.push_sql(" <= ");
                                pass.push_bind_param::<BigInt, _>(&page_cursor.count)?;
                            }
                        };

                        pass.push_sql(")");
                    }
                    Right(_) => pass.push_sql(" union "),
                }
            }
        }

        // union
        if has_cursor_pages && has_offset_pages {
            pass.push_sql(" union ");
        }

        if has_offset_pages {
            let page_offsets: Vec<&PageOffset> = offset_indices
                .into_iter()
                .map(|i| pages[i].as_offset().unwrap())
                .collect_vec();

            pass.push_sql("select *, null as ");
            pass.push_sql(PAGE_ID_COLUMN_NAME);
            pass.push_sql(" from (select *, row_number() over (");
            if let Some(partition) = self.partition.as_ref() {
                pass.push_sql("partition by ");
                pass.push_sql(&partition.encode()?);
                if self.partition_order.is_some() {
                    pass.push_sql(" order by ");
                    PO::encode(pass.reborrow());
                }
            }
            pass.push_sql(") as ");
            pass.push_sql(OFFSET_COLUMN_NAME);
            pass.push_sql(" from ( ");
            query.walk_ast(pass.reborrow())?;

            pass.push_sql(") ");
            pass.push_sql(OFFSET_SUBQUERY1_ALIAS);
            pass.push_sql(") ");
            pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
            pass.push_sql(" where ");
            for item in intersperse(page_offsets.iter().map(Left), Right(())) {
                match item {
                    Left(page_offset) => {
                        pass.push_sql("(");
                        pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
                        pass.push_sql(".");
                        pass.push_sql(OFFSET_COLUMN_NAME);
                        pass.push_sql(" > "); // row_number starts at 1
                        pass.push_bind_param::<BigInt, _>(&page_offset.left)?;
                        pass.push_sql(" and ");
                        pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
                        pass.push_sql(".");
                        pass.push_sql(OFFSET_COLUMN_NAME);
                        pass.push_sql(" <= ");
                        pass.push_bind_param::<BigInt, _>(&page_offset.right)?;
                        pass.push_sql(")");
                    }
                    Right(_) => pass.push_sql(" or "),
                }
            }
        }

        Ok(())
    }
}

impl<DB, P> QueryFragment<DB> for PaginatedQueryWrapper<P, DB>
where
    DB: Backend,
    P: Paginated<DB>,
    i64: ToSql<BigInt, DB>,
{
    fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        pass.unsafe_to_cache_prepared();

        let query = self.get_query();

        let pages = match self.get_pages() {
            Some(pages) => pages,
            None => return query.walk_ast(pass),
        };
        if pages.is_empty() {
            return Err(diesel::result::Error::QueryBuilderError(
                "no pages specified for a paginated query".into(),
            ));
        }

        let PageSplit {
            cursor_indices,
            offset_indices,
        } = Page::split(pages);

        let has_cursor_pages = !cursor_indices.is_empty();
        let has_offset_pages = !offset_indices.is_empty();

        if has_cursor_pages {
            let cursor_queries = self.get_cursor_queries();
            let page_cursors: Vec<&PageCursor<P::QuerySource>> = cursor_indices
                .into_iter()
                .map(|i| pages[i].as_cursor().unwrap())
                .collect_vec();

            pass.push_sql("with ");
            for (i, cursor_query) in cursor_queries.iter().enumerate() {
                pass.push_sql(&format!("query{i} as ("));
                cursor_query.walk_ast(pass.reborrow())?;
                pass.push_sql(")");
                if i < cursor_queries.len() - 1 {
                    pass.push_sql(", ");
                }
            }

            for item in intersperse(page_cursors.into_iter().enumerate().map(Left), Right(())) {
                match item {
                    Left((i, page_cursor)) => {
                        if let Some(partition) = self.get_partition() {
                            pass.push_sql("(select *, row_number() over (partition by ");
                            pass.push_sql(&partition.encode()?);
                            if self.get_partition_order().is_some() {
                                pass.push_sql(" order by ");
                                P::PO::encode(pass.reborrow());
                            }
                            pass.push_sql(") as ");
                            pass.push_sql(OFFSET_COLUMN_NAME);
                        } else {
                            pass.push_sql("(select *, null as ");
                            pass.push_sql(OFFSET_COLUMN_NAME);
                        }

                        pass.push_sql(", ");
                        pass.push_sql(&format!("'{}' as ", page_cursor.id));
                        pass.push_sql(PAGE_ID_COLUMN_NAME);

                        static SUBQUERY_NAME: &str = "q";
                        pass.push_sql(&format!(" from query{i} as "));
                        pass.push_sql(SUBQUERY_NAME);

                        match self.get_partition().is_some() {
                            false => {
                                pass.push_sql(" limit ");
                                pass.push_bind_param::<BigInt, _>(&page_cursor.count)?;
                            }
                            true => {
                                pass.push_sql(" where ");
                                pass.push_sql(SUBQUERY_NAME);
                                pass.push_sql(".");
                                pass.push_sql(OFFSET_COLUMN_NAME);
                                pass.push_sql(" <= ");
                                pass.push_bind_param::<BigInt, _>(&page_cursor.count)?;
                            }
                        };

                        pass.push_sql(")");
                    }
                    Right(_) => pass.push_sql(" union "),
                }
            }
        }

        // union
        if has_cursor_pages && has_offset_pages {
            pass.push_sql(" union ");
        }

        if has_offset_pages {
            let page_offsets: Vec<&PageOffset> = offset_indices
                .into_iter()
                .map(|i| pages[i].as_offset().unwrap())
                .collect_vec();

            pass.push_sql("select *, null as ");
            pass.push_sql(PAGE_ID_COLUMN_NAME);
            pass.push_sql(" from (select *, row_number() over (");
            if let Some(partition) = self.get_partition() {
                pass.push_sql("partition by ");
                pass.push_sql(&partition.encode()?);
                if self.get_partition_order().is_some() {
                    pass.push_sql(" order by ");
                    P::PO::encode(pass.reborrow());
                }
            }
            pass.push_sql(") as ");
            pass.push_sql(OFFSET_COLUMN_NAME);
            pass.push_sql(" from ( ");
            query.walk_ast(pass.reborrow())?;

            pass.push_sql(") ");
            pass.push_sql(OFFSET_SUBQUERY1_ALIAS);
            pass.push_sql(") ");
            pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
            pass.push_sql(" where ");
            for item in intersperse(page_offsets.iter().map(Left), Right(())) {
                match item {
                    Left(page_offset) => {
                        pass.push_sql("(");
                        pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
                        pass.push_sql(".");
                        pass.push_sql(OFFSET_COLUMN_NAME);
                        pass.push_sql(" > "); // row_number starts at 1
                        pass.push_bind_param::<BigInt, _>(&page_offset.left)?;
                        pass.push_sql(" and ");
                        pass.push_sql(OFFSET_SUBQUERY2_ALIAS);
                        pass.push_sql(".");
                        pass.push_sql(OFFSET_COLUMN_NAME);
                        pass.push_sql(" <= ");
                        pass.push_bind_param::<BigInt, _>(&page_offset.right)?;
                        pass.push_sql(")");
                    }
                    Right(_) => pass.push_sql(" or "),
                }
            }
        }

        Ok(())
    }
}

partition!(
     1: A,
     2: A B,
     3: A B C,
     4: A B C D,
     5: A B C D E,
     6: A B C D E F,
     7: A B C D E F G,
     8: A B C D E F G H,
     9: A B C D E F G H I,
    10: A B C D E F G H I J,
    11: A B C D E F G H I J K,
    12: A B C D E F G H I J K L,
    13: A B C D E F G H I J K L M,
    14: A B C D E F G H I J K L M N,
    15: A B C D E F G H I J K L M N O,
    16: A B C D E F G H I J K L M N O P,
    17: A B C D E F G H I J K L M N O P Q,
    18: A B C D E F G H I J K L M N O P Q R,
    19: A B C D E F G H I J K L M N O P Q R S,
    20: A B C D E F G H I J K L M N O P Q R S T,
    21: A B C D E F G H I J K L M N O P Q R S T U,
    22: A B C D E F G H I J K L M N O P Q R S T U V,
    23: A B C D E F G H I J K L M N O P Q R S T U V W,
    24: A B C D E F G H I J K L M N O P Q R S T U V W X,
    25: A B C D E F G H I J K L M N O P Q R S T U V W X Y,
    26: A B C D E F G H I J K L M N O P Q R S T U V W X Y Z,
);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Paged<K, QS> {
    pub page: Page<QS>,
    pub key: K,
}

impl<K, QS> AsRef<Page<QS>> for Paged<K, QS> {
    fn as_ref(&self) -> &Page<QS> {
        &self.page
    }
}

mod paginated_query_impls {
    use super::*;
    use diesel::dsl::*;
    use diesel::query_dsl::methods::*;
    use diesel::sql_types::{Nullable, Text};

    // Query impl also produces auto-impl of AsQuery
    impl<QS, Q0: Query, Q2, P, PO> Query for PaginatedQuery<QS, Q0, Q2, P, PO> {
        type SqlType = (Q0::SqlType, Nullable<BigInt>, Nullable<Text>);
    }

    impl<QS, Q0: QueryId, Q2, P, PO> QueryId for PaginatedQuery<QS, Q0, Q2, P, PO> {
        type QueryId = Q0::QueryId;

        const HAS_STATIC_QUERY_ID: bool = false;
    }

    impl<C: Connection, QS, Q0, Q2, P, PO> RunQueryDsl<C> for PaginatedQuery<QS, Q0, Q2, P, PO> {}

    impl<P, PO, QS, Q0: QueryDsl, Q2: QueryDsl> QueryDsl for PaginatedQuery<QS, Q0, Q2, P, PO> {}

    impl<'a, P, PO, QS: ?Sized, Q0: BoxedDsl<'a, DB>, Q2: BoxedDsl<'a, DB>, DB> BoxedDsl<'a, DB>
        for PaginatedQuery<QS, Q0, Q2, P, PO>
    {
        type Output = PaginatedQuery<QS, IntoBoxed<'a, Q0, DB>, IntoBoxed<'a, Q2, DB>, P, PO>;
        fn internal_into_boxed(self) -> IntoBoxed<'a, Self, DB> {
            PaginatedQuery {
                query: self.query.internal_into_boxed(),
                cursor_queries: self
                    .cursor_queries
                    .into_iter()
                    .map(|q| q.internal_into_boxed())
                    .collect_vec(),
                pages: self.pages,
                partition: self.partition,
                partition_order: self.partition_order,
            }
        }
    }

    macro_rules! paginated_query_query_dsl_method {
        ($trait:ident$(<$($gen:ident $(: $bound:tt $(+ $bound2:tt)?)?),+>)? { fn $fn_name:ident(self $(, $param:ident: $param_ty:ty)*) -> $output_ty:ident<Self $(, $output_gen:ident)*>; }) => {
            impl<QS: ?Sized, P, PO, Q0: $trait$(<$($gen),+>)?, Q2: $trait$(<$($gen),+>)? $($(, $gen $(: $bound $(+ $bound2)?)?)+)?> $trait$(<$($gen),+>)? for PaginatedQuery<QS, Q0, Q2, P, PO> {
                type Output = PaginatedQuery<QS, $output_ty<Q0 $(, $output_gen)*>, $output_ty<Q2 $(, $output_gen)*>, P, PO>;
                fn $fn_name(self $(, $param: $param_ty)*) -> $output_ty<Self $(, $output_gen)*> {
                    PaginatedQuery {
                        cursor_queries: self.cursor_queries.into_iter().map(|q| q.$fn_name($($param.clone()),*)).collect_vec(),
                        query: self.query.$fn_name($($param),*),
                        pages: self.pages,
                        partition: self.partition,
                        partition_order: self.partition_order,
                    }
                }
            }
        };
        ($trait:ident$(<$($gen:ident $(: $bound:tt $(+ $bound2:tt)?)?),+>)? { fn $fn_name:ident(self $(, $param:ident: $param_ty:ty)*) -> Self::Output; }) => {
            impl<QS: ?Sized, P, PO, Q0: $trait$(<$($gen),+>)?, Q2: $trait$(<$($gen),+>)? $($(, $gen $(: $bound $(+ $bound2)?)?)+)?> $trait$(<$($gen),+>)? for PaginatedQuery<QS, Q0, Q2, P, PO> {
                type Output = PaginatedQuery<QS, <Q0 as $trait$(<$($gen),+>)?>::Output, <Q2 as $trait$(<$($gen),+>)?>::Output, P, PO>;
                fn $fn_name(self $(, $param: $param_ty)*) -> Self::Output {
                    PaginatedQuery {
                        cursor_queries: self.cursor_queries.into_iter().map(|q| q.$fn_name($($param.clone()),*)).collect_vec(),
                        query: self.query.$fn_name($($param),*),
                        pages: self.pages,
                        partition: self.partition,
                        partition_order: self.partition_order,
                    }
                }
            }
        };
    }

    paginated_query_query_dsl_method!(DistinctDsl { fn distinct(self) -> Distinct<Self>; });
    #[cfg(feature = "postgres")]
    paginated_query_query_dsl_method!(DistinctOnDsl<Selection: Clone> { fn distinct_on(self, selection: Selection) -> DistinctOn<Self, Selection>; });
    paginated_query_query_dsl_method!(SelectDsl<Selection: Clone + Expression> { fn select(self, selection: Selection) -> Self::Output; });
    paginated_query_query_dsl_method!(FilterDsl<Predicate: Clone> { fn filter(self, predicate: Predicate) -> Self::Output; });
    paginated_query_query_dsl_method!(OrFilterDsl<Predicate: Clone> { fn or_filter(self, predicate: Predicate) -> Self::Output; });
    paginated_query_query_dsl_method!(FindDsl<PK: Clone> { fn find(self, id: PK) -> Self::Output; });
    paginated_query_query_dsl_method!(GroupByDsl<Expr: Clone + Expression> { fn group_by(self, expr: Expr) -> GroupBy<Self, Expr>; });
    paginated_query_query_dsl_method!(HavingDsl<Predicate: Clone> { fn having(self, predicate: Predicate) -> Having<Self, Predicate>; });
    paginated_query_query_dsl_method!(LockingDsl<Lock: Clone> { fn with_lock(self, lock: Lock) -> Self::Output; });
    paginated_query_query_dsl_method!(ModifyLockDsl<Modifier: Clone> { fn modify_lock(self, modifier: Modifier) -> Self::Output; });
    paginated_query_query_dsl_method!(SingleValueDsl { fn single_value(self) -> Self::Output; });
    paginated_query_query_dsl_method!(SelectNullableDsl { fn nullable(self) -> Self::Output; });
}

mod paginated_query_wrapper_impls {
    use super::*;

    // Query impl also produces auto-impl of AsQuery
    impl<DB: Backend, P: Paginated<DB>> Query for PaginatedQueryWrapper<P, DB> {
        type SqlType = P::SqlType;
    }

    impl<DB: Backend, P: Paginated<DB>> QueryId for PaginatedQueryWrapper<P, DB> {
        type QueryId = P::QueryId;

        const HAS_STATIC_QUERY_ID: bool = false;
    }

    impl<C: Connection, P: Paginated<C::Backend>> RunQueryDsl<C> for PaginatedQueryWrapper<P, C::Backend> {}

    impl<DB: Backend, P: Paginated<DB>> QueryDsl for PaginatedQueryWrapper<P, DB> {}
}
