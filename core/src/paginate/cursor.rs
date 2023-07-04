use crate::paginate::PageExt;
use chrono::NaiveDateTime;
use diesel::expression::expression_types::NotSelectable;
use diesel::expression::is_aggregate::No;
use diesel::expression::ValidGrouping;
use diesel::sql_types::{Bool, SingleValue, Timestamp};
use diesel::{AppearsOnTable, Column, Expression, ExpressionMethods};
use dyn_clone::DynClone;
use std::borrow::Borrow;
use std::cmp::Ordering;
use uuid::Uuid;

#[cfg(feature = "async-graphql-4")]
use async_graphql_4 as async_graphql;
#[cfg(feature = "async-graphql-5")]
use async_graphql_5 as async_graphql;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[cfg_attr(
    any(feature = "async-graphql-4", feature = "async-graphql-5"),
    derive(async_graphql::InputObject)
)]
pub struct ApiPageCursor {
    #[cfg_attr(
        any(feature = "async-graphql-4", feature = "async-graphql-5"),
        graphql(validator(custom = "crate::paginate::GraphqlPaginationCountValidator"))
    )]
    pub count: u32,
    pub cursor: NaiveDateTime,
    pub direction: CursorDirection,
    /// defaults to false
    pub is_comparator_inclusive: Option<bool>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[cfg_attr(
    any(feature = "async-graphql-4", feature = "async-graphql-5"),
    derive(async_graphql::Enum)
)]
pub enum CursorDirection {
    Following,
    Preceding,
}

#[derive(Derivative)]
#[derivative(Debug(bound = ""), Eq(bound = ""), Hash(bound = ""), PartialEq(bound = ""))]
pub struct PageCursor<Table: ?Sized> {
    pub count: i64,
    pub cursor: NaiveDateTime,
    #[derivative(Hash = "ignore")]
    #[derivative(PartialEq = "ignore")]
    pub(crate) id: Uuid,
    pub(crate) column_name: &'static str,
    pub(crate) direction: CursorDirection,
    pub(crate) is_comparator_inclusive: bool,
    #[derivative(Debug = "ignore")]
    #[derivative(Hash = "ignore")]
    #[derivative(PartialEq = "ignore")]
    pub(crate) column_cursor_comparison_expression: Box<dyn ColumnCursorComparisonExpression<Table>>,
    #[derivative(Debug = "ignore")]
    #[derivative(Hash = "ignore")]
    #[derivative(PartialEq = "ignore")]
    pub(crate) column_order_by_expression: Box<dyn ColumnOrderByExpression<Table>>,
}

impl<Table: ?Sized> Clone for PageCursor<Table> {
    fn clone(&self) -> Self {
        Self {
            count: self.count,
            cursor: self.cursor,
            id: self.id,
            column_name: self.column_name,
            direction: self.direction,
            is_comparator_inclusive: self.is_comparator_inclusive,
            column_cursor_comparison_expression: dyn_clone::clone_box(&*self.column_cursor_comparison_expression),
            column_order_by_expression: dyn_clone::clone_box(&*self.column_order_by_expression),
        }
    }
}

pub trait ColumnCursorComparisonExpression<Table: ?Sized>:
    AppearsOnTable<Table>
    + DynClone
    + Expression<SqlType = Bool>
    + QF // see bottom of file for QF definition
    + Send
    + Sync
    + ValidGrouping<(), IsAggregate = No>
    + 'static
{
}

pub trait ColumnOrderByExpression<Table: ?Sized>:
    AppearsOnTable<Table> + DynClone + Expression<SqlType = NotSelectable> + QF + Send + Sync + 'static
{
}

impl<
        Table: diesel::Table + ?Sized,
        CE: AppearsOnTable<Table>
            + DynClone
            + Expression<SqlType = Bool>
            + QF
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
    > ColumnCursorComparisonExpression<Table> for CE
{
}

impl<
        Table: diesel::Table + ?Sized,
        CE: AppearsOnTable<Table> + DynClone + Expression<SqlType = NotSelectable> + QF + Send + Sync + 'static,
    > ColumnOrderByExpression<Table> for CE
{
}

impl ApiPageCursor {
    pub fn on_column<C>(self, column: C) -> Result<PageCursor<C::Table>, anyhow::Error>
    where
        C: AppearsOnTable<C::Table>
            + Clone
            + Column<SqlType = Timestamp>
            + QF
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
        <C as Expression>::SqlType: SingleValue,
    {
        let is_comparator_inclusive = self.is_comparator_inclusive.unwrap_or_default();
        Ok(PageCursor {
            count: self.count as i64,
            id: Uuid::new_v4(),
            column_name: C::NAME,
            column_cursor_comparison_expression: match (is_comparator_inclusive, self.direction) {
                (false, CursorDirection::Following) => Box::new(column.clone().gt(self.cursor)),
                (false, CursorDirection::Preceding) => Box::new(column.clone().lt(self.cursor)),
                (true, CursorDirection::Following) => Box::new(column.clone().ge(self.cursor)),
                (true, CursorDirection::Preceding) => Box::new(column.clone().le(self.cursor)),
            },
            column_order_by_expression: match self.direction {
                CursorDirection::Following => Box::new(column.asc()),
                CursorDirection::Preceding => Box::new(column.desc()),
            },
            direction: self.direction,
            cursor: self.cursor,
            is_comparator_inclusive,
        })
    }
}

impl<C> TryFrom<(ApiPageCursor, C)> for PageCursor<C::Table>
where
    C: AppearsOnTable<C::Table>
        + Clone
        + Column<SqlType = Timestamp>
        + QF
        + Send
        + Sync
        + ValidGrouping<(), IsAggregate = No>
        + 'static,
    <C as Expression>::SqlType: SingleValue,
{
    type Error = anyhow::Error;
    fn try_from((value, column): (ApiPageCursor, C)) -> Result<Self, Self::Error> {
        ApiPageCursor::on_column(value, column)
    }
}

impl<Table: ?Sized> PageCursor<Table> {
    pub fn column_name(&self) -> &str {
        self.column_name
    }
}

impl<Table: ?Sized> PartialOrd for PageCursor<Table> {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        if self.cursor != rhs.cursor {
            self.cursor.partial_cmp(&rhs.cursor)
        } else {
            self.count.partial_cmp(&rhs.count)
        }
    }
}

impl<Table: ?Sized> Ord for PageCursor<Table> {
    fn cmp(&self, rhs: &Self) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

impl<Table: ?Sized> PageExt for PageCursor<Table> {
    fn is_empty(&self) -> bool {
        self.count == 0
    }
    fn merge(page_cursors: impl IntoIterator<Item = impl Borrow<Self>>) -> Vec<Self> {
        let mut page_cursors = page_cursors
            .into_iter()
            .map(|page_cursor| page_cursor.borrow().clone())
            .collect::<Vec<Self>>();

        page_cursors.sort();

        // no actual merging can occur for cursor based pagination because we
        // cannot know the actual density of records between cursors in advance
        page_cursors
    }
}

cfg_if! {
    if #[cfg(all(not(feature = "mysql"), not(feature = "postgres"), not(feature = "sqlite")))] {
        pub trait QF {}
        impl<T> QF for T {}
    }
}
cfg_if! {
    if #[cfg(all(feature = "mysql", not(feature = "postgres"), not(feature = "sqlite")))] {
        pub trait QF: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> {}
        impl<T: diesel::query_builder::QueryFragment<diesel::mysql::Mysql>> QF for T {}
    }
}
cfg_if! {
    if #[cfg(all(not(feature = "mysql"), feature = "postgres", not(feature = "sqlite")))] {
        pub trait QF: diesel::query_builder::QueryFragment<diesel::pg::Pg> {}
        impl<T: diesel::query_builder::QueryFragment<diesel::pg::Pg>> QF for T {}
    }
}
cfg_if! {
    if #[cfg(all(not(feature = "mysql"), not(feature = "postgres"), feature = "sqlite"))] {
        pub trait QF: diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite> {}
        impl<T: diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite>> QF for T {}
    }
}
cfg_if! {
    if #[cfg(all(feature = "mysql", feature = "postgres", not(feature = "sqlite")))] {
        pub trait QF: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::pg::Pg> {}
        impl<T: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::pg::Pg>> QF for T {}
    }
}
cfg_if! {
    if #[cfg(all(not(feature = "mysql"), feature = "postgres", feature = "sqlite"))] {
        pub trait QF: diesel::query_builder::QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite> {}
        impl<T: diesel::query_builder::QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite>> QF for T {}
    }
}
cfg_if! {
    if #[cfg(all(feature = "mysql", not(feature = "postgres"), feature = "sqlite"))] {
        pub trait QF: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite> {}
        impl<T: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite>> QF for T {}
    }
}
cfg_if! {
    if #[cfg(all(feature = "mysql", feature = "postgres", feature = "sqlite"))] {
        pub trait QF: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite> {}
        impl<T: diesel::query_builder::QueryFragment<diesel::mysql::Mysql> + diesel::query_builder::QueryFragment<diesel::pg::Pg> + diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite>> QF for T {}
    }
}
