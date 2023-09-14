use crate::paginate::DbPageExt;
use ::diesel::dsl::{self, And, IsNotNull, Or};
use ::diesel::expression::{is_aggregate::No, AsExpression, ValidGrouping};
use ::diesel::expression_methods::NullableExpressionMethods;
use ::diesel::helper_types::{Gt, GtEq, Lt, LtEq};
use ::diesel::sql_types::is_nullable::{IsNullable, NotNull};
use ::diesel::sql_types::BoolOrNullableBool;
use ::diesel::sql_types::MaybeNullableType;
use ::diesel::sql_types::OneIsNullable;
use ::diesel::sql_types::{Bool, Nullable, SingleValue, SqlType};
use ::diesel::{AppearsOnTable, BoolExpressionMethods, Column, Expression, ExpressionMethods, QuerySource};
use chrono::NaiveDateTime;
use diesel::expression::expression_types::NotSelectable;
use dyn_clone::DynClone;
use std::borrow::Borrow;
use std::cmp::Ordering;
use uuid::Uuid;

#[cfg(feature = "async-graphql-4")]
use async_graphql_4 as async_graphql;
#[cfg(feature = "async-graphql-5")]
use async_graphql_5 as async_graphql;
#[cfg(feature = "async-graphql-6")]
use async_graphql_6 as async_graphql;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[cfg_attr(
    any(
        feature = "async-graphql-4",
        feature = "async-graphql-5",
        feature = "async-graphql-6"
    ),
    derive(async_graphql::InputObject)
)]
pub struct PageCursor {
    #[cfg_attr(
        any(
            feature = "async-graphql-4",
            feature = "async-graphql-5",
            feature = "async-graphql-6"
        ),
        graphql(validator(custom = "crate::paginate::GraphqlPaginationCountValidator"))
    )]
    pub count: u32,
    pub cursor: NaiveDateTime,
    pub direction: CursorDirection,
    /// defaults to false
    pub is_comparator_inclusive: Option<bool>,
}

impl Ord for PageCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd for PageCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.cursor != other.cursor {
            self.cursor.partial_cmp(&other.cursor)
        } else if self.count != other.count {
            self.count.partial_cmp(&other.count)
        } else if self.direction != other.direction {
            self.direction.partial_cmp(&other.direction)
        } else if self.is_comparator_inclusive != other.is_comparator_inclusive {
            if other.is_comparator_inclusive.unwrap_or_default() {
                Some(Ordering::Less)
            } else {
                Some(Ordering::Greater)
            }
        } else {
            Some(Ordering::Equal)
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[cfg_attr(
    any(
        feature = "async-graphql-4",
        feature = "async-graphql-5",
        feature = "async-graphql-6"
    ),
    derive(async_graphql::Enum)
)]
pub enum CursorDirection {
    Following,
    Preceding,
}

#[derive(Derivative)]
#[derivative(Debug(bound = ""), Eq(bound = ""), Hash(bound = ""), PartialEq(bound = ""))]
pub struct DbPageCursor<QS: ?Sized> {
    pub count: i64,
    pub cursor: NaiveDateTime,
    pub column: DbPageCursorColumn<QS>,
    #[derivative(Hash = "ignore")]
    #[derivative(PartialEq = "ignore")]
    pub(crate) id: Uuid,
    pub(crate) direction: CursorDirection,
    pub(crate) is_comparator_inclusive: bool,
}

#[derive(Derivative)]
#[derivative(Debug(bound = ""), Eq(bound = ""), Hash(bound = ""), PartialEq(bound = ""))]
pub struct DbPageCursorColumn<QS: ?Sized> {
    pub name: &'static str,
    #[derivative(Debug = "ignore")]
    #[derivative(Hash = "ignore")]
    #[derivative(PartialEq = "ignore")]
    pub(crate) cursor_comparison_expression: Box<dyn ColumnCursorComparisonExpression<QS>>,
    #[derivative(Debug = "ignore")]
    #[derivative(Hash = "ignore")]
    #[derivative(PartialEq = "ignore")]
    pub(crate) order_by_expression: Box<dyn ColumnOrderByExpression<QS>>,
}

pub trait DbPageFrom<T> {
    fn page_from(value: T) -> Self;
}

impl<QS: ?Sized> Clone for DbPageCursor<QS> {
    fn clone(&self) -> Self {
        Self {
            count: self.count,
            cursor: self.cursor,
            id: self.id,
            direction: self.direction,
            is_comparator_inclusive: self.is_comparator_inclusive,
            column: DbPageCursorColumn {
                name: self.column.name,
                cursor_comparison_expression: dyn_clone::clone_box(&*self.column.cursor_comparison_expression),
                order_by_expression: dyn_clone::clone_box(&*self.column.order_by_expression),
            },
        }
    }
}

pub trait ColumnCursorComparisonExpression<QS: ?Sized>:
    AppearsOnTable<QS>
    + DynClone
    + Expression<SqlType = Nullable<Bool>>
    + QF // see bottom of file for QF definition
    + Send
    + Sync
    + ValidGrouping<(), IsAggregate = No>
    + 'static
{
}

pub trait ColumnOrderByExpression<QS: ?Sized>:
    AppearsOnTable<QS> + DynClone + Expression<SqlType = (NotSelectable, NotSelectable)> + QF + Send + Sync + 'static
{
}

impl<
        QS: QuerySource + ?Sized,
        CE: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
    > ColumnCursorComparisonExpression<QS> for CE
{
}

impl<
        QS: QuerySource + ?Sized,
        CE: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = (NotSelectable, NotSelectable)>
            + QF
            + Send
            + Sync
            + 'static,
    > ColumnOrderByExpression<QS> for CE
{
}

impl PageCursor {
    pub fn on_column<QS, C>(self, column: C) -> DbPageCursor<QS>
    where
        QS: QuerySource,
        C: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync,
        <C as Expression>::SqlType: SingleValue,
        NaiveDateTime: AsExpression<C::SqlType>,

        Gt<C, NaiveDateTime>: Expression,
        Lt<C, NaiveDateTime>: Expression,
        GtEq<C, NaiveDateTime>: Expression,
        LtEq<C, NaiveDateTime>: Expression,

        dsl::Nullable<Gt<C, NaiveDateTime>>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
        dsl::Nullable<GtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
        dsl::Nullable<Lt<C, NaiveDateTime>>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
        dsl::Nullable<LtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
    {
        let is_comparator_inclusive = self.is_comparator_inclusive.unwrap_or_default();

        DbPageCursor {
            count: self.count as i64,
            id: Uuid::new_v4(),
            direction: self.direction,
            cursor: self.cursor,
            is_comparator_inclusive,
            column: DbPageCursorColumn {
                name: C::NAME,
                cursor_comparison_expression: match (is_comparator_inclusive, self.direction) {
                    (false, CursorDirection::Following) => Box::new(column.clone().gt(self.cursor).nullable()),
                    (false, CursorDirection::Preceding) => Box::new(column.clone().lt(self.cursor).nullable()),
                    (true, CursorDirection::Following) => Box::new(column.clone().ge(self.cursor).nullable()),
                    (true, CursorDirection::Preceding) => Box::new(column.clone().le(self.cursor).nullable()),
                },
                order_by_expression: match self.direction {
                    CursorDirection::Following => Box::new((column.clone().asc(), column.asc())),
                    CursorDirection::Preceding => Box::new((column.clone().desc(), column.desc())),
                },
            },
        }
    }

    pub fn on_columns<QS, C1, C2, N1, N2>(self, column1: C1, column2: C2) -> DbPageCursor<QS>
    where
        QS: QuerySource,
        C1: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync,
        C2: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync + 'static,
        <C1 as Expression>::SqlType: SingleValue + SqlType<IsNull = N1>,
        <C2 as Expression>::SqlType: SingleValue + SqlType<IsNull = N2>,
        NaiveDateTime: AsExpression<C1::SqlType> + AsExpression<C2::SqlType>,
        <NaiveDateTime as AsExpression<C2::SqlType>>::Expression: QF,

        N1: OneIsNullable<N1>,
        <N1 as OneIsNullable<N1>>::Out: MaybeNullableType<Bool>,
        N2: OneIsNullable<N2>,
        <N2 as OneIsNullable<N2>>::Out: MaybeNullableType<Bool>,

        dsl::Nullable<Gt<C1, NaiveDateTime>>: Expression,
        dsl::Nullable<Lt<C1, NaiveDateTime>>: Expression,
        dsl::Nullable<GtEq<C1, NaiveDateTime>>: Expression,
        dsl::Nullable<LtEq<C1, NaiveDateTime>>: Expression,

        IsNotNull<C1>: Expression + BoolExpressionMethods,
        <IsNotNull<C1> as Expression>::SqlType: SqlType,
        <<<IsNotNull<C1> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
        <<<IsNotNull<C1> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,

        dsl::Nullable<Gt<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
        <dsl::Nullable<Gt<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
        <<<dsl::Nullable<Gt<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
        And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
        <And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
            SqlType + BoolOrNullableBool,
        <<And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
            OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
        <<<And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
        <<<And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
        <<And<
            IsNotNull<C1>,
            dsl::Nullable<Gt<C1, NaiveDateTime>>,
            Nullable<Bool>,
        > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
        <<<And<
            IsNotNull<C1>,
            dsl::Nullable<Gt<C1, NaiveDateTime>>,
            Nullable<Bool>,
        > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
        dsl::Nullable<Or<
            And<IsNotNull<C1>, dsl::Nullable<Gt<C1, NaiveDateTime>>, Nullable<Bool>>,
            Gt<C2, NaiveDateTime>,
            <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
        >>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
    + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,

        dsl::Nullable<GtEq<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
        <dsl::Nullable<GtEq<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
        <<<dsl::Nullable<GtEq<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
        And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
        <And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
            SqlType + BoolOrNullableBool,
        <<And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
            OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
        <<<And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
        <<<And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
        <<And<
            IsNotNull<C1>,
            dsl::Nullable<GtEq<C1, NaiveDateTime>>,
            Nullable<Bool>,
        > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
        <<<And<
            IsNotNull<C1>,
            dsl::Nullable<GtEq<C1, NaiveDateTime>>,
            Nullable<Bool>,
        > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
        dsl::Nullable<Or<
            And<IsNotNull<C1>, dsl::Nullable<GtEq<C1, NaiveDateTime>>, Nullable<Bool>>,
            GtEq<C2, NaiveDateTime>,
            <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
        >>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
    + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,

        dsl::Nullable<Lt<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
        <dsl::Nullable<Lt<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
        <<<dsl::Nullable<Lt<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
        And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
        <And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
            SqlType + BoolOrNullableBool,
        <<And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
            OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
        <<<And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
        <<<And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
        <<And<
            IsNotNull<C1>,
            dsl::Nullable<Lt<C1, NaiveDateTime>>,
            Nullable<Bool>,
        > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
        <<<And<
            IsNotNull<C1>,
            dsl::Nullable<Lt<C1, NaiveDateTime>>,
            Nullable<Bool>,
        > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
        dsl::Nullable<Or<
            And<IsNotNull<C1>, dsl::Nullable<Lt<C1, NaiveDateTime>>, Nullable<Bool>>,
            Lt<C2, NaiveDateTime>,
            <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
        >>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
    + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,

        dsl::Nullable<LtEq<C1, NaiveDateTime>>: AsExpression<Nullable<Bool>>,
        <dsl::Nullable<LtEq<C1, NaiveDateTime>> as Expression>::SqlType: SqlType,
        <<<dsl::Nullable<LtEq<C1, NaiveDateTime>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
        And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>>: Expression,
        <And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType:
            SqlType + BoolOrNullableBool,
        <<And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull:
            OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
        <<<And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<IsNullable>>::Out: MaybeNullableType<Bool>,
        <<<And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>> as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<NotNull>>::Out: MaybeNullableType<Bool>,
        <<And<
            IsNotNull<C1>,
            dsl::Nullable<LtEq<C1, NaiveDateTime>>,
            Nullable<Bool>,
        > as Expression>::SqlType as SqlType>::IsNull: OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>,
        <<<And<
            IsNotNull<C1>,
            dsl::Nullable<LtEq<C1, NaiveDateTime>>,
            Nullable<Bool>,
        > as Expression>::SqlType as SqlType>::IsNull as OneIsNullable<<<<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out as SqlType>::IsNull>>::Out: MaybeNullableType<Bool>,
        dsl::Nullable<Or<
            And<IsNotNull<C1>, dsl::Nullable<LtEq<C1, NaiveDateTime>>, Nullable<Bool>>,
            LtEq<C2, NaiveDateTime>,
            <<N2 as OneIsNullable<N2>>::Out as MaybeNullableType<Bool>>::Out,
        >>: AppearsOnTable<QS>
            + DynClone
            + Expression<SqlType = Nullable<Bool>>
    + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ValidGrouping<(), IsAggregate = No>
            + 'static,
    {
        let is_comparator_inclusive = self.is_comparator_inclusive.unwrap_or_default();
        DbPageCursor {
            count: self.count as i64,
            id: Uuid::new_v4(),
            direction: self.direction,
            cursor: self.cursor,
            is_comparator_inclusive,
            column: DbPageCursorColumn {
                name: C1::NAME,
                cursor_comparison_expression: match (is_comparator_inclusive, self.direction) {
                    (false, CursorDirection::Following) => Box::new(
                        column1
                            .clone()
                            .is_not_null()
                            .and(column1.clone().gt(self.cursor).nullable())
                            .or(column2.clone().gt(self.cursor))
                            .nullable(),
                    ),
                    (false, CursorDirection::Preceding) => Box::new(
                        column1
                            .clone()
                            .is_not_null()
                            .and(column1.clone().lt(self.cursor).nullable())
                            .or(column2.clone().lt(self.cursor))
                            .nullable(),
                    ),
                    (true, CursorDirection::Following) => Box::new(
                        column1
                            .clone()
                            .is_not_null()
                            .and(column1.clone().ge(self.cursor).nullable())
                            .or(column2.clone().ge(self.cursor))
                            .nullable(),
                    ),
                    (true, CursorDirection::Preceding) => Box::new(
                        column1
                            .clone()
                            .is_not_null()
                            .and(column1.clone().le(self.cursor).nullable())
                            .or(column2.clone().le(self.cursor))
                            .nullable(),
                    ),
                },
                order_by_expression: match self.direction {
                    CursorDirection::Following => Box::new((column1.asc(), column2.asc())),
                    CursorDirection::Preceding => Box::new((column1.desc(), column2.desc())),
                },
            },
        }
    }
}

impl<QS, C> From<(PageCursor, C)> for DbPageCursor<QS>
where
    QS: QuerySource,
    C: AppearsOnTable<QS> + Clone + Column + QF + Send + Sync + ValidGrouping<(), IsAggregate = No> + 'static,
    <C as Expression>::SqlType: SingleValue,
    NaiveDateTime: AsExpression<C::SqlType>,

    Gt<C, NaiveDateTime>: Expression,
    Lt<C, NaiveDateTime>: Expression,
    GtEq<C, NaiveDateTime>: Expression,
    LtEq<C, NaiveDateTime>: Expression,

    dsl::Nullable<Gt<C, NaiveDateTime>>: AppearsOnTable<QS>
        + DynClone
        + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
        + Send
        + Sync
        + ValidGrouping<(), IsAggregate = No>
        + 'static,
    dsl::Nullable<GtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
        + DynClone
        + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
        + Send
        + Sync
        + ValidGrouping<(), IsAggregate = No>
        + 'static,
    dsl::Nullable<Lt<C, NaiveDateTime>>: AppearsOnTable<QS>
        + DynClone
        + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
        + Send
        + Sync
        + ValidGrouping<(), IsAggregate = No>
        + 'static,
    dsl::Nullable<LtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
        + DynClone
        + Expression<SqlType = Nullable<Bool>>
        + QF // see bottom of file for QF definition
        + Send
        + Sync
        + ValidGrouping<(), IsAggregate = No>
        + 'static,
{
    fn from((value, column): (PageCursor, C)) -> Self {
        PageCursor::on_column(value, column)
    }
}

impl<QS: ?Sized> PartialOrd for DbPageCursor<QS> {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        if self.cursor != rhs.cursor {
            self.cursor.partial_cmp(&rhs.cursor)
        } else {
            self.count.partial_cmp(&rhs.count)
        }
    }
}

impl<QS: ?Sized> Ord for DbPageCursor<QS> {
    fn cmp(&self, rhs: &Self) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

impl<QS: ?Sized> DbPageExt for DbPageCursor<QS> {
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
