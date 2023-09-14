mod cursor;
mod diesel;
mod env;
mod graphql;
mod offset;

pub use crate::paginate::cursor::*;
pub use crate::paginate::diesel::*;
pub use crate::paginate::env::*;
pub use crate::paginate::offset::*;

#[cfg(any(
    feature = "async-graphql-4",
    feature = "async-graphql-5",
    feature = "async-graphql-6"
))]
pub(crate) use crate::paginate::graphql::*;

use ::chrono::NaiveDateTime;
use ::diesel::dsl::{self, And, IsNotNull, Or};
use ::diesel::expression::{is_aggregate::No, AsExpression, ValidGrouping};
use ::diesel::helper_types::{Gt, GtEq, Lt, LtEq};
use ::diesel::sql_types::is_nullable::{IsNullable, NotNull};
use ::diesel::sql_types::BoolOrNullableBool;
use ::diesel::sql_types::MaybeNullableType;
use ::diesel::sql_types::OneIsNullable;
use ::diesel::sql_types::{Bool, Nullable, SingleValue, SqlType};
use ::diesel::{AppearsOnTable, BoolExpressionMethods, Column, Expression, QuerySource};
use ::dyn_clone::DynClone;
use ::itertools::Itertools;
use ::std::borrow::{Borrow, Cow};
use ::std::cmp::Ordering;
use ::std::collections::{BTreeMap, HashMap};
use ::uuid::Uuid;

#[cfg(feature = "async-graphql-4")]
use async_graphql_4 as async_graphql;
#[cfg(feature = "async-graphql-5")]
use async_graphql_5 as async_graphql;
#[cfg(feature = "async-graphql-6")]
use async_graphql_6 as async_graphql;

#[derive(
    AsVariant, AsVariantMut, Clone, Copy, Debug, Deserialize, Eq, Hash, IsVariant, Ord, PartialEq, PartialOrd, Serialize,
)]
#[cfg_attr(
    any(
        feature = "async-graphql-4",
        feature = "async-graphql-5",
        feature = "async-graphql-6"
    ),
    derive(async_graphql::OneofObject)
)]
pub enum Page {
    Cursor(PageCursor),
    Offset(PageOffset),
}

impl Page {
    pub fn count(&self) -> u32 {
        match self {
            Self::Cursor(cursor) => cursor.count,
            Self::Offset(offset) => offset.count,
        }
    }
}

#[derive(AsVariant, AsVariantMut, Derivative, IsVariant, Unwrap)]
#[derivative(
    Clone(bound = ""),
    Debug(bound = ""),
    Eq(bound = ""),
    Hash(bound = ""),
    PartialEq(bound = "")
)]
pub enum DbPage<QS: ?Sized> {
    Cursor(DbPageCursor<QS>),
    Offset(DbPageOffset),
}

impl<QS: ?Sized> Ord for DbPage<QS> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<QS: ?Sized> PartialOrd for DbPage<QS> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            DbPage::Cursor(lhs) => match other {
                DbPage::Cursor(rhs) => lhs.partial_cmp(rhs),
                DbPage::Offset(_) => Some(Ordering::Less),
            },
            DbPage::Offset(lhs) => match other {
                DbPage::Cursor(_) => Some(Ordering::Greater),
                DbPage::Offset(rhs) => lhs.partial_cmp(rhs),
            },
        }
    }
}

pub trait DbPageExt {
    fn is_empty(&self) -> bool;
    fn merge(items: impl IntoIterator<Item = impl Borrow<Self>>) -> Vec<Self>
    where
        Self: Sized;
}

impl<QS> DbPageExt for DbPage<QS> {
    fn is_empty(&self) -> bool {
        match self {
            Self::Cursor(page_cursor) => page_cursor.is_empty(),
            Self::Offset(page_offset) => page_offset.is_empty(),
        }
    }
    fn merge(pages: impl IntoIterator<Item = impl Borrow<Self>>) -> Vec<Self> {
        let pages = pages.into_iter().collect_vec();
        let (page_cursors, page_offsets): (Vec<_>, Vec<_>) = pages
            .iter()
            .map(|page| {
                let page = page.borrow();
                (page.as_cursor(), page.as_offset())
            })
            .unzip();
        let page_cursors = page_cursors.into_iter().flatten().collect_vec();
        let page_offsets = page_offsets.into_iter().flatten().collect_vec();

        let mut pages = Vec::<Self>::new();
        pages.extend(&mut DbPageCursor::merge(page_cursors).into_iter().map(DbPage::Cursor));
        pages.extend(&mut DbPageOffset::merge(page_offsets).into_iter().map(DbPage::Offset));

        pages
    }
}

pub(crate) struct DbPageSplit {
    cursor_indices: Vec<usize>,
    offset_indices: Vec<usize>,
}

impl<QS> DbPage<QS> {
    pub(crate) fn split(pages: &[Self]) -> DbPageSplit {
        let (page_cursors, page_offsets): (Vec<_>, Vec<_>) = pages
            .iter()
            .enumerate()
            .map(|(i, page)| match page {
                DbPage::Cursor(_) => (Some(i), None),
                DbPage::Offset(_) => (None, Some(i)),
            })
            .unzip();

        DbPageSplit {
            cursor_indices: page_cursors.into_iter().flatten().collect(),
            offset_indices: page_offsets.into_iter().flatten().collect(),
        }
    }
}

pub fn split_multipaginated_results<T: ?Sized, R: Clone>(
    results: Vec<(R, Option<i64>, Option<String>)>,
    pages: Vec<DbPage<T>>,
) -> HashMap<DbPage<T>, Vec<R>> {
    use std::ops::Bound::*;
    use std::str::FromStr;

    let mut pages_by_cursor_id = HashMap::<Uuid, &DbPage<T>>::default();
    let mut pages_by_page_offset_range = BTreeMap::<Range, &DbPage<T>>::default();
    for page in &pages {
        match page {
            DbPage::Cursor(page_cursor) => {
                pages_by_cursor_id.insert(page_cursor.id, page);
            }
            DbPage::Offset(page_offset) => {
                pages_by_page_offset_range.insert(page_offset.into(), page);
            }
        }
    }
    let (range_min, range_max) = if !pages_by_page_offset_range.is_empty() {
        (
            pages_by_page_offset_range.first_key_value().unwrap().0.left_exclusive,
            pages_by_page_offset_range.last_key_value().unwrap().0.right_inclusive,
        )
    } else {
        (0, 0) // never used
    };

    let mut results_by_page = HashMap::<DbPage<T>, Vec<R>>::default();
    for (result, row_number, page_id) in results {
        if let Some(page_id) = page_id {
            let page_id = Uuid::from_str(&page_id).unwrap();
            let page = pages_by_cursor_id.get(&page_id).unwrap();
            if !results_by_page.contains_key(page) {
                results_by_page.insert((*page).clone(), vec![]);
            }
            results_by_page.get_mut(page).unwrap().push(result);
        } else if let Some(row_number) = row_number {
            let mut matches = pages_by_page_offset_range.range((
                Excluded(Range {
                    left_exclusive: range_min,
                    right_inclusive: row_number - 1,
                    kind: RangeKind::LowerBound,
                }),
                Excluded(Range {
                    left_exclusive: row_number,
                    right_inclusive: range_max,
                    kind: RangeKind::UpperBound,
                }),
            ));
            if let Some((_, first_match)) = matches.next() {
                let new_pages_and_results = matches.map(|(_, page)| (*page, result.clone())).collect::<Vec<_>>();

                if !results_by_page.contains_key(first_match) {
                    results_by_page.insert((*first_match).clone(), vec![]);
                }
                results_by_page.get_mut(first_match).unwrap().push(result);

                for (page, result) in new_pages_and_results {
                    if !results_by_page.contains_key(page) {
                        results_by_page.insert((*page).clone(), vec![]);
                    }
                    results_by_page.get_mut(page).unwrap().push(result);
                }
            }
        }
    }

    results_by_page
}

impl Page {
    pub fn on_column<QS, C>(self, column: C) -> DbPage<QS>
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
        match self {
            Self::Cursor(cursor) => DbPage::Cursor(cursor.on_column(column)),
            Self::Offset(offset) => DbPage::Offset(offset.into()),
        }
    }

    pub fn on_columns<QS, C1, C2, N1, N2>(self, column1: C1, column2: C2) -> DbPage<QS>
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
        match self {
            Self::Cursor(cursor) => DbPage::Cursor(cursor.on_columns::<QS, C1, C2, N1, N2>(column1, column2)),
            Self::Offset(offset) => DbPage::Offset(offset.into()),
        }
    }

    pub fn map_on_column<QS, C>(column: C) -> impl (Fn(Self) -> DbPage<QS>) + 'static
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
            + dyn_clone::DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ::diesel::expression::ValidGrouping<(), IsAggregate = ::diesel::expression::is_aggregate::No>
            + 'static,
        dsl::Nullable<GtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
            + dyn_clone::DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ::diesel::expression::ValidGrouping<(), IsAggregate = ::diesel::expression::is_aggregate::No>
            + 'static,
        dsl::Nullable<Lt<C, NaiveDateTime>>: AppearsOnTable<QS>
            + dyn_clone::DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ::diesel::expression::ValidGrouping<(), IsAggregate = ::diesel::expression::is_aggregate::No>
            + 'static,
        dsl::Nullable<LtEq<C, NaiveDateTime>>: AppearsOnTable<QS>
            + dyn_clone::DynClone
            + Expression<SqlType = Nullable<Bool>>
            + QF // see bottom of file for QF definition
            + Send
            + Sync
            + ::diesel::expression::ValidGrouping<(), IsAggregate = ::diesel::expression::is_aggregate::No>
            + 'static,
    {
        move |page| page.on_column(column.clone())
    }
}

// necessary to use instead of something like Borrow
// because coercion from double reference to single reference
// does not occur automatically when passing in a value that needs
// to implement Borrow<DbPage>
// e.g. &[&DbPage].iter() == Iterator<Item = &&DbPage> and &&DbPage: !Borrow<DbPage>
pub trait DbPageRef<'a, QS: ?Sized> {
    fn page_ref(&'a self) -> &'a DbPage<QS>;
}

pub trait AsDbPage<QS: ?Sized> {
    fn as_page(&self) -> Option<&DbPage<QS>>;
}

impl<QS> AsRef<DbPage<QS>> for DbPage<QS> {
    fn as_ref(&self) -> &DbPage<QS> {
        self
    }
}

impl<'a, T, QS> DbPageRef<'a, QS> for T
where
    T: AsRef<DbPage<QS>>,
{
    fn page_ref(&'a self) -> &'a DbPage<QS> {
        self.as_ref()
    }
}

impl<QS, P: Borrow<DbPage<QS>>> AsDbPage<QS> for P {
    fn as_page(&self) -> Option<&DbPage<QS>> {
        Some(self.borrow())
    }
}

impl<QS> AsDbPage<QS> for Option<DbPage<QS>> {
    fn as_page(&self) -> Option<&DbPage<QS>> {
        self.as_ref()
    }
}

impl<QS> AsDbPage<QS> for &Option<DbPage<QS>> {
    fn as_page(&self) -> Option<&DbPage<QS>> {
        self.as_ref()
    }
}

impl<QS> AsDbPage<QS> for Option<&DbPage<QS>> {
    fn as_page(&self) -> Option<&DbPage<QS>> {
        *self
    }
}

impl<QS> AsDbPage<QS> for &Option<&DbPage<QS>> {
    fn as_page(&self) -> Option<&DbPage<QS>> {
        **self
    }
}

pub trait OptDbPageRef<'a, QS: ?Sized> {
    fn opt_page_ref(&'a self) -> Option<&'a DbPage<QS>>;
}

impl<'a, T, QS: ?Sized> OptDbPageRef<'a, QS> for Option<T>
where
    T: OptDbPageRef<'a, QS>,
{
    fn opt_page_ref(&'a self) -> Option<&'a DbPage<QS>> {
        match self {
            Some(some) => some.opt_page_ref(),
            None => None,
        }
    }
}

impl<'a, 'b: 'a, T, QS: ?Sized> OptDbPageRef<'a, QS> for &'b T
where
    T: OptDbPageRef<'a, QS>,
{
    fn opt_page_ref(&'a self) -> Option<&'a DbPage<QS>> {
        OptDbPageRef::opt_page_ref(*self)
    }
}

impl<'a, QS: ?Sized> OptDbPageRef<'a, QS> for DbPage<QS> {
    fn opt_page_ref(&'a self) -> Option<&'a DbPage<QS>> {
        Some(self)
    }
}

pub trait OptDbPageRefs<'a, QS: ?Sized> {
    type _IntoIter: IntoIterator<Item = Self::_Item> + 'a;
    type _Item: for<'b> DbPageRef<'b, QS>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter>;
}

#[derive(Clone, Debug)]
pub enum OptDbPageRefsEither<L, R> {
    Left(L),
    Right(R),
}
#[derive(Clone, Debug)]
pub enum OptDbPageRefsEitherIter<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> IntoIterator for OptDbPageRefsEither<L, R>
where
    L: IntoIterator,
    R: IntoIterator,
{
    type IntoIter = OptDbPageRefsEitherIter<L::IntoIter, R::IntoIter>;
    type Item = OptDbPageRefsEither<L::Item, R::Item>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Left(left) => OptDbPageRefsEitherIter::Left(left.into_iter()),
            Self::Right(right) => OptDbPageRefsEitherIter::Right(right.into_iter()),
        }
    }
}

impl<L, R> Iterator for OptDbPageRefsEitherIter<L, R>
where
    L: Iterator,
    R: Iterator,
{
    type Item = OptDbPageRefsEither<L::Item, R::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Left(left) => left.next().map(OptDbPageRefsEither::Left),
            Self::Right(right) => right.next().map(OptDbPageRefsEither::Right),
        }
    }
}

impl<'a, L, R, QS: ?Sized> DbPageRef<'a, QS> for OptDbPageRefsEither<L, R>
where
    L: DbPageRef<'a, QS>,
    R: DbPageRef<'a, QS>,
{
    fn page_ref(&'a self) -> &'a DbPage<QS> {
        match self {
            Self::Left(left) => left.page_ref(),
            Self::Right(right) => right.page_ref(),
        }
    }
}

impl<'a, 'b: 'a, T, QS: ?Sized> OptDbPageRefs<'a, QS> for &'b T
where
    T: OptDbPageRefs<'a, QS>,
{
    type _IntoIter = T::_IntoIter;
    type _Item = T::_Item;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        OptDbPageRefs::opt_page_refs(*self)
    }
}

impl<'a, T, QS: ?Sized> OptDbPageRefs<'a, QS> for Option<T>
where
    T: OptDbPageRefs<'a, QS>,
{
    type _IntoIter = T::_IntoIter;
    type _Item = T::_Item;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        match self {
            Some(some) => OptDbPageRefs::opt_page_refs(some),
            None => None,
        }
    }
}

impl<'a, 'b: 'a, T: ToOwned, QS: ?Sized> OptDbPageRefs<'a, QS> for Cow<'b, T>
where
    T: OptDbPageRefs<'a, QS> + IntoIterator,
    T::Owned: OptDbPageRefs<'a, QS> + IntoIterator,
    T::Item: 'b,
    <T::Owned as IntoIterator>::Item: 'b,
{
    type _IntoIter = OptDbPageRefsEither<T::_IntoIter, <T::Owned as OptDbPageRefs<'a, QS>>::_IntoIter>;
    type _Item = OptDbPageRefsEither<T::_Item, <T::Owned as OptDbPageRefs<'a, QS>>::_Item>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        match self {
            Cow::Borrowed(borrowed) => OptDbPageRefs::opt_page_refs(borrowed).map(OptDbPageRefsEither::Left),
            Cow::Owned(owned) => OptDbPageRefs::opt_page_refs(owned).map(OptDbPageRefsEither::Right),
        }
    }
}

impl<'a, QS: 'a> OptDbPageRefs<'a, QS> for Vec<DbPage<QS>> {
    type _IntoIter = &'a [DbPage<QS>];
    type _Item = &'a DbPage<QS>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        Some(self)
    }
}

impl<'a, 'b: 'a, QS: 'a> OptDbPageRefs<'a, QS> for &'b [DbPage<QS>] {
    type _IntoIter = &'a [DbPage<QS>];
    type _Item = &'a DbPage<QS>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        Some(self)
    }
}

impl<'a, const N: usize, QS: 'a> OptDbPageRefs<'a, QS> for [DbPage<QS>; N] {
    type _IntoIter = &'a [DbPage<QS>];
    type _Item = &'a DbPage<QS>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        Some(self)
    }
}

impl<'a, 'b: 'a, QS: 'a> OptDbPageRefs<'a, QS> for Cow<'b, [DbPage<QS>]> {
    type _IntoIter = &'a [DbPage<QS>];
    type _Item = &'a DbPage<QS>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        Some(&**self)
    }
}
