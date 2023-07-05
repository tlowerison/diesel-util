mod cursor;
mod diesel;
mod env;
mod graphql;
mod offset;

pub use crate::paginate::cursor::*;
pub use crate::paginate::diesel::*;
pub use crate::paginate::env::*;
pub use crate::paginate::offset::*;

#[cfg(any(feature = "async-graphql-4", feature = "async-graphql-5"))]
pub(crate) use crate::paginate::graphql::*;

use itertools::Itertools;
use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

#[cfg(feature = "async-graphql-4")]
use async_graphql_4 as async_graphql;
#[cfg(feature = "async-graphql-5")]
use async_graphql_5 as async_graphql;

#[derive(AsVariant, AsVariantMut, Clone, Copy, Debug, Deserialize, Eq, Hash, IsVariant, PartialEq, Serialize)]
#[cfg_attr(
    any(feature = "async-graphql-4", feature = "async-graphql-5"),
    derive(async_graphql::OneofObject)
)]
pub enum Page {
    Cursor(PageCursor),
    Offset(PageOffset),
}

impl Page {
    pub fn on_column<QS, C>(self, column: C) -> DbPage<QS>
    where
        QS: ::diesel::query_source::QuerySource,
        C: ::diesel::AppearsOnTable<QS>
            + Clone
            + ::diesel::Column<SqlType = ::diesel::sql_types::Timestamp>
            + QF
            + Send
            + Sync
            + ::diesel::expression::ValidGrouping<(), IsAggregate = ::diesel::expression::is_aggregate::No>
            + 'static,
        <C as ::diesel::expression::Expression>::SqlType: ::diesel::sql_types::SingleValue,
    {
        match self {
            Self::Cursor(cursor) => DbPage::Cursor(cursor.on_column(column)),
            Self::Offset(offset) => DbPage::Offset(offset.into()),
        }
    }

    pub fn map_on_column<QS, C>(column: C) -> impl (Fn(Self) -> DbPage<QS>) + 'static
    where
        QS: ::diesel::query_source::QuerySource,
        C: ::diesel::AppearsOnTable<QS>
            + Clone
            + ::diesel::Column<SqlType = ::diesel::sql_types::Timestamp>
            + QF
            + Send
            + Sync
            + ::diesel::expression::ValidGrouping<(), IsAggregate = ::diesel::expression::is_aggregate::No>
            + 'static,
        <C as ::diesel::expression::Expression>::SqlType: ::diesel::sql_types::SingleValue,
    {
        move |page| page.on_column(column.clone())
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
