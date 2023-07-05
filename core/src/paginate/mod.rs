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
pub enum ApiPage {
    Cursor(ApiPageCursor),
    Offset(ApiPageOffset),
}

#[derive(AsVariant, AsVariantMut, Derivative, IsVariant, Unwrap)]
#[derivative(
    Clone(bound = ""),
    Debug(bound = ""),
    Eq(bound = ""),
    Hash(bound = ""),
    PartialEq(bound = "")
)]
pub enum Page<QS: ?Sized> {
    Cursor(PageCursor<QS>),
    Offset(PageOffset),
}

impl<QS: ?Sized> Ord for Page<QS> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<QS: ?Sized> PartialOrd for Page<QS> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Page::Cursor(lhs) => match other {
                Page::Cursor(rhs) => lhs.partial_cmp(rhs),
                Page::Offset(_) => Some(Ordering::Less),
            },
            Page::Offset(lhs) => match other {
                Page::Cursor(_) => Some(Ordering::Greater),
                Page::Offset(rhs) => lhs.partial_cmp(rhs),
            },
        }
    }
}

pub trait PageExt {
    fn is_empty(&self) -> bool;
    fn merge(items: impl IntoIterator<Item = impl Borrow<Self>>) -> Vec<Self>
    where
        Self: Sized;
}

impl<QS> PageExt for Page<QS> {
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
        pages.extend(&mut PageCursor::merge(page_cursors).into_iter().map(Page::Cursor));
        pages.extend(&mut PageOffset::merge(page_offsets).into_iter().map(Page::Offset));

        pages
    }
}

pub(crate) struct PageSplit {
    cursor_indices: Vec<usize>,
    offset_indices: Vec<usize>,
}

impl<QS> Page<QS> {
    pub(crate) fn split(pages: &[Self]) -> PageSplit {
        let (page_cursors, page_offsets): (Vec<_>, Vec<_>) = pages
            .iter()
            .enumerate()
            .map(|(i, page)| match page {
                Page::Cursor(_) => (Some(i), None),
                Page::Offset(_) => (None, Some(i)),
            })
            .unzip();

        PageSplit {
            cursor_indices: page_cursors.into_iter().flatten().collect(),
            offset_indices: page_offsets.into_iter().flatten().collect(),
        }
    }
}

pub fn split_multipaginated_results<T: ?Sized, R: Clone>(
    results: Vec<(R, Option<i64>, Option<String>)>,
    pages: Vec<Page<T>>,
) -> HashMap<Page<T>, Vec<R>> {
    use std::ops::Bound::*;
    use std::str::FromStr;

    let mut pages_by_cursor_id = HashMap::<Uuid, &Page<T>>::default();
    let mut pages_by_page_offset_range = BTreeMap::<Range, &Page<T>>::default();
    for page in &pages {
        match page {
            Page::Cursor(page_cursor) => {
                pages_by_cursor_id.insert(page_cursor.id, page);
            }
            Page::Offset(page_offset) => {
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

    let mut results_by_page = HashMap::<Page<T>, Vec<R>>::default();
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
// to implement Borrow<Page>
// e.g. &[&Page].iter() == Iterator<Item = &&Page> and &&Page: !Borrow<Page>
pub trait PageRef<'a, QS: ?Sized> {
    fn page_ref(&'a self) -> &'a Page<QS>;
}

pub trait AsPage<QS: ?Sized> {
    fn as_page(&self) -> Option<&Page<QS>>;
}

impl<QS> AsRef<Page<QS>> for Page<QS> {
    fn as_ref(&self) -> &Page<QS> {
        self
    }
}

impl<'a, T, QS> PageRef<'a, QS> for T
where
    T: AsRef<Page<QS>>,
{
    fn page_ref(&'a self) -> &'a Page<QS> {
        self.as_ref()
    }
}

impl<QS, P: Borrow<Page<QS>>> AsPage<QS> for P {
    fn as_page(&self) -> Option<&Page<QS>> {
        Some(self.borrow())
    }
}

impl<QS> AsPage<QS> for Option<Page<QS>> {
    fn as_page(&self) -> Option<&Page<QS>> {
        self.as_ref()
    }
}

impl<QS> AsPage<QS> for &Option<Page<QS>> {
    fn as_page(&self) -> Option<&Page<QS>> {
        self.as_ref()
    }
}

impl<QS> AsPage<QS> for Option<&Page<QS>> {
    fn as_page(&self) -> Option<&Page<QS>> {
        *self
    }
}

impl<QS> AsPage<QS> for &Option<&Page<QS>> {
    fn as_page(&self) -> Option<&Page<QS>> {
        **self
    }
}

pub trait OptPageRef<'a, QS: ?Sized> {
    fn opt_page_ref(&'a self) -> Option<&'a Page<QS>>;
}

impl<'a, T, QS: ?Sized> OptPageRef<'a, QS> for Option<T>
where
    T: OptPageRef<'a, QS>,
{
    fn opt_page_ref(&'a self) -> Option<&'a Page<QS>> {
        match self {
            Some(some) => some.opt_page_ref(),
            None => None,
        }
    }
}

impl<'a, 'b: 'a, T, QS: ?Sized> OptPageRef<'a, QS> for &'b T
where
    T: OptPageRef<'a, QS>,
{
    fn opt_page_ref(&'a self) -> Option<&'a Page<QS>> {
        OptPageRef::opt_page_ref(*self)
    }
}

impl<'a, QS: ?Sized> OptPageRef<'a, QS> for Page<QS> {
    fn opt_page_ref(&'a self) -> Option<&'a Page<QS>> {
        Some(self)
    }
}

pub trait OptPageRefs<'a, QS: ?Sized> {
    type _IntoIter: IntoIterator<Item = Self::_Item> + 'a;
    type _Item: for<'b> PageRef<'b, QS>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter>;
}

#[derive(Clone, Debug)]
pub enum OptPageRefsEither<L, R> {
    Left(L),
    Right(R),
}
#[derive(Clone, Debug)]
pub enum OptPageRefsEitherIter<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> IntoIterator for OptPageRefsEither<L, R>
where
    L: IntoIterator,
    R: IntoIterator,
{
    type IntoIter = OptPageRefsEitherIter<L::IntoIter, R::IntoIter>;
    type Item = OptPageRefsEither<L::Item, R::Item>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Left(left) => OptPageRefsEitherIter::Left(left.into_iter()),
            Self::Right(right) => OptPageRefsEitherIter::Right(right.into_iter()),
        }
    }
}

impl<L, R> Iterator for OptPageRefsEitherIter<L, R>
where
    L: Iterator,
    R: Iterator,
{
    type Item = OptPageRefsEither<L::Item, R::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Left(left) => left.next().map(OptPageRefsEither::Left),
            Self::Right(right) => right.next().map(OptPageRefsEither::Right),
        }
    }
}

impl<'a, L, R, QS: ?Sized> PageRef<'a, QS> for OptPageRefsEither<L, R>
where
    L: PageRef<'a, QS>,
    R: PageRef<'a, QS>,
{
    fn page_ref(&'a self) -> &'a Page<QS> {
        match self {
            Self::Left(left) => left.page_ref(),
            Self::Right(right) => right.page_ref(),
        }
    }
}

impl<'a, 'b: 'a, T, QS: ?Sized> OptPageRefs<'a, QS> for &'b T
where
    T: OptPageRefs<'a, QS>,
{
    type _IntoIter = T::_IntoIter;
    type _Item = T::_Item;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        OptPageRefs::opt_page_refs(*self)
    }
}

impl<'a, T, QS: ?Sized> OptPageRefs<'a, QS> for Option<T>
where
    T: OptPageRefs<'a, QS>,
{
    type _IntoIter = T::_IntoIter;
    type _Item = T::_Item;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        match self {
            Some(some) => OptPageRefs::opt_page_refs(some),
            None => None,
        }
    }
}

impl<'a, 'b: 'a, T: ToOwned, QS: ?Sized> OptPageRefs<'a, QS> for Cow<'b, T>
where
    T: OptPageRefs<'a, QS> + IntoIterator,
    T::Owned: OptPageRefs<'a, QS> + IntoIterator,
    T::Item: 'b,
    <T::Owned as IntoIterator>::Item: 'b,
{
    type _IntoIter = OptPageRefsEither<T::_IntoIter, <T::Owned as OptPageRefs<'a, QS>>::_IntoIter>;
    type _Item = OptPageRefsEither<T::_Item, <T::Owned as OptPageRefs<'a, QS>>::_Item>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        match self {
            Cow::Borrowed(borrowed) => OptPageRefs::opt_page_refs(borrowed).map(OptPageRefsEither::Left),
            Cow::Owned(owned) => OptPageRefs::opt_page_refs(owned).map(OptPageRefsEither::Right),
        }
    }
}

impl<'a, QS: 'a> OptPageRefs<'a, QS> for Vec<Page<QS>> {
    type _IntoIter = &'a [Page<QS>];
    type _Item = &'a Page<QS>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        Some(self)
    }
}

impl<'a, 'b: 'a, QS: 'a> OptPageRefs<'a, QS> for &'b [Page<QS>] {
    type _IntoIter = &'a [Page<QS>];
    type _Item = &'a Page<QS>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        Some(self)
    }
}

impl<'a, const N: usize, QS: 'a> OptPageRefs<'a, QS> for [Page<QS>; N] {
    type _IntoIter = &'a [Page<QS>];
    type _Item = &'a Page<QS>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        Some(self)
    }
}

impl<'a, 'b: 'a, QS: 'a> OptPageRefs<'a, QS> for Cow<'b, [Page<QS>]> {
    type _IntoIter = &'a [Page<QS>];
    type _Item = &'a Page<QS>;
    fn opt_page_refs(&'a self) -> Option<Self::_IntoIter> {
        Some(&**self)
    }
}
