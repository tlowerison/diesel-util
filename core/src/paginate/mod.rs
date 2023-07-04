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
use std::borrow::Borrow;
use std::cmp::Ordering;

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
pub enum Page<Table: ?Sized> {
    Cursor(PageCursor<Table>),
    Offset(PageOffset),
}

impl<Table: ?Sized> Ord for Page<Table> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<Table: ?Sized> PartialOrd for Page<Table> {
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

impl<Tab> PageExt for Page<Tab> {
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

impl<Tab> Page<Tab> {
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

// necessary to use instead of something like Borrow
// because coercion from double reference to single reference
// does not occur automatically when passing in a value that needs
// to implement Borrow<Page>
// e.g. &[&Page].iter() == Iterator<Item = &&Page> and &&Page: !Borrow<Page>
pub trait PageRef<'a, Tab: ?Sized> {
    fn page_ref(&'a self) -> &'a Page<Tab>;
}

pub trait AsPage<Tab: ?Sized> {
    fn as_page(&self) -> Option<&Page<Tab>>;
}

impl<Tab> AsRef<Page<Tab>> for Page<Tab> {
    fn as_ref(&self) -> &Page<Tab> {
        self
    }
}

impl<'a, T, Tab> PageRef<'a, Tab> for T
where
    T: AsRef<Page<Tab>>,
{
    fn page_ref(&'a self) -> &'a Page<Tab> {
        self.as_ref()
    }
}

impl<Tab, P: Borrow<Page<Tab>>> AsPage<Tab> for P {
    fn as_page(&self) -> Option<&Page<Tab>> {
        Some(self.borrow())
    }
}

impl<Tab> AsPage<Tab> for Option<Page<Tab>> {
    fn as_page(&self) -> Option<&Page<Tab>> {
        self.as_ref()
    }
}

impl<Tab> AsPage<Tab> for &Option<Page<Tab>> {
    fn as_page(&self) -> Option<&Page<Tab>> {
        self.as_ref()
    }
}

impl<Tab> AsPage<Tab> for Option<&Page<Tab>> {
    fn as_page(&self) -> Option<&Page<Tab>> {
        *self
    }
}

impl<Tab> AsPage<Tab> for &Option<&Page<Tab>> {
    fn as_page(&self) -> Option<&Page<Tab>> {
        **self
    }
}
