use crate::paginate::DbPageExt;
use std::borrow::Borrow;
use std::cmp::Ordering;

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
pub struct PageOffset {
    #[cfg_attr(
        any(
            feature = "async-graphql-4",
            feature = "async-graphql-5",
            feature = "async-graphql-6"
        ),
        graphql(validator(custom = "crate::paginate::GraphqlPaginationCountValidator"))
    )]
    pub count: u32,
    pub index: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct DbPageOffset {
    pub left: i64,
    pub right: i64,
}

impl PageOffset {
    pub fn with_count(count: u32) -> Self {
        Self { count, index: 0 }
    }
}

impl Ord for PageOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd for PageOffset {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        if self.index != rhs.index {
            self.index.partial_cmp(&rhs.index)
        } else {
            self.count.partial_cmp(&rhs.count)
        }
    }
}

impl DbPageOffset {
    pub fn with_count(count: u32) -> Self {
        Self {
            left: 0,
            right: count as i64,
        }
    }
}

impl From<PageOffset> for DbPageOffset {
    fn from(value: PageOffset) -> Self {
        Self {
            left: (value.index * value.count) as i64,
            right: (value.count + value.index * value.count) as i64,
        }
    }
}

impl PartialOrd for DbPageOffset {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        if self.left != rhs.left {
            self.left.partial_cmp(&rhs.left)
        } else {
            self.right.partial_cmp(&rhs.right)
        }
    }
}

impl Ord for DbPageOffset {
    fn cmp(&self, rhs: &Self) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

impl DbPageExt for DbPageOffset {
    fn is_empty(&self) -> bool {
        self.left == self.right
    }
    fn merge(page_offsets: impl IntoIterator<Item = impl Borrow<Self>>) -> Vec<Self> {
        let mut page_offsets = page_offsets
            .into_iter()
            .map(|page_offset| *page_offset.borrow())
            .collect::<Vec<Self>>();

        page_offsets.sort();

        let mut merged = Vec::new();
        if !page_offsets.is_empty() {
            merged.push(page_offsets[0]);
        }
        for page in page_offsets.into_iter().skip(1) {
            let merged_len = merged.len();
            let last_merged = &mut merged[merged_len - 1];
            if page.left <= last_merged.right {
                last_merged.right = last_merged.right.max(page.right);
            } else {
                merged.push(page);
            }
        }
        merged
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Range {
    pub(crate) left_exclusive: i64,
    pub(crate) right_inclusive: i64,
    pub(crate) kind: RangeKind,
}

impl From<&DbPageOffset> for Range {
    fn from(value: &DbPageOffset) -> Self {
        Self {
            left_exclusive: value.left,
            right_inclusive: value.right,
            kind: RangeKind::Basic,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RangeKind {
    Basic,
    LowerBound,
    UpperBound,
}

impl PartialOrd for Range {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        use RangeKind::*;
        let (range, bound, bound_is_lower, parity) = match (&self.kind, &rhs.kind) {
            (Basic, LowerBound) => (self, rhs, true, false),
            (Basic, UpperBound) => (self, rhs, false, false),
            (LowerBound, Basic) => (rhs, self, true, true),
            (UpperBound, Basic) => (rhs, self, false, true),
            (LowerBound, UpperBound) => return Some(Ordering::Less),
            (UpperBound, LowerBound) => return Some(Ordering::Greater),
            _ => {
                if self.left_exclusive != rhs.left_exclusive {
                    return self.left_exclusive.partial_cmp(&rhs.left_exclusive);
                } else {
                    return self.right_inclusive.partial_cmp(&rhs.right_inclusive);
                }
            }
        };

        Some(
            if (bound.left_exclusive <= range.left_exclusive && bound.right_inclusive >= range.right_inclusive)
                ^ bound_is_lower
                ^ parity
            {
                Ordering::Greater
            } else {
                Ordering::Less
            },
        )
    }
}

impl Ord for Range {
    fn cmp(&self, rhs: &Self) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}
