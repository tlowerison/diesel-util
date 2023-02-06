use crate::IsNotDeleted;
use diesel_async::methods::{ExecuteDsl, LoadQuery};
use diesel_async::AsyncConnection;
use scoped_futures::ImpliedLifetimeBound;

pub trait ScopedExecuteDsl<'upper_bound, 'scoped, C>: ExecuteDsl<C>
where
    C: AsyncConnection,
{
}

pub trait ScopedLoadQuery<
    'upper_bound,
    'scoped,
    'query,
    C,
    R,
    ImpliedBound = ImpliedLifetimeBound<'upper_bound, 'scoped>,
>: LoadQuery<'query, C, R> where
    C: AsyncConnection,
{
}

pub trait ScopedIsNotDeleted<
    'upper_bound,
    'scoped,
    'query,
    C,
    S,
    T,
    ImpliedBound = ImpliedLifetimeBound<'upper_bound, 'scoped>,
>: IsNotDeleted<'query, C, S, T> where
    C: AsyncConnection,
{
}

impl<'upper_bound: 'scoped, 'scoped, C, T> ScopedExecuteDsl<'upper_bound, 'scoped, C> for T
where
    C: AsyncConnection,
    T: ExecuteDsl<C>,
{
}

impl<'upper_bound: 'scoped, 'scoped, 'query, C, R, T>
    ScopedLoadQuery<'upper_bound, 'scoped, 'query, C, R> for T
where
    C: AsyncConnection,
    T: LoadQuery<'query, C, R>,
{
}

impl<'upper_bound: 'scoped, 'scoped, 'query, C, S, T, U>
    ScopedIsNotDeleted<'upper_bound, 'scoped, 'query, C, S, T> for U
where
    C: AsyncConnection,
    U: IsNotDeleted<'query, C, S, T>,
{
}
