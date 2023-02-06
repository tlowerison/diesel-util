use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub trait SqlUuidEnum: Clone + Sized {
    type Variants: Iterator<Item = Self>;
    fn id(&self) -> Uuid;
    fn variants() -> Self::Variants;
    fn with_title(&self) -> WithTitle<Self>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WithTitle<T> {
    pub id: Uuid,
    pub title: T,
}

#[macro_export]
macro_rules! lock_conn {
    ($db_conn:expr) => {
        std::ops::DerefMut::deref_mut(std::ops::DerefMut::deref_mut(
            &mut $db_conn.connection().await?.write().await,
        ))
    };
}

#[macro_export]
macro_rules! db_get {
    ($ty:ident, $table:ident, $conn:ident, $ids:expr$(,)?) => {{
        async {
            let ids = $ids;

            $table::table
                .filter($table::deleted_at.is_null().and($table::id.eq_any(ids)))
                .get_results::<$ty>($crate::lock_conn!($conn))
                .await
        }
    }};
}

#[macro_export]
macro_rules! db_get_page {
    ($ty:ident, $table:ident, $conn:ident, $page:expr $(,)?) => {
        $crate::paste! {
            {
                async {
                    $table::table
                        .filter($table::deleted_at.is_null())
                        .paginate($page)
                        .get_results::<$ty>($crate::lock_conn!($conn))
                        .await
                }
            }
        }
    };
}

#[macro_export]
macro_rules! db_get_pages {
    ($ty:ident, $table:ident, $conn:ident, $pages:expr $(,)?) => {
        $crate::paste! {
            {
                async {
                    $table::table
                        .filter($table::deleted_at.is_null())
                        .multipaginate($pages.iter())
                        .get_results::<$ty>($crate::lock_conn!($conn))
                        .await
                }
            }
        }
    };
}

#[macro_export]
macro_rules! db_insert_with_audit {
    ($ty:ident, $table:ident, $audit_ty:ident, $audit_table:ident, $conn:ident, $values:expr$(,)?) => {{
        let values = $values;

        $conn.raw_tx(|db_conn| {
            $crate::scoped_futures::ScopedFutureExt::scope_boxed(async move {
                let all_inserted = diesel::insert_into($table::table)
                    .values(values)
                    .get_results::<$ty>(db_conn)
                    .await?;

                let audit_posts: Vec<$audit_ty> = all_inserted
                    .clone()
                    .into_iter()
                    .map(|inserted| inserted.into())
                    .collect();

                diesel::insert_into($audit_table::table)
                    .values(audit_posts)
                    .execute(db_conn)
                    .await?;

                Ok::<_, diesel::result::Error>(all_inserted)
            })
        })
    }};
    ($ty:ident, $table:ident, $conn:ident, $values:expr$(,)?) => {
        $crate::paste! {
            db_insert_with_audit! {
                $ty,
                $table,
                [<$ty Audit>],
                [<$table _audit>],
                $conn,
                $values
            }
        }
    };
}

#[macro_export]
macro_rules! _db_update_with_audit_async_block {
    ($ty:ident, $table:ident, $audit_ty:ident, $audit_table:ident, $conn:ident, $values:expr$(, $has_deleted_at:tt)? $(,)?) => {
        {
            async {
                let values = $values;

                let ids: Vec<_> = values.iter().map(|value| value.id).collect();
                let no_change_value_ids: Vec<_> = values
                    .iter()
                    .filter_map(|value| if !value.includes_changes() { Some(value.id) } else { None })
                    .collect();

                let num_changed_values = ids.len() - no_change_value_ids.len();
                if num_changed_values == 0 {
                    Ok(vec![])
                } else {
                    let mut value_map: std::collections::HashMap<_, $ty> = $conn.raw_tx::<_, diesel::result::Error, _>(move |db_conn| $crate::scoped_futures::ScopedFutureExt::scope_boxed(async move {
                        // TODO: switch to parallel requests in the future -- requires DbConn to be cloneable
                        let mut all_updated = Vec::with_capacity(num_changed_values);
                        for value in values.iter().filter(|value| value.includes_changes()) {
                            let updated = diesel::update($table::table.find(value.id()));
                            // conditionally filter out deleted_at = true if column exists
                            $crate::_db_update_with_audit_async_block! { @ deleted_at updated $table $($has_deleted_at)? }
                            all_updated.push(updated.set(value).get_result::<$ty>(&mut *db_conn).await?);
                        }

                        let audit_posts: Vec<$audit_ty> = all_updated
                            .clone()
                            .into_iter()
                            .map(|updated| updated.into())
                            .collect();

                        diesel::insert_into($audit_table::table)
                            .values(audit_posts)
                            .execute(&mut *db_conn)
                            .await?;

                        Ok(
                            $table::table
                                .filter($table::id.eq_any(no_change_value_ids))
                                .get_results(&mut *db_conn)
                                .await?
                                .into_iter()
                                .chain(all_updated.into_iter())
                                .map(|value| (value.id, value))
                                .collect()
                        )
                    })).await?;

                    Ok::<_, diesel::result::Error>(
                        ids
                            .iter()
                            .map(|id| value_map.remove(id).unwrap())
                            .collect::<Vec<_>>()
                    )
                }
            }
        }
    };

    // default behavior: assume deleted_at is a column
    (@ deleted_at $updated:ident $table:ident) => { $crate::_db_update_with_audit_async_block! { @ deleted_at $updated $table true } };

    // deleted_at is not a column
    (@ deleted_at $updated:ident $table:ident false) => {};

    // filter columns for those which deleted_at is null
    (@ deleted_at $updated:ident $table:ident true) => { let $updated = $updated.filter($table::deleted_at.is_null()); };
}

#[macro_export]
macro_rules! db_update_with_audit {
    ($ty:ident, $table:ident, $audit_ty:ident, $audit_table:ident, $conn:ident, $values:expr$(, $has_deleted_at:tt)? $(,)?) => {
        $crate::scoped_futures::ScopedFutureExt::scoped($crate::_db_update_with_audit_async_block! {
                $ty,
                $table,
                $audit_ty,
                $audit_table,
                $conn,
                $values
                $(, $has_deleted_at)?
        })
    };
    ($ty:ident, $table:ident, $conn:ident, $values:expr$(, $has_deleted_at:tt)? $(,)?) => {
        $crate::paste! {
            db_update_with_audit! {
                $ty,
                $table,
                [<$ty Audit>],
                [<$table _audit>],
                $conn,
                $values
                $(, $has_deleted_at)?
            }
        }
    };
}

#[macro_export]
macro_rules! db_update_with_audit_send {
    ($ty:ident, $table:ident, $audit_ty:ident, $audit_table:ident, $conn:ident, $values:expr$(, $has_deleted_at:tt)? $(,)?) => {
        $crate::scoped_futures::ScopedFutureExt::scope_boxed($crate::_db_update_with_audit_async_block! {
                $ty,
                $table,
                $audit_ty,
                $audit_table,
                $conn,
                $values
                $(, $has_deleted_at)?
        })
    };
    ($ty:ident, $table:ident, $conn:ident, $values:expr$(, $has_deleted_at:tt)? $(,)?) => {
        $crate::paste! {
            db_update_with_audit_send! {
                $ty,
                $table,
                [<$ty Audit>],
                [<$table _audit>],
                $conn,
                $values
                $(, $has_deleted_at)?
            }
        }
    };
}

#[macro_export]
macro_rules! db_delete_with_audit {
    ($ty:ident, $table:ident, $audit_ty:ident, $audit_table:ident, $conn:ident, $by_column_name:ident, $values:expr $(,)?) => {{
        let values = $values.into_iter().collect::<Vec<_>>();

        $conn.raw_tx(|db_conn| {
            $crate::scoped_futures::ScopedFutureExt::scope_boxed(async move {
                let mut all_deleted = diesel::update($table::table.filter($table::deleted_at.is_null()).filter($table::$by_column_name.eq_any(values)))
                    .set(($table::updated_at.eq(Utc::now().naive_utc()), $table::deleted_at.eq(Utc::now().naive_utc())))
                    .get_results::<$ty>(db_conn)
                    .await?;

                let audit_posts: Vec<$audit_ty> = all_deleted.clone().into_iter().map(|deleted| deleted.into()).collect();

                diesel::insert_into($audit_table::table).values(audit_posts).execute(db_conn).await?;

                Ok::<Vec<$ty>, diesel::result::Error>(all_deleted)
            })
        })
    }};
    ($ty:ident, $table:ident, $audit_ty:ident, $audit_table:ident, $conn:ident, $values:expr $(,)?) => {
        db_delete_with_audit! { $ty, $table, $audit_ty, $audit_table, $conn, id, $values }
    };
    ($ty:ident, $table:ident, $conn:ident, $by_column_name:ident, $values:expr $(,)?) => {
        $crate::paste! { db_delete_with_audit! { $ty, $table, [<$ty Audit>], [<$table _audit>], $conn, $by_column_name, $values } }
    };
    ($ty:ident, $table:ident, $conn:ident, $values:expr $(,)?) => {
        $crate::paste! { db_delete_with_audit! { $ty, $table, [<$ty Audit>], [<$table _audit>], $conn, id, $values } }
    };
}

#[macro_export]
macro_rules! db_delete_with_audit_no_updated_at {
    ($ty:ident, $table:ident, $audit_ty:ident, $audit_table:ident, $conn:ident, $by_column_name:ident, $values:expr $(,)?) => {{
        let values = $values.into_iter().collect::<Vec<_>>();

        $conn.raw_tx(|db_conn| {
            $crate::scoped_futures::ScopedFutureExt::scope_boxed(async move {
                let mut all_deleted = diesel::update($table::table.filter($table::deleted_at.is_null()).filter($table::$by_column_name.eq_any(values)))
                    .set(($table::deleted_at.eq(Utc::now().naive_utc()),))
                    .get_results::<$ty>(db_conn)
                    .await?;

                let audit_posts: Vec<$audit_ty> = all_deleted.clone().into_iter().map(|deleted| deleted.into()).collect();

                diesel::insert_into($audit_table::table).values(audit_posts).execute(db_conn).await?;

                Ok::<Vec<$ty>, diesel::result::Error>(all_deleted)
            })
        })
    }};
    ($ty:ident, $table:ident, $audit_ty:ident, $audit_table:ident, $conn:ident, $values:expr $(,)?) => {
        db_delete_with_audit_no_updated_at! { $ty, $table, $audit_ty, $audit_table, $conn, id, $values }
    };
    ($ty:ident, $table:ident, $conn:ident, $by_column_name:ident, $values:expr $(,)?) => {
        $crate::paste! { db_delete_with_audit_no_updated_at! { $ty, $table, [<$ty Audit>], [<$table _audit>], $conn, $by_column_name, $values } }
    };
    ($ty:ident, $table:ident, $conn:ident, $values:expr $(,)?) => {
        $crate::paste! { db_delete_with_audit_no_updated_at! { $ty, $table, [<$ty Audit>], [<$table _audit>], $conn, id, $values } }
    };
}
