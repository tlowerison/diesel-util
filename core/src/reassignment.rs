pub trait Reassignment {
    type ParentId;
    type ChildId;
    fn parent_id(&self) -> &Self::ParentId;
    fn child_id(&self) -> &Self::ChildId;
}

#[derive(Clone, Debug)]
pub struct SetReassignment<'a, C, P, R> {
    pub child: C,
    pub parent: P,
    pub reassignments: &'a [R],
}

impl<'a, C, P, R> SetReassignment<'a, C, P, R> {
    pub fn new(child: C, parent: P, reassignments: &'a [R]) -> Self {
        Self {
            child,
            parent,
            reassignments,
        }
    }
}

impl<C, P, R> diesel::query_builder::AsChangeset for SetReassignment<'_, C, P, R>
where
    C: diesel::Column,
    P: diesel::Column,
    R: Reassignment,
{
    type Target = C::Table;
    type Changeset = Self;
    fn as_changeset(self) -> Self::Changeset {
        self
    }
}

impl<DB, C, P, R, CId, PId> diesel::query_builder::QueryFragment<DB> for SetReassignment<'_, C, P, R>
where
    DB: diesel::backend::Backend + diesel::sql_types::HasSqlType<CId> + diesel::sql_types::HasSqlType<PId>,
    C: diesel::Column<SqlType = CId>,
    P: diesel::Column<SqlType = PId>,
    R: Reassignment,
    R::ChildId: diesel::serialize::ToSql<CId, DB>,
    R::ParentId: diesel::serialize::ToSql<PId, DB>,
{
    fn walk_ast<'b>(
        &'b self,
        mut pass: diesel::query_builder::AstPass<'_, 'b, DB>,
    ) -> diesel::prelude::QueryResult<()> {
        pass.push_sql("(");
        pass.push_sql("case");
        for reassignment in self.reassignments {
            pass.push_sql(" when ");
            pass.push_sql(C::NAME);
            pass.push_sql(" = ");
            pass.push_bind_param::<CId, _>(reassignment.child_id())?;
            pass.push_sql(" then ");
            pass.push_bind_param::<PId, _>(reassignment.parent_id())?;
        }
        pass.push_sql(" else ");
        pass.push_sql(P::NAME);
        pass.push_sql(")");
        Ok(())
    }
}
