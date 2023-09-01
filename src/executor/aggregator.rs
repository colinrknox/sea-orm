use std::marker::PhantomData;

use sea_query::{Expr, SelectStatement};

use crate::{
    error::*, ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, Select, SelectModel,
    SelectTwo, SelectTwoModel, Selector, SelectorRaw, SelectorTrait,
};

/// Defined a structure to handle aggregate values from a query on a Model
#[derive(Clone, Debug)]
pub struct Aggregator<'db, C, S>
where
    C: ConnectionTrait,
    S: SelectorTrait + 'db,
{
    pub(crate) query: SelectStatement,
    pub(crate) db: &'db C,
    pub(crate) selector: PhantomData<S>,
}

impl<'db, C, S> Aggregator<'db, C, S>
where
    C: ConnectionTrait,
    S: SelectorTrait + 'db,
{
    pub fn count<T>(&self, col: T) -> Result<i64, DbErr>
    where
        T: ColumnTrait,
    {
        Ok(10)
    }
}

#[async_trait::async_trait]
pub trait AggregatorTrait<'db, C>
where
    C: ConnectionTrait,
{
    type Selector: SelectorTrait + Send + Sync + 'db;

    fn aggregate(self, db: &'db C) -> Aggregator<'db, C, Self::Selector>;
}

impl<'db, C, S> AggregatorTrait<'db, C> for Selector<S>
where
    C: ConnectionTrait,
    S: SelectorTrait + Send + Sync + 'db,
{
    type Selector = S;

    fn aggregate(self, db: &'db C) -> Aggregator<'db, C, S> {
        Aggregator {
            query: self.query,
            db,
            selector: PhantomData,
        }
    }
}

impl<'db, C, S> AggregatorTrait<'db, C> for SelectorRaw<S>
where
    C: ConnectionTrait,
    S: SelectorTrait + Send + Sync + 'db,
{
    type Selector = S;

    fn aggregate(self, db: &'db C) -> Aggregator<'db, C, S> {
        let sql = self.stmt.sql.trim()[6..].trim();
        let mut query = SelectStatement::new();
        query.expr(if let Some(values) = self.stmt.values {
            Expr::cust_with_values(sql, values.0)
        } else {
            Expr::cust(sql)
        });
        Aggregator {
            query,
            db,
            selector: PhantomData,
        }
    }
}

impl<'db, C, M, E> AggregatorTrait<'db, C> for Select<E>
where
    C: ConnectionTrait,
    E: EntityTrait<Model = M>,
    M: FromQueryResult + Sized + Send + Sync + 'db,
{
    type Selector = SelectModel<M>;

    fn aggregate(self, db: &'db C) -> Aggregator<'db, C, Self::Selector> {
        self.into_model().aggregate(db)
    }
}

impl<'db, C, M, N, E, F> AggregatorTrait<'db, C> for SelectTwo<E, F>
where
    C: ConnectionTrait,
    E: EntityTrait<Model = M>,
    F: EntityTrait<Model = N>,
    M: FromQueryResult + Sized + Send + Sync + 'db,
    N: FromQueryResult + Sized + Send + Sync + 'db,
{
    type Selector = SelectTwoModel<M, N>;

    fn aggregate(self, db: &'db C) -> Aggregator<'db, C, Self::Selector> {
        self.into_model().aggregate(db)
    }
}
