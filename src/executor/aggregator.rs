use std::marker::PhantomData;

use bigdecimal::Num;
use futures::Stream;
use sea_query::{Alias, Expr, SelectStatement};

use crate::{
    error::*, ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, Select, SelectModel,
    SelectTwo, SelectTwoModel, Selector, SelectorRaw, SelectorTrait,
};

pub type PinBoxStream<'db, Item> = Pin<Box<dyn Stream<Item = Item> + 'db>>;

/// Defined a structure to handle aggregate values from a query on a Model
#[derive(Clone, Debug)]
pub struct Aggregator<'db, C, S, T>
where
    C: ConnectionTrait,
    S: SelectorTrait + 'db,
    T: Num,
{
    pub(crate) query: SelectStatement,
    pub(crate) selector: PhantomData<S>,
}

impl<'db, C, S, T> Aggregator<'db, C, S, T>
where
    C: ConnectionTrait,
    S: SelectorTrait + 'db,
{
    pub async fn one(&self, db: &'db C) -> Result<T, DbErr> {
        let builder = db.get_database_backend();
        let stmt = builder.build(&self.query);
        let result = match db.query_one(stmt).await? {
            Some(res) => res,
            None => return Ok(0 as T),
        };
        let num = result.try_get::<T>("", "num")?;
        Ok(num)
    }
}

#[async_trait::async_trait]
pub trait AggregatorTrait<'db, C>
where
    C: ConnectionTrait,
{
    type Selector: SelectorTrait + Send + Sync + 'db;
    type Res: Num;

    fn count<T: ColumnTrait>(self, col: T) -> Aggregator<'db, C, Self::Selector, Self::Res>;
    fn max<T>(self, col: T) -> Aggregator<'db, C, Self::Selector, Self::Res>;
    fn min<T>(self, col: T) -> Aggregator<'db, C, Self::Selector, Self::Res>;
    fn avg<T>(self, col: T) -> Aggregator<'db, C, Self::Selector, Self::Res>;
}

impl<'db, C, S> AggregatorTrait<'db, C> for Selector<S>
where
    C: ConnectionTrait,
    S: SelectorTrait + Send + Sync + 'db,
{
    type Selector = S;

    fn count<T: ColumnTrait>(self, col: T) -> Aggregator<'db, C, Self::Selector, u64> {
        let query = SelectStatement::new()
            .expr(Expr::count(col))
            .from_subquery(self.query.clone(), Alias::new("sub_query"))
            .to_owned();
        Aggregator {
            query,
            selector: PhantomData,
        }
    }
}

// impl<'db, C, S> AggregatorTrait<'db, C> for SelectorRaw<S>
// where
//     C: ConnectionTrait,
//     S: SelectorTrait + Send + Sync + 'db,
// {
//     type Selector = S;
//
//     fn aggregate(self, db: &'db C) -> Aggregator<'db, C, S> {
//         let sql = self.stmt.sql.trim()[6..].trim();
//         let mut query = SelectStatement::new();
//         query.expr(if let Some(values) = self.stmt.values {
//             Expr::cust_with_values(sql, values.0)
//         } else {
//             Expr::cust(sql)
//         });
//         Aggregator {
//             query,
//             db,
//             selector: PhantomData,
//         }
//     }
// }
//
// impl<'db, C, M, E> AggregatorTrait<'db, C> for Select<E>
// where
//     C: ConnectionTrait,
//     E: EntityTrait<Model = M>,
//     M: FromQueryResult + Sized + Send + Sync + 'db,
// {
//     type Selector = SelectModel<M>;
//
//     fn aggregate(self, db: &'db C) -> Aggregator<'db, C, Self::Selector> {
//         self.into_model().aggregate(db)
//     }
// }
//
// impl<'db, C, M, N, E, F> AggregatorTrait<'db, C> for SelectTwo<E, F>
// where
//     C: ConnectionTrait,
//     E: EntityTrait<Model = M>,
//     F: EntityTrait<Model = N>,
//     M: FromQueryResult + Sized + Send + Sync + 'db,
//     N: FromQueryResult + Sized + Send + Sync + 'db,
// {
//     type Selector = SelectTwoModel<M, N>;
//
//     fn aggregate(self, db: &'db C) -> Aggregator<'db, C, Self::Selector> {
//         self.into_model().aggregate(db)
//     }
// }
