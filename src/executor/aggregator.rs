use std::marker::PhantomData;

use bigdecimal::{num_traits::NumCast, Num};
use sea_query::{Alias, ColumnType, Expr, SelectStatement, SimpleExpr};

use crate::{
    error::*, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult, QueryResult,
    Select, SelectModel, SelectTwo, SelectTwoModel, Selector, SelectorRaw, SelectorTrait,
    TryGetable, TryGetableFromJson,
};

/// Defined a structure to handle aggregate values from a query on a Model
#[derive(Clone, Debug)]
pub struct Aggregator<'db, C, S, T>
where
    C: ConnectionTrait,
    S: SelectorTrait + 'db,
    T: ColumnTrait,
{
    pub(crate) query: SelectStatement,
    pub(crate) db: PhantomData<&'db C>,
    pub(crate) col: T,
    pub(crate) selector: PhantomData<S>,
}

impl<'db, C, S, T> Aggregator<'db, C, S, T>
where
    C: ConnectionTrait,
    S: SelectorTrait + 'db,
    T: ColumnTrait,
{
    pub async fn one<N: TryGetable + From<i64>>(&self, db: &'db C) -> Result<N, DbErr> {
        let builder = db.get_database_backend();
        let stmt = builder.build(&self.query);
        let result = match db.query_one(stmt).await? {
            Some(res) => res,
            None => return Ok(N::from(0)),
        };
        // let sum = match self.col.def().get_column_type() {
        //     ColumnType::Float | ColumnType::Double => result.try_get::<f64>("", "s"),
        //     ColumnType::Integer
        //     | ColumnType::SmallInteger
        //     | ColumnType::BigInteger
        //     | ColumnType::TinyInteger => result.try_get::<i64>("", "s"),
        //     ColumnType::Unsigned
        //     | ColumnType::BigUnsigned
        //     | ColumnType::TinyUnsigned
        //     | ColumnType::SmallUnsigned => result.try_get::<u64>("", "s"),
        //     _ => result.try_get::<f64>("", "s"),
        // };
        let sum = result.try_get::<N>("", "s");
        sum
    }
}

#[async_trait::async_trait]
pub trait AggregatorTrait<'db, C>
where
    C: ConnectionTrait,
{
    type Selector: SelectorTrait + Send + Sync + 'db;

    fn sum<T: ColumnTrait>(self, col: T) -> Aggregator<'db, C, Self::Selector, T>;
    // fn max<T>(self, col: T) -> Aggregator<'db, C, Self::Selector, Self::Res>;
    // fn min<T>(self, col: T) -> Aggregator<'db, C, Self::Selector, Self::Res>;
    // fn avg<T>(self, col: T) -> Aggregator<'db, C, Self::Selector, Self::Res>;
}

impl<'db, C, M, E> AggregatorTrait<'db, C> for Select<E>
where
    C: ConnectionTrait,
    E: EntityTrait<Model = M>,
    M: FromQueryResult + Sized + Send + Sync + 'db,
{
    type Selector = SelectModel<M>;

    fn sum<T>(self, col: T) -> Aggregator<'db, C, Self::Selector, T>
    where
        T: ColumnTrait,
    {
        let query = SelectStatement::new()
            .expr_as(
                Expr::cust(format!("SUM(\"sub_query\".\"{}\")", col.to_string())),
                Alias::new("s"),
            )
            .from_subquery(self.query.clone().to_owned(), Alias::new("sub_query"))
            .to_owned();
        Aggregator {
            query,
            db: PhantomData,
            col,
            selector: PhantomData,
        }
    }
}

#[cfg(test)]
#[cfg(feature = "mock")]
mod tests {
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::entity::prelude::*;
    use crate::{tests_cfg::*, ConnectionTrait, Statement};
    use crate::{DatabaseConnection, DbBackend, MockDatabase, Transaction};

    static RAW_STMT: Lazy<Statement> = Lazy::new(|| {
        Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT "fruit"."id", "fruit"."name", "fruit"."cake_id" FROM "fruit""#,
            [],
        )
    });

    fn setup() -> (DatabaseConnection, Vec<fruit::Model>) {
        let page1 = vec![
            fruit::Model {
                id: 1,
                name: "Blueberry".into(),
                cake_id: Some(1),
            },
            fruit::Model {
                id: 2,
                name: "Rasberry".into(),
                cake_id: Some(1),
            },
            fruit::Model {
                id: 3,
                name: "Strawberry".into(),
                cake_id: Some(2),
            },
        ];

        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([page1.clone()])
            .into_connection();

        (db, page1)
    }

    fn setup_num_items() -> (DatabaseConnection, i64) {
        let num_items = 3;
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[maplit::btreemap! {
                "num" => Into::<Value>::into(num_items),
            }]])
            .into_connection();

        (db, num_items)
    }

    fn setup_sum() -> (DatabaseConnection, i64) {
        let sum = 6;
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([[maplit::btreemap! {
                "s" => Into::<Value>::into(sum),
            }]])
            .into_connection();
        (db, sum)
    }

    #[smol_potat::test]
    async fn count() -> Result<(), DbErr> {
        let (db, sum) = setup_sum();
        let builder = db.get_database_backend();

        let aggregator = fruit::Entity::find().sum(fruit::Column::Id);
        assert_eq!(builder.build(&aggregator.query).to_string(),
            "SELECT SUM(\"sub_query\".\"id\") AS \"s\" FROM (SELECT \"fruit\".\"id\", \"fruit\".\"name\", \"fruit\".\"cake_id\" FROM \"fruit\") AS \"sub_query\"");

        let result = aggregator.one::<i64>(&db).await?;
        assert_eq!(result, sum);
        Ok(())
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
