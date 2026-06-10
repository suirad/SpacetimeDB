//! Minimal 2-table fixture whose reducers exercise every read/write split for
//! the access-set analyzer tests.

use spacetimedb::{ReducerContext, Table};

#[spacetimedb::table(accessor = a)]
pub struct TableA {
    #[primary_key]
    pub id: u64,
    pub value: u64,
}

#[spacetimedb::table(accessor = b)]
pub struct TableB {
    #[primary_key]
    pub id: u64,
    pub value: u64,
}

#[spacetimedb::reducer]
pub fn writes_a(ctx: &ReducerContext) {
    ctx.db.a().insert(TableA { id: 1, value: 1 });
}

#[spacetimedb::reducer]
pub fn reads_b(ctx: &ReducerContext) {
    let found = ctx.db.b().id().find(1u64);
    assert!(found.is_none() || found.unwrap().id == 1);
}

#[spacetimedb::reducer]
pub fn reads_a_writes_b(ctx: &ReducerContext) {
    let total: u64 = ctx.db.a().iter().map(|row| row.value).sum();
    ctx.db.b().insert(TableB { id: 2, value: total });
}

#[spacetimedb::reducer]
pub fn touches_both(ctx: &ReducerContext) {
    let a_sum: u64 = ctx.db.a().iter().map(|row| row.value).sum();
    let b_sum: u64 = ctx.db.b().iter().map(|row| row.value).sum();
    ctx.db.a().insert(TableA { id: 10, value: b_sum });
    ctx.db.b().insert(TableB { id: 10, value: a_sum });
}
