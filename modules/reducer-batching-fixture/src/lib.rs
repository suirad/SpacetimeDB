use spacetimedb::{ReducerContext, Table};

// Two independent tables so heavy_a and heavy_b are always disjoint and
// therefore batchable against each other by the admission predicate.

#[spacetimedb::table(accessor = table_a, public)]
pub struct TableA {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub val: u64,
}

#[spacetimedb::table(accessor = table_b, public)]
pub struct TableB {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub val: u64,
}

#[spacetimedb::table(accessor = table_c, public)]
pub struct TableC {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub val: u64,
}

#[spacetimedb::table(accessor = table_d, public)]
pub struct TableD {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub val: u64,
}

#[spacetimedb::table(accessor = table_e, public)]
pub struct TableE {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub val: u64,
}

#[spacetimedb::table(accessor = table_f, public)]
pub struct TableF {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub val: u64,
}

#[spacetimedb::table(accessor = table_g, public)]
pub struct TableG {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub val: u64,
}

/// Insert `n` rows into table `a`. Use n≥5000 in tests so the measured runtime
/// sits clearly above the fork threshold even in debug builds (~5–15 ms).
#[spacetimedb::reducer]
pub fn heavy_a(ctx: &ReducerContext, n: u64) {
    for i in 0..n {
        ctx.db.table_a().insert(TableA { id: 0, val: i });
    }
}

/// Insert `n` rows into table `b`. Disjoint from heavy_a — the canonical fork
/// pair for scheduler batching tests.
#[spacetimedb::reducer]
pub fn heavy_b(ctx: &ReducerContext, n: u64) {
    for i in 0..n {
        ctx.db.table_b().insert(TableB { id: 0, val: i });
    }
}

/// Insert exactly one row into table `a`. Cheap enough to stay below the fork
/// threshold; used to confirm cheap reducers never spin up the pool.
#[spacetimedb::reducer]
pub fn cheap_a(ctx: &ReducerContext) {
    ctx.db.table_a().insert(TableA { id: 0, val: 0 });
}

/// Single-insert reducers for tables c–g. Used in the mixed phase to exercise
/// prefix truncation: same-table writers self-conflict when submitted concurrently.
#[spacetimedb::reducer]
pub fn cheap_c(ctx: &ReducerContext) {
    ctx.db.table_c().insert(TableC { id: 0, val: 0 });
}

#[spacetimedb::reducer]
pub fn cheap_d(ctx: &ReducerContext) {
    ctx.db.table_d().insert(TableD { id: 0, val: 0 });
}

#[spacetimedb::reducer]
pub fn cheap_e(ctx: &ReducerContext) {
    ctx.db.table_e().insert(TableE { id: 0, val: 0 });
}

#[spacetimedb::reducer]
pub fn cheap_f(ctx: &ReducerContext) {
    ctx.db.table_f().insert(TableF { id: 0, val: 0 });
}

#[spacetimedb::reducer]
pub fn cheap_g(ctx: &ReducerContext) {
    ctx.db.table_g().insert(TableG { id: 0, val: 0 });
}

/// Insert `n` rows into `a` then panic. Verifies that a worker trap does not
/// brick the pool — subsequent batch pairs should still succeed.
#[spacetimedb::reducer]
pub fn heavy_panic(ctx: &ReducerContext, n: u64) {
    for i in 0..n {
        ctx.db.table_a().insert(TableA { id: 0, val: i });
    }
    panic!("intentional reducer panic for trap-recovery test");
}

/// Log the current row counts for all seven tables so tests can assert DB
/// consistency without a SQL helper. Format keeps `a=X b=Y` as a prefix so
/// existing two-field parsers still work; c–g follow.
#[spacetimedb::reducer]
pub fn log_counts(ctx: &ReducerContext) {
    let a = ctx.db.table_a().count();
    let b = ctx.db.table_b().count();
    let c = ctx.db.table_c().count();
    let d = ctx.db.table_d().count();
    let e = ctx.db.table_e().count();
    let f = ctx.db.table_f().count();
    let g = ctx.db.table_g().count();
    log::info!("counts a={a} b={b} c={c} d={d} e={e} f={f} g={g}");
}
