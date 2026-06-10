use spacetimedb::{ReducerContext, Table};

#[spacetimedb::table(accessor = a)]
pub struct TableA {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub v: u64,
}

#[spacetimedb::table(accessor = b)]
pub struct TableB {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub v: u64,
}

#[spacetimedb::table(accessor = c)]
pub struct TableC {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub v: u64,
}

#[spacetimedb::table(accessor = d)]
pub struct TableD {
    #[auto_inc]
    #[primary_key]
    pub id: u64,
    pub v: u64,
}

#[spacetimedb::reducer]
pub fn write_a(ctx: &ReducerContext, v: u64) {
    ctx.db.a().insert(TableA { id: 0, v });
}

#[spacetimedb::reducer]
pub fn write_b(ctx: &ReducerContext, v: u64) {
    ctx.db.b().insert(TableB { id: 0, v });
}

#[spacetimedb::reducer]
pub fn heavy_a(ctx: &ReducerContext, n: u32) {
    for i in 0..n {
        ctx.db.a().insert(TableA { id: 0, v: i as u64 });
    }
    let sum: u64 = ctx.db.a().iter().map(|row| row.v).sum();
    log::info!("heavy_a sum={sum}");
}

#[spacetimedb::reducer]
pub fn heavy_b(ctx: &ReducerContext, n: u32) {
    for i in 0..n {
        ctx.db.b().insert(TableB { id: 0, v: i as u64 });
    }
    let sum: u64 = ctx.db.b().iter().map(|row| row.v).sum();
    log::info!("heavy_b sum={sum}");
}

#[spacetimedb::reducer]
pub fn write_c_1(ctx: &ReducerContext, v: u64) {
    ctx.db.c().insert(TableC { id: 0, v });
}

#[spacetimedb::reducer]
pub fn write_c_2(ctx: &ReducerContext, v: u64) {
    ctx.db.c().insert(TableC { id: 0, v });
}

#[spacetimedb::reducer]
pub fn read_c(ctx: &ReducerContext) {
    let count = ctx.db.c().iter().count();
    log::info!("read_c count={count}");
}

#[spacetimedb::reducer]
pub fn read_d_1(ctx: &ReducerContext) {
    let count = ctx.db.d().iter().count();
    log::info!("read_d_1 count={count}");
}

#[spacetimedb::reducer]
pub fn read_d_2(ctx: &ReducerContext) {
    let count = ctx.db.d().iter().count();
    log::info!("read_d_2 count={count}");
}

fn touch_a(ctx: &ReducerContext) {
    ctx.db.a().insert(TableA { id: 0, v: 1 });
}

fn touch_b(ctx: &ReducerContext) {
    ctx.db.b().insert(TableB { id: 0, v: 1 });
}

// The fn-pointer array prevents LLVM/wasm-opt from devirtualizing the dispatch,
// forcing call_indirect with table-relevant targets — the analyzer must wildcard this.
static FNS: [fn(&ReducerContext); 2] = [touch_a, touch_b];

#[spacetimedb::reducer]
pub fn indirect_touch(ctx: &ReducerContext, flag: bool) {
    FNS[flag as usize](ctx);
}
