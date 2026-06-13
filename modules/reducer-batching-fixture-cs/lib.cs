using SpacetimeDB;
using System.Linq;

namespace ReducerBatchingFixture;

public static partial class Module
{
    [SpacetimeDB.Table(Accessor = "table_a")]
    public partial struct TableA(ulong id, ulong val)
    {
        [AutoInc]
        [PrimaryKey]
        public ulong id = id;
        public ulong val = val;
    }

    [SpacetimeDB.Table(Accessor = "table_b")]
    public partial struct TableB(ulong id, ulong val)
    {
        [AutoInc]
        [PrimaryKey]
        public ulong id = id;
        public ulong val = val;
    }

    [SpacetimeDB.Table(Accessor = "table_c")]
    public partial struct TableC(ulong id, ulong val)
    {
        [AutoInc]
        [PrimaryKey]
        public ulong id = id;
        public ulong val = val;
    }

    [SpacetimeDB.Table(Accessor = "table_d")]
    public partial struct TableD(ulong id, ulong val)
    {
        [AutoInc]
        [PrimaryKey]
        public ulong id = id;
        public ulong val = val;
    }

    [SpacetimeDB.Table(Accessor = "table_e")]
    public partial struct TableE(ulong id, ulong val)
    {
        [AutoInc]
        [PrimaryKey]
        public ulong id = id;
        public ulong val = val;
    }

    [SpacetimeDB.Table(Accessor = "table_f")]
    public partial struct TableF(ulong id, ulong val)
    {
        [AutoInc]
        [PrimaryKey]
        public ulong id = id;
        public ulong val = val;
    }

    [SpacetimeDB.Table(Accessor = "table_g")]
    public partial struct TableG(ulong id, ulong val)
    {
        [AutoInc]
        [PrimaryKey]
        public ulong id = id;
        public ulong val = val;
    }

    [SpacetimeDB.Reducer]
    public static void heavy_a(ReducerContext ctx, ulong n)
    {
        for (ulong i = 0; i < n; i++)
            ctx.Db.table_a.Insert(new(0, i));
    }

    [SpacetimeDB.Reducer]
    public static void heavy_b(ReducerContext ctx, ulong n)
    {
        for (ulong i = 0; i < n; i++)
            ctx.Db.table_b.Insert(new(0, i));
    }

    [SpacetimeDB.Reducer]
    public static void cheap_a(ReducerContext ctx)
    {
        ctx.Db.table_a.Insert(new(0, 0));
    }

    [SpacetimeDB.Reducer]
    public static void cheap_c(ReducerContext ctx)
    {
        ctx.Db.table_c.Insert(new(0, 0));
    }

    [SpacetimeDB.Reducer]
    public static void cheap_d(ReducerContext ctx)
    {
        ctx.Db.table_d.Insert(new(0, 0));
    }

    [SpacetimeDB.Reducer]
    public static void cheap_e(ReducerContext ctx)
    {
        ctx.Db.table_e.Insert(new(0, 0));
    }

    [SpacetimeDB.Reducer]
    public static void cheap_f(ReducerContext ctx)
    {
        ctx.Db.table_f.Insert(new(0, 0));
    }

    [SpacetimeDB.Reducer]
    public static void cheap_g(ReducerContext ctx)
    {
        ctx.Db.table_g.Insert(new(0, 0));
    }

    [SpacetimeDB.Reducer]
    public static void log_counts(ReducerContext ctx)
    {
        ulong a = (ulong)ctx.Db.table_a.Iter().Count();
        ulong b = (ulong)ctx.Db.table_b.Iter().Count();
        ulong c = (ulong)ctx.Db.table_c.Iter().Count();
        ulong d = (ulong)ctx.Db.table_d.Iter().Count();
        ulong e = (ulong)ctx.Db.table_e.Iter().Count();
        ulong f = (ulong)ctx.Db.table_f.Iter().Count();
        ulong g = (ulong)ctx.Db.table_g.Iter().Count();
        Log.Info($"counts a={a} b={b} c={c} d={d} e={e} f={f} g={g}");
    }
}
