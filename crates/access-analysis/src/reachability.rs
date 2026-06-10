//! Direct-call graph + reachability, with constrained `call_indirect` handling.
//!
//! We follow only DIRECT `call` edges from each reducer body. A `call_indirect`
//! is constrained to (element-segment targets ∩ type-signature): if any possible
//! target is table-relevant we wildcard, otherwise it's ignored — so fmt/panic/log
//! vtables don't wildcard everything.

use walrus::ir::{Instr, InstrSeqId};
use walrus::{FunctionId, LocalFunction, Module, TypeId};

use crate::imports::ImportTable;
use crate::map::{HashMap, HashSet};

/// A direct-call graph over the module's functions.
pub struct CallGraph {
    callees: HashMap<FunctionId, HashSet<FunctionId>>,
    callers: HashMap<FunctionId, HashSet<FunctionId>>,
}

impl CallGraph {
    pub fn build(module: &Module) -> Self {
        let mut callees: HashMap<FunctionId, HashSet<FunctionId>> = HashMap::new();
        let mut callers: HashMap<FunctionId, HashSet<FunctionId>> = HashMap::new();
        for (fid, lf) in module.funcs.iter_local() {
            let mut set = HashSet::new();
            for_each_instr(lf, |instr| {
                if let Instr::Call(c) = instr {
                    set.insert(c.func);
                }
            });
            for callee in &set {
                callers.entry(*callee).or_default().insert(fid);
            }
            callees.insert(fid, set);
        }
        CallGraph { callees, callers }
    }

    pub fn callees_of(&self, func: FunctionId) -> impl Iterator<Item = FunctionId> + '_ {
        self.callees.get(&func).into_iter().flatten().copied()
    }

    /// Forward-reachable closure (including the seeds) over direct call edges.
    pub fn forward_closure(&self, seeds: impl IntoIterator<Item = FunctionId>) -> HashSet<FunctionId> {
        let mut stack: Vec<FunctionId> = seeds.into_iter().collect();
        let mut seen: HashSet<FunctionId> = stack.iter().copied().collect();
        while let Some(f) = stack.pop() {
            for callee in self.callees_of(f) {
                if seen.insert(callee) {
                    stack.push(callee);
                }
            }
        }
        seen
    }

    /// Reverse-reachable closure: every function that can reach any of `seeds`
    /// via direct calls, including the seeds themselves.
    pub fn callers_closure(&self, seeds: &HashSet<FunctionId>) -> HashSet<FunctionId> {
        let mut stack: Vec<FunctionId> = seeds.iter().copied().collect();
        let mut seen: HashSet<FunctionId> = seeds.clone();
        while let Some(f) = stack.pop() {
            if let Some(callers) = self.callers.get(&f) {
                for caller in callers {
                    if seen.insert(*caller) {
                        stack.push(*caller);
                    }
                }
            }
        }
        seen
    }
}

/// The set of function ids any `call_indirect` could dispatch to: the union of
/// every element segment's function targets (including passive/declared ones,
/// reachable via `table.init`, to stay sound), intersected per call-site with
/// the indirect call's type-signature.
pub struct IndirectTargets {
    all: HashSet<FunctionId>,
}

impl IndirectTargets {
    pub fn build(module: &Module) -> Self {
        let mut all = HashSet::new();
        for elem in module.elements.iter() {
            match &elem.items {
                walrus::ElementItems::Functions(funcs) => {
                    all.extend(funcs.iter().copied());
                }
                walrus::ElementItems::Expressions(_, exprs) => {
                    for e in exprs {
                        if let walrus::ConstExpr::RefFunc(f) = e {
                            all.insert(*f);
                        }
                    }
                }
            }
        }
        IndirectTargets { all }
    }

    /// Possible targets of a `call_indirect` of type `ty`.
    pub fn possible_for(&self, module: &Module, ty: TypeId) -> Vec<FunctionId> {
        let (wp, wr) = module.types.params_results(ty);
        self.all
            .iter()
            .copied()
            .filter(|f| {
                let fty = module.funcs.get(*f).ty();
                let (p, r) = module.types.params_results(fty);
                p == wp && r == wr
            })
            .collect()
    }
}

/// Visit every instruction of a local function (descending into all nested
/// instruction sequences) exactly once.
pub fn for_each_instr(lf: &LocalFunction, mut f: impl FnMut(&Instr)) {
    let mut stack: Vec<InstrSeqId> = vec![lf.entry_block()];
    let mut visited: HashSet<InstrSeqId> = HashSet::new();
    while let Some(seq_id) = stack.pop() {
        if !visited.insert(seq_id) {
            continue;
        }
        for (instr, _) in &lf.block(seq_id).instrs {
            f(instr);
            match instr {
                Instr::Block(b) => stack.push(b.seq),
                Instr::Loop(l) => stack.push(l.seq),
                Instr::IfElse(ie) => {
                    stack.push(ie.consequent);
                    stack.push(ie.alternative);
                }
                _ => {}
            }
        }
    }
}

/// Compute the set of "table-relevant" functions: every function that can
/// transitively (via direct calls) reach a table op, a name resolver, an
/// unknown `spacetime_*` import, or a dangerous host op.
pub fn table_relevant_funcs(module: &Module, imports: &ImportTable, graph: &CallGraph) -> HashSet<FunctionId> {
    let mut seeds = HashSet::new();
    for func in module.funcs.iter() {
        if imports.is_table_relevant_import(func.id()) {
            seeds.insert(func.id());
        }
    }
    graph.callers_closure(&seeds)
}
