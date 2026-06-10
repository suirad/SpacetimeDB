use crate::analyze::AccessSet;

/// Pairwise conflict matrix for a fixed set of reducers.
///
/// Bit (i, j) is set iff reducer i and reducer j have conflicting access sets.
/// read∩read is not a conflict — two readers never block each other.
pub struct ConflictMatrix {
    n: usize,
    bits: Vec<u64>,
}

impl ConflictMatrix {
    /// Build the conflict matrix for `sets`.
    pub fn build(sets: &[AccessSet]) -> ConflictMatrix {
        let n = sets.len();
        let total_bits = n.checked_mul(n).expect("reducer count overflow");
        let words = total_bits.div_ceil(64);
        let mut bits = vec![0u64; words];

        for i in 0..n {
            for j in i..n {
                if conflicts_pair(&sets[i], &sets[j]) {
                    set_bit(&mut bits, i * n + j);
                    set_bit(&mut bits, j * n + i);
                }
            }
        }

        ConflictMatrix { n, bits }
    }

    /// O(1) conflict test.
    pub fn conflicts(&self, i: usize, j: usize) -> bool {
        assert!(i < self.n && j < self.n, "index out of bounds");
        let idx = i * self.n + j;
        (self.bits[idx / 64] >> (idx % 64)) & 1 == 1
    }

    /// Number of reducers in the matrix.
    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }
}

fn set_bit(bits: &mut [u64], idx: usize) {
    bits[idx / 64] |= 1u64 << (idx % 64);
}

fn conflicts_pair(a: &AccessSet, b: &AccessSet) -> bool {
    a.wildcard
        || b.wildcard
        || !a.writes.is_disjoint(&b.reads)
        || !a.writes.is_disjoint(&b.writes)
        || !b.writes.is_disjoint(&a.reads)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::map::BTreeSet;
    use spacetimedb_schema::identifier::Identifier;

    fn id(name: &str) -> Identifier {
        Identifier::for_test(name)
    }

    fn reads(tables: &[&str]) -> AccessSet {
        AccessSet {
            reads: tables.iter().map(|s| id(s)).collect::<BTreeSet<_>>(),
            writes: BTreeSet::new(),
            wildcard: false,
        }
    }

    fn writes(tables: &[&str]) -> AccessSet {
        AccessSet {
            reads: BTreeSet::new(),
            writes: tables.iter().map(|s| id(s)).collect::<BTreeSet<_>>(),
            wildcard: false,
        }
    }

    #[allow(dead_code)]
    fn reads_writes(r: &[&str], w: &[&str]) -> AccessSet {
        AccessSet {
            reads: r.iter().map(|s| id(s)).collect::<BTreeSet<_>>(),
            writes: w.iter().map(|s| id(s)).collect::<BTreeSet<_>>(),
            wildcard: false,
        }
    }

    fn wildcard() -> AccessSet {
        AccessSet {
            reads: BTreeSet::new(),
            writes: BTreeSet::new(),
            wildcard: true,
        }
    }

    #[test]
    fn two_readers_no_conflict() {
        let sets = [reads(&["tbl"]), reads(&["tbl"])];
        let m = ConflictMatrix::build(&sets);
        assert!(!m.conflicts(0, 1));
        assert!(!m.conflicts(1, 0));
    }

    #[test]
    fn writer_vs_reader_conflict_symmetric() {
        let sets = [writes(&["tbl"]), reads(&["tbl"])];
        let m = ConflictMatrix::build(&sets);
        assert!(m.conflicts(0, 1));
        assert!(m.conflicts(1, 0));
    }

    #[test]
    fn writer_vs_writer_conflict() {
        let sets = [writes(&["tbl"]), writes(&["tbl"])];
        let m = ConflictMatrix::build(&sets);
        assert!(m.conflicts(0, 1));
        assert!(m.conflicts(1, 0));
    }

    #[test]
    fn wildcard_conflicts_with_everything_including_itself() {
        let sets = [wildcard(), reads(&["tbl"]), writes(&["other"])];
        let m = ConflictMatrix::build(&sets);
        assert!(m.conflicts(0, 0));
        assert!(m.conflicts(0, 1));
        assert!(m.conflicts(0, 2));
        assert!(m.conflicts(1, 0));
        assert!(m.conflicts(2, 0));
    }

    #[test]
    fn disjoint_write_sets_no_conflict() {
        let sets = [writes(&["a"]), writes(&["b"])];
        let m = ConflictMatrix::build(&sets);
        assert!(!m.conflicts(0, 1));
        assert!(!m.conflicts(1, 0));
    }

    #[test]
    fn writer_self_conflicts_pure_reader_does_not() {
        // Writer: writes∩writes is nonempty → two invocations conflict.
        let writer = writes(&["a"]);
        // Pure reader: no writes → no self-conflict.
        let reader = reads(&["a"]);
        let sets = [writer, reader];
        let m = ConflictMatrix::build(&sets);
        assert!(m.conflicts(0, 0), "writer should self-conflict");
        assert!(!m.conflicts(1, 1), "pure reader should not self-conflict");
    }
}
