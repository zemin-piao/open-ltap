//! V2c P7 — time-travel reads, GC gating, and branching over columnar
//! fragments. This is the *second* half of the V2c research gate (the reverse
//! path, `reconstruct`, is the first). Once heap pages are demoted and the
//! canonical state is columnar fragments + a delta-record tail, three questions
//! that the pageserver answers today via layer files must be re-answered over
//! fragments:
//!
//!   * **page@LSN reads** — which fragment is the base and which tail records
//!     roll it forward to the read LSN ([`Lake::resolve`]);
//!   * **GC** — when is an old heap image layer redundant, i.e. rebuildable
//!     from a fragment + the retained tail ([`Lake::image_redundant`]);
//!   * **PITR / branching** — a read at a past LSN, and a branch that inherits
//!     the parent's history frozen at its branch point ([`Branch`]).
//!
//! Modeled as pure functions over fragment metadata (`(rel, key_range, lsn)` —
//! exactly what V2b's tee writes) so the *policy* is pinned in tests before it
//! ever drives real pageserver GC (where a wrong answer deletes data that's
//! still needed). See `docs/v2c-p7.md` for the design write-up, including why
//! the branch model is storage-agnostic (works on Delta today; Iceberg's native
//! branch/tag model is an optimization, not a prerequisite).

pub type Lsn = u64;

/// A page range of one relation materialized as a columnar fragment at `lsn` —
/// the `(rel, key_range, lsn)` metadata V2b emits. `blocks` is `[start, end)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fragment {
    pub rel: u32,
    pub start: u32,
    pub end: u32,
    pub lsn: Lsn,
}

impl Fragment {
    fn covers(&self, rel: u32, block: u32) -> bool {
        self.rel == rel && block >= self.start && block < self.end
    }
}

/// How a `(rel, block, read_lsn)` request resolves against a fragment set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resolve {
    /// Rebuild the page from the fragment at `base_lsn`, rolling the delta tail
    /// `(base_lsn, read_lsn]` forward. `base_lsn == read_lsn` means no tail.
    Serve { base_lsn: Lsn, read_lsn: Lsn },
    /// `read_lsn` predates retention — the tail needed to reach it is GC'd.
    BelowHorizon,
    /// No fragment covers this page.
    Uncovered,
}

/// One timeline's fragment index plus its retained-tail floor. Delta/change
/// records below `tail_floor` have been GC'd; `tip` is the latest applied LSN.
#[derive(Clone, Debug, Default)]
pub struct Lake {
    pub fragments: Vec<Fragment>,
    pub tail_floor: Lsn,
    pub tip: Lsn,
}

impl Lake {
    /// The newest fragment covering `(rel, block)` at an LSN ≤ `read_lsn` — the
    /// base a read rolls forward from.
    fn base(&self, rel: u32, block: u32, read_lsn: Lsn) -> Option<Fragment> {
        self.fragments
            .iter()
            .copied()
            .filter(|f| f.covers(rel, block) && f.lsn <= read_lsn)
            .max_by_key(|f| f.lsn)
    }

    /// Resolve a page@LSN read. A read is serveable when a base fragment exists
    /// at `base_lsn ≤ read_lsn` AND the tail `(base_lsn, read_lsn]` still exists
    /// (i.e. `base_lsn ≥ tail_floor`, or the read lands exactly on the fragment
    /// so no tail is needed).
    pub fn resolve(&self, rel: u32, block: u32, read_lsn: Lsn) -> Resolve {
        match self.base(rel, block, read_lsn) {
            Some(f) if f.lsn == read_lsn || f.lsn >= self.tail_floor => {
                Resolve::Serve { base_lsn: f.lsn, read_lsn }
            }
            // The best base is older than the retained tail: the records needed
            // to roll it forward to read_lsn are gone.
            Some(_) => Resolve::BelowHorizon,
            // Covered only by fragments newer than read_lsn → the state at
            // read_lsn is below retention.
            None if self.fragments.iter().any(|f| f.covers(rel, block)) => Resolve::BelowHorizon,
            None => Resolve::Uncovered,
        }
    }

    /// Is a heap image layer for `(rel, [start, end))` redundant — safe to GC —
    /// given we must keep serving reads at LSN ≥ `gc_horizon`? Yes iff every
    /// block is covered by a fragment at an LSN in `[tail_floor, gc_horizon]`:
    /// that fragment plus the retained tail reconstructs any read ≥ gc_horizon,
    /// so the heap image is no longer the source of record. This is the V2c GC
    /// gate — coverage by fragments, not by image layers.
    pub fn image_redundant(&self, rel: u32, start: u32, end: u32, gc_horizon: Lsn) -> bool {
        (start..end).all(|b| {
            self.fragments
                .iter()
                .any(|f| f.covers(rel, b) && f.lsn <= gc_horizon && f.lsn >= self.tail_floor)
        })
    }
}

/// A branch: it inherits the parent's history frozen at `branch_lsn`, and its
/// own writes live above that in `own`. (Copy-on-write into the ancestor's
/// fragment stack, the same shape Neon branches use over layer files.)
#[derive(Clone, Debug)]
pub struct Branch<'a> {
    pub parent: &'a Lake,
    pub branch_lsn: Lsn,
    pub own: Lake,
}

impl Branch<'_> {
    /// Resolve a read on the branch. At or below the branch point the state is
    /// the parent's. Above it, the branch's own fragments win for pages it has
    /// rewritten; pages it never touched inherit the parent's state *frozen at
    /// the branch point*.
    pub fn resolve(&self, rel: u32, block: u32, read_lsn: Lsn) -> Resolve {
        if read_lsn <= self.branch_lsn {
            return self.parent.resolve(rel, block, read_lsn);
        }
        match self.own.resolve(rel, block, read_lsn) {
            Resolve::Uncovered => self.parent.resolve(rel, block, self.branch_lsn),
            other => other,
        }
    }
}

/// The effective GC horizon of a parent timeline: it can never advance past the
/// oldest branch point, because each branch pins the parent's history there.
/// (`configured` is the PITR-window horizon; branches only ever hold it back.)
pub fn effective_gc_horizon(configured: Lsn, branch_points: &[Lsn]) -> Lsn {
    branch_points.iter().copied().fold(configured, Lsn::min)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frag(rel: u32, start: u32, end: u32, lsn: Lsn) -> Fragment {
        Fragment { rel, start, end, lsn }
    }

    fn lake() -> Lake {
        // rel 1, blocks [0,10) fragmented at LSN 100 and again at 300;
        // blocks [10,20) only at LSN 200. Tail retained from 150; tip 400.
        Lake {
            fragments: vec![frag(1, 0, 10, 100), frag(1, 0, 10, 300), frag(1, 10, 20, 200)],
            tail_floor: 150,
            tip: 400,
        }
    }

    #[test]
    fn resolve_picks_newest_base_below_read_and_rolls_tail() {
        // read block 5 @ 350: newest fragment ≤ 350 is the one at 300.
        assert_eq!(lake().resolve(1, 5, 350), Resolve::Serve { base_lsn: 300, read_lsn: 350 });
    }

    #[test]
    fn resolve_below_horizon_when_tail_is_gone() {
        // read block 5 @ 120: best base is 100, but tail_floor is 150, so the
        // records (100, 120] needed to roll forward are GC'd.
        assert_eq!(lake().resolve(1, 5, 120), Resolve::BelowHorizon);
    }

    #[test]
    fn resolve_exact_fragment_lsn_needs_no_tail() {
        // read block 5 @ 100 lands exactly on the fragment — serveable even
        // though 100 < tail_floor, because no roll-forward is required.
        assert_eq!(lake().resolve(1, 5, 100), Resolve::Serve { base_lsn: 100, read_lsn: 100 });
    }

    #[test]
    fn resolve_uncovered_vs_below_horizon() {
        // block 50 has no fragment at all.
        assert_eq!(lake().resolve(1, 50, 350), Resolve::Uncovered);
        // block 12 is covered only at LSN 200; a read @ 180 is below that.
        assert_eq!(lake().resolve(1, 12, 180), Resolve::BelowHorizon);
    }

    #[test]
    fn gc_gate_needs_full_coverage_in_window() {
        let l = lake();
        // blocks [0,10) have a fragment at 300 (in [tail_floor=150, horizon=350])
        // → the older image layer is redundant.
        assert!(l.image_redundant(1, 0, 10, 350));
        // With horizon 250, the only in-window fragment for [0,10) is at 100,
        // which is below tail_floor (150) → NOT redundant (can't roll forward).
        assert!(!l.image_redundant(1, 0, 10, 250));
        // A range straddling covered [0,10) and uncovered [10,30) isn't fully
        // covered → not redundant.
        assert!(!l.image_redundant(1, 0, 30, 350));
    }

    #[test]
    fn branch_inherits_below_and_diverges_above() {
        let parent = lake();
        // Branch at 300; the branch re-fragments block 5 at LSN 500.
        let own = Lake { fragments: vec![frag(1, 5, 6, 500)], tail_floor: 300, tip: 600 };
        let b = Branch { parent: &parent, branch_lsn: 300, own };

        // Below the branch point: parent's state.
        assert_eq!(b.resolve(1, 5, 200), parent.resolve(1, 5, 200));
        // Above, on a page the branch rewrote: the branch's own fragment.
        assert_eq!(b.resolve(1, 5, 550), Resolve::Serve { base_lsn: 500, read_lsn: 550 });
        // Above, on a page the branch never touched (block 8): inherit the
        // parent frozen at the branch point (300).
        assert_eq!(b.resolve(1, 8, 550), Resolve::Serve { base_lsn: 300, read_lsn: 300 });
    }

    #[test]
    fn gc_horizon_held_back_by_branches() {
        // Configured PITR horizon 400, but branches at 250 and 320 pin history.
        assert_eq!(effective_gc_horizon(400, &[250, 320]), 250);
        // No branches → the configured horizon stands.
        assert_eq!(effective_gc_horizon(400, &[]), 400);
    }
}
