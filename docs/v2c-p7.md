# V2c P7 — GC, PITR, and branching over columnar fragments

*The second half of the V2c research gate. The first half — byte-placement-exact
heap-page reconstruction (P5/P6) — is done and live-verified (`src/reconstruct.rs`,
`examples/rebuild.rs`, checksum + datums confirmed against real PG). This is the
other half: once heap pages are demoted, how do reads-at-LSN, garbage
collection, and branching work when the canonical state is columnar fragments
instead of layer files? Prototype: `src/timetravel.rs` (pure functions, pinned
in tests).*

## 1. The setup

After V2b, the canonical materialization of a relation is a set of **fragments**
— `(rel, key_range, lsn)` columnar files, each holding the visible rows of a
block range as of one LSN, with per-row `(block, offnum)` (the Databricks
layout; see `reconstruct`/`fragment`). Above each fragment sits the **delta
tail** — the WAL/change records for LSNs greater than the fragment's, which is
exactly V2a's commit-ordered log (and the M4 freshness tail). A page at an LSN
is *reconstructed*, not stored: pick the base fragment ≤ the LSN, roll the tail
forward, hand the result to `reconstruct::build_page`.

The pageserver answers three things from layer files today that must now be
answered from fragments + tail. Getting the **policy** wrong deletes data that
is still needed, so it is modeled as pure functions and pinned in tests before
it ever drives real GC.

## 2. page@LSN reads (`Lake::resolve`)

To serve `(rel, block, read_lsn)`:

1. **base** = the newest fragment covering `(rel, block)` with `lsn ≤ read_lsn`.
2. **serveable** iff `base.lsn == read_lsn` (no tail needed) **or**
   `base.lsn ≥ tail_floor` (the tail records `(base.lsn, read_lsn]` still
   exist). Otherwise the read is **below the retention horizon** — the records
   needed to roll the base forward have been GC'd.
3. No fragment covers the page at all → **uncovered** (distinct from
   below-horizon: covered only by *newer* fragments is below-horizon).

The result is a plan `Serve { base_lsn, read_lsn }`; the caller reads the base
fragment's rows, applies tail records in `(base_lsn, read_lsn]`, and rebuilds.

## 3. PITR

PITR falls out for free: a read at a past LSN **is** the same `resolve`. Fragment
metadata already carries `lsn`, so "time-travel to LSN L" = resolve every page
at L. The retention window is expressed as `tail_floor` (how far back the delta
tail is kept) plus a fragment at or below the window's oldest LSN. No separate
snapshot machinery — the lake format's LSN addressing *is* the time axis.

## 4. GC (`Lake::image_redundant`)

The V2c GC gate flips from "is this page covered by an **image layer**" to "is
it covered by a **fragment**". A heap image layer for `(rel, [start,end))` is
redundant — safe to drop — iff **every** block in the range is covered by a
fragment at an LSN in `[tail_floor, gc_horizon]`. That fragment plus the
retained tail reconstructs any read `≥ gc_horizon`, so the heap image is no
longer the source of record.

Delta tail records are kept down to `gc_horizon` (= `tail_floor` in steady
state); fragments below the horizon that a newer fragment supersedes can be
compacted away by the same coverage argument. The result: heap layers stop
being uploaded (V2c proper), and storage at rest is fragments + a bounded tail —
image layers, which dominate bytes today, are gone.

## 5. Branching (`Branch`)

A branch is copy-on-write into the ancestor's fragment stack — the same shape
Neon branches use over layer files, re-expressed over fragments:

- A branch has a `branch_lsn` and its own `Lake` for writes above it.
- **Read ≤ branch_lsn** → the parent's state (inherited history).
- **Read > branch_lsn** → the branch's own fragment wins for pages it rewrote;
  a page the branch never touched inherits the parent's state **frozen at
  `branch_lsn`**.
- **GC coupling**: a branch pins the parent's history at its branch point, so
  the parent's effective GC horizon can never advance past the oldest branch
  point (`effective_gc_horizon = min(configured, min branch_lsn)`). This is the
  load-bearing safety invariant — without it, GC on the parent silently breaks
  a branch.

## 6. Delta vs Iceberg — the format decision

**Finding: branching does not require Iceberg.** The branch model above is
*logical* — it resolves reads over (parent fragments ≤ branch_lsn) + (branch's
own fragments) with no storage-format branch primitive. It works over Delta
today: a branch is a separate fragment set + a recorded `(parent, branch_lsn)`,
and `resolve` does the merge. So V2c can ship branching on the existing Delta
sink.

**Where Iceberg still wins** (an optimization, not a prerequisite): Iceberg has
first-class **branches and tags** in table metadata, so a branch/tag could be a
native catalog reference into the parent's snapshots instead of an out-of-band
`(parent, branch_lsn)` record the transcoder tracks itself — cleaner metadata,
engine-visible time travel, and tag-based PITR points. The sink was kept thin
(`sink.rs` note: "so an Iceberg backend could be added later") precisely for
this. Recommendation: **build V2c on Delta with the logical branch model; add an
Iceberg backend when native branch/tag metadata is worth the second sink** — it
does not block the research gate.

## 7. Prototype status & what's fork-side

`src/timetravel.rs` pins the policy: `Lake::resolve` (reads), `image_redundant`
(GC gate), `Branch::resolve` (branch reads), `effective_gc_horizon` (branch/GC
coupling). 7 tests cover base selection, below-horizon vs uncovered, the
exact-LSN no-tail case, the GC window (including the below-`tail_floor` and
partial-coverage negatives), branch inherit/diverge, and horizon pinning.

**Fork-side (needs the neon stack, not built here):**
- the real fragment **index** (the tee writes `(rel, key_range, lsn)`; this
  needs to be queryable at GC/read time — Delta commit metadata or a side
  index);
- wiring `image_redundant` into the pageserver's **GC loop** and `resolve` into
  the **GetPage/read path** (with `reconstruct` doing the rebuild);
- the delta-tail retention actually tracking `tail_floor`.

**Open refinements (noted, not blocking the gate):**
- a branch read above `branch_lsn` on a rewritten page whose first branch
  fragment is *newer* than the read must roll forward from the **parent** at
  `branch_lsn` + the branch's tail — the prototype resolves to the parent-frozen
  base in that gap, which is correct-but-conservative; a full model threads the
  branch tail onto a parent base;
- multi-level branch trees (branch of a branch) compose `Branch` recursively;
- key-range splits/merges as fragments are recompacted (coverage is by block,
  so splits are transparent; merges need overlap handling).

Together with `reconstruct` (P5/P6), this closes the V2c research gate on paper
and in prototype: **fragments + tail → any page@LSN, GC by fragment coverage,
branches by CoW — no heap layers, no Iceberg dependency.**
