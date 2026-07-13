# The OLTP hot tier — point reads/writes over columnar

> ## ⏸ PARKED — 2026-07-13
> **This track is descoped, not pursued.** The roadmap's primary path to the North Star
> ("columnar as the only canonical materialization") is `docs/v2-scope.md`'s **P0 → V2a → V2b
> → V2c** axis — transcode at the storage layer, Postgres's own heap pages demoted to a
> rebuildable cache. Databricks' shipped LTAP architecture validated that this axis alone
> reaches "one copy, OLTP and OLAP both served," with no separate lake-native point interface
> of the kind designed below (see `docs/v2-scope.md` §1's prior-art note, added 2026-07-13).
> This document is kept **for reference only** — a design register in case real need for a
> lake-native PK-addressed interface emerges later — and is explicitly **not** part of the
> critical path. Nothing below has been implemented; nothing below is currently planned to be.
> See `docs/v2-scope.md` §5 P10 for the one-paragraph rationale in context.

*Status: design document, no code. Written 2026-07-13; parked the same day (see banner above).
Companion to `docs/v2-scope.md` P10
("The OLTP hot tier: point reads/writes directly over columnar"), which this doc expands.
Grounded entirely by reading the current codebase (`src/txbuf.rs`, `src/engine.rs`,
`src/sink.rs`, `src/serve.rs`, `src/snapshot.rs`, `src/catalog.rs`, `src/pgwire.rs`) — no new
design is claimed to already exist; every citation below is to what's actually there today.*

---

## 0. Framing — two different "OLTP over columnar" problems

Before designing anything, one distinction has to be made explicit, because it changes
tractability enormously and the North Star's phrasing ("serve OLTP directly over columnar")
is genuinely ambiguous between two audiences:

1. **Serving Postgres's own OLTP traffic** — the buffer pool asks for an 8 KB heap page, an
   index probe needs an exact `(block, offnum)`, and the answer must be byte-identical to what
   real Postgres would have on disk. This is `docs/v2-scope.md`'s **V2c** and its **P5 "reverse
   path"** — bit-exact page reconstruction from Parquet + WAL tail. It is explicitly flagged
   there as *"the research question... nobody outside Databricks has shipped it."* Nothing in
   this document changes that assessment or duplicates it.
2. **Serving a new, lake-native point-read/write interface** — a client that wants "give me
   the current row for primary key K" or "tell me if K exists," addressed by the table's own
   primary key, with no obligation to reconstruct anything Postgres-page-shaped. This is a
   fundamentally easier problem: it only has to be internally consistent with what open-ltap
   already decoded, not byte-identical to a Postgres page. **This document designs (2), not
   (1).** It is the tractable slice of "OLTP over columnar," and every sub-problem below is
   scoped accordingly.

The two are not in tension — (2) could even backstop (1) as an ingredient (a fast way to find
*which page* a row lives on), but that composition is explicitly deferred (see §8) rather than
assumed.

**A second boundary, stated up front because "point... writes" is easy to misread:** nothing
here proposes accepting a write from a client that isn't Postgres. Postgres (or Neon) remains
the sole write authority, exactly as CLAUDE.md's settled decisions already require ("Physical
WAL, not logical replication... the decoder can later attach to Neon safekeepers" — the write
path is never open-ltap's to own). "Writes" in this document means *how fast a Postgres-
committed row becomes point-read-visible in the hot tier* — the ingest side of items §1 and
§7 — never a new external write-acceptance API. If that boundary is ever revisited, it stops
being this project's product and needs its own design; it is out of scope here.

---

## 1. What already exists (grounding recap)

The instinct to reach for "build an LSM engine" undersells how much of the shape already
exists, just not wired together or exposed. Concretely, today:

| Concept | What plays that role today | Gap |
|---|---|---|
| Memtable (recent writes, queryable) | `Mirror` — `HashMap<Ctid, RowVersion>` per table, updated synchronously in `handle_commit` before any Delta flush (`engine.rs:188`, `:751-767`) | keyed by physical `ctid`, not primary key; never queried from outside `Engine::preimage()` (`engine.rs:381-385`) |
| Flush-to-SSTable cadence | `PendingBatch` + `flush_all`, triggered every `LTAP_FLUSH_ROWS` (5000) rows or `LTAP_FLUSH_MS` (750ms) (`engine.rs:162-181`, `:315-346`, defaults at `engine.rs:122-124`) | already LSM-shaped cadence; the batch itself is an unkeyed `Vec`, not queryable |
| Durability of recent writes | Postgres's/Neon's own WAL retention (via the replication slot, or Delta watermarks on the safekeeper path) — **not** a log open-ltap owns | none needed; reuse, don't reinvent (see §1 design below) |
| Tombstones | `_ltap_deleted` column, correct at the Delta-log layer by construction (M2d); `Mirror` expresses a delete as a bare `remove()`, not a positive marker (`engine.rs:765`) | absence-from-memtable ≠ confirmed-tombstone — see §5 |
| Point lookup into columnar | none — `compact()` and `load_mirror()` both read **every active file in full** (`sink.rs:555-568`, `:430-436`) | no file pruning, no PK sort order to prune against |
| Snapshot-at-LSN | none exposed, but every row already carries `_ltap_lsn`, and every Delta commit already carries an `open-ltap.commit` watermark (`sink.rs:14-20`, `:402-406`) | the primitive exists; nobody reads it that way yet |
| Secondary indexes | none; `pg_index` is read only to name PK columns for compaction (`catalog.rs:186-269`) | see §6 |
| Compaction | `DeltaSink::compact` — full replace: read every file, keep latest-per-PK, rewrite the whole survivor set in one commit (`sink.rs:535-666`) | correct, but O(table size) per pass — wrong shape for per-write cadence |
| Random-access oracle | `Oracle` — page-level (`RelTag`, `blkno`, `LSN`) → 8 KB page via the pageserver's `pagestream_v3` (`engine.rs:238-293`, `pgwire.rs:85-360`) | answers a different question than a PK lookup — see §8 |

The upshot: most of this document is "re-key and expose what's there," not "invent an engine
from nothing." Where new machinery genuinely is needed, it's called out plainly.

---

## 2. The eight sub-problems

### 2.1 Write-optimized ingest tier / memtable, and how it flushes to columnar

**Design space:**
- **(A) Re-key the existing Mirror by primary key**, in place of (or alongside) its current
  ctid keying, and treat it as the memtable directly — durability continues to ride Postgres's
  own WAL retention (the memtable is a pure read-side structure, rebuilt from the Delta change
  log on restart exactly as `load_mirror` already does, `sink.rs:419-422`). Flush cadence
  (750ms / 5000 rows) is unchanged; each flush produces the columnar tier's newest "SSTable."
- **(B) A dedicated local WAL** for the hot tier, independent of Postgres/Neon's retention
  window, for durability that doesn't depend on the upstream slot staying open. This duplicates
  infrastructure the project already gets for free (M1's exactly-once watermarks,
  `sink.rs:14-20`) and contradicts the North Star's framing of physical replication *as* the
  ingest mechanism, not a second one bolted on top.
- **(C) Double-buffered memtable** (mutable + immutable-being-flushed), the standard LSM
  technique for write availability during a flush. Speculative until real throughput numbers
  show today's single-buffer flush (already an atomic, sub-second Delta commit) actually
  stalls writers — no such evidence exists yet.

**Recommendation: (A).** The PK-keying fix is also, independently, worth doing for
correctness — `docs/v2-scope.md`'s own Unit E1 notes flag that current-state reads must
partition by PK, "NOT `_ltap_ctid`," because a non-HOT update moves a row's physical address
and the old ctid's last-known version would otherwise wrongly look current. Re-keying Mirror
by PK fixes that pre-existing note *and* produces the memtable this section asks for, in one
change. (B) and (C) are both explicitly rejected for now — (B) as scope creep against a
settled decision, (C) as solving a problem not yet observed.

### 2.2 Single-row point-read path: memtable + columnar + tombstones

The hard part isn't the memtable half (§2.1 gives that); it's avoiding a full-file scan on the
columnar half. Today there is **no file pruning at all** — `compact()` and `load_mirror()`
both read every active Parquet file (`sink.rs:555-568`, `:430-436`).

**Design space:**
- **(A) Lean on Delta's own per-file min/max column statistics**, written automatically by the
  Parquet writer and surfaced in each `Add` action, to skip files whose PK range can't contain
  the lookup key — no DataFusion needed (consistent with the M0 thin-sink decision), just
  reading `Add.stats` JSON directly.
- **(B) A dedicated PK → file/row-group index**, maintained alongside compaction. More precise
  than min/max stats (which degrade badly for non-sortable keys like random UUIDs), at the
  cost of a new structure to keep consistent and write on every compaction.
- **(C) Sort compacted output by primary key** (a small change to `compact()`'s survivor
  ordering, `sink.rs:602-621`, which today only preserves original row order via
  `sort_unstable()` on row index). This makes (A)'s min/max pruning nearly as tight as a real
  index, for free, and is exactly the trick every LSM/lakehouse system already relies on
  (sorted SSTables; Iceberg's planned row-level indexes and Delta's Z-order/liquid clustering
  are more elaborate versions of the same idea).

**Recommendation: (A) + (C) together.** Zero new dependencies, composes directly with the
existing `compact()` rewrite, and degrades gracefully (falls back to a full scan) for
not-yet-compacted files or non-sortable composite keys — it doesn't have to be perfect to be a
large win. **Honest open item:** whether delta-rs's writer actually populates useful stats for
this project's full type matrix (`uuid`-as-string, `bytea`, `timestamptz`) is unverified —
first thing to check before relying on it (see §4 open questions).

**Read-side merge order:** memtable (freshest, §2.1) → PK-pruned Delta files, newest-compacted
first → miss. Latency is then dominated by object-store GETs for the (hopefully few) candidate
files; a local file/page cache is a likely-necessary companion, reusing rather than inventing
caching infrastructure (V2c's row-page cache is the same idea one layer down).

### 2.3 MVCC visibility / snapshot-at-LSN reads

This is the pleasant surprise of the whole document. Because visibility here is **total LSN
order**, not Postgres's per-row xmin/xmax MVCC, a single scalar target LSN is already a fully
consistent snapshot cut across every key — there is no xmin/xmax resolution to do at all. This
is arguably *simpler* than what real Postgres MVCC has to do, not harder, and the M5 oracle
already proved the identical pattern works for pages ("page versions are keyed by record-END
LSN," `engine.rs:252-254`).

**Design space:**
- **(A) Recent history via the memtable, older history via Delta's own version log.** Every
  Delta commit already carries an exact `open-ltap.commit` LSN watermark
  (`sink.rs:14-20`, written at `sink.rs:402-406`) — so "find the Delta version whose commit
  watermark is the greatest ≤ target LSN" is already answerable from data the sink writes
  today, with zero new bookkeeping. Read that Delta version (`VERSION AS OF`), then apply the
  same `_ltap_lsn ≤ target` filter within it for exactness (needed because Delta's version
  granularity is per-flush-batch, coarser than per-row LSN). For very recent LSNs (inside the
  tail-retention window `LTAP_TAIL_RETAIN_MS` already uses for M4), answer straight from the
  memtable's history instead of touching Delta at all.
- **(B) Bounded-only:** only support "AS OF" for LSNs within the existing tail retention
  window, same limitation M4 already accepts for its `min_lsn` long-poll.

**Recommendation: (A).** This is close to free — the watermark mechanism that already exists
for exactly-once resume (M1) turns out to also be a version→LSN index for snapshot reads,
without adding anything new to write. Call this out as a genuine design win, not just a gap
being closed: the existing exactly-once machinery is pulling double duty here. The one real
limitation, inherent and not fixable by any of this — same as the whole M0–M4 product — is
that an in-progress (uncommitted) Postgres transaction is invisible until it commits; physical
replication cannot show a client its own uncommitted writes mid-transaction.

### 2.4 In-place update over immutable columnar

Taken literally, this sub-problem has no solution, and shouldn't be given one: nothing in any
LSM or lakehouse system (RocksDB, Cassandra, Iceberg, Delta) mutates an on-disk file in place.
"In-place update" is always an illusion assembled from a mutable overlay (§2.1's memtable) plus
periodic physical cleanup (§2.7's compaction) plus a read-time merge (§2.2) making it *look*
synchronous. open-ltap already committed to this model in M2d (append-only change log,
`sink.rs:1-9`) and it is the correct choice, not a compromise.

**Design space, for completeness:**
- **(A) Keep the append-only model as-is.** §2.1 + §2.2 + §2.7, once built, already deliver
  update semantics that feel synchronous to a reader. No new machinery needed here.
- **(B) Synchronous single-file rewrite on every UPDATE.** Rejected outright: rewriting a
  potentially multi-GB Parquet file for a one-row change is catastrophic write amplification —
  exactly the failure mode §2.7 exists to avoid, just paid synchronously instead of batched.
- **(C) Iceberg-style positional/equality delete files** — a small side-file marking specific
  rows in a data file as superseded, without touching the data file's bytes. A real middle
  ground, but it's the same idea as §2.7's DV-based compaction option; not duplicated here.

**Recommendation: (A).** This sub-problem is really asking "do §2.1+§2.2+§2.7 add up to
update semantics" — yes, by construction, once built. No separate design is owed.

### 2.5 Delete/tombstone read path at point granularity

This is the single most important correctness subtlety in the whole document, and it's a real
bug waiting to happen if glossed over.

**The trap:** Delta's own log is always internally consistent — a DELETE unconditionally
writes an explicit tombstone row (`sink.rs`'s `EmitRow { deleted: true, .. }`), so the
Delta-layer `QUALIFY`-style read is correct regardless of what the memtable holds. But
`Mirror`'s *current* handling of a delete is a bare `t.mirror.remove(&ctid)`
(`engine.rs:765`) — **absence**, not a positive marker. For a keyed point-read protocol, that's
a latent false-positive: if a reader treats "not in memtable" as "ask Delta, and if Delta's
pruned scan finds an old un-superseded-looking row, return it," a DELETE whose memtable entry
was evicted (or never distinguished from "row never existed") could resurface a stale row.

**Design space:**
- **(A) Explicit tombstone markers, retained until confirmed redundant.** Change the delete
  path to store a positive tombstone entry in the keyed memtable (not a bare removal),
  evicted only once the compaction that physically drops the corresponding Delta rows has
  itself completed — the same tombstone lifecycle RocksDB and every real LSM engine use
  (a tombstone lives until compaction proves no older version survives beneath it).
- **(B) Never evict tombstones.** Simplest, but memory-unbounded for high-churn tables —
  exactly the concern CLAUDE.md's "M2 leftovers" already flags for the ctid-keyed mirror
  ("mirror memory bounds"); rejected for the same reason.
- **(C) Always fall through correctly instead of retaining tombstones.** Requires the
  point-read protocol to *never* treat a memtable miss as "definitely doesn't exist" and
  always resolve via a correct, LSN-ordered Delta-layer lookup on miss. This is necessary
  regardless of (A) vs (B) — it's the real invariant, and (A) is really just "don't discard
  information before you've confirmed it's redundant" layered on top of it.

**Recommendation: (A) + (C).** Both are required together: (C) is the invariant that makes the
design correct at all (a point-read protocol must always be prepared to fall through to a
correct Delta lookup); (A) is the latency optimization that makes the common case fast without
violating it. Flag this prominently in any implementation: it is the one place a naive
"memtable is a cache, Delta is truth" mental model silently breaks if tombstones are modeled as
absence instead of a fact.

### 2.6 Secondary indexes — punt, and why

**Ground truth:** Databricks's own shipped design explicitly does not transcode indexes
("served and rebuilt from that hot cache tier," `docs/v2-scope.md` §1 fact 4). open-ltap today
has zero index machinery beyond reading `pg_index` purely to name primary-key columns for
compaction (`catalog.rs:186-269`).

**Design space:**
- **(A) Punt entirely.** Serve point reads/writes by primary key only. Any query needing a
  non-PK predicate goes to the analytical path (a Delta/Iceberg scan, with whatever file
  pruning §2.2 already buys on columns that happen to have useful stats — "OLAP-shaped"
  filtering, not an OLTP point lookup).
- **(B) Auxiliary compacted index tables.** A second compacted `(secondary_key, pk)` table per
  indexed column, maintained by the same compaction pass, mirroring Postgres's own
  `CREATE INDEX`es (readable from `pg_index`'s non-PK entries, already partially plumbed).
  Real, buildable, no new external dependency — but it multiplies every one of §2.2–§2.5's
  problems (pruning, snapshot reads, tombstones, compaction write-amp) by the number of
  indexed columns, before the primary-key path is even proven.
- **(C) Hint-only, no real index.** Read `pg_index` as a signal for which columns to sort or
  Z-order within compacted files, improving (A)'s incidental pruning without building a
  dedicated structure.

**Recommendation: (A), explicitly, for now.** Three reasons: it matches the one production
implementation of this idea that exists (Databricks); it avoids inventing an N-times-over
version of every other hard problem in this document before the PK-only path is validated; and
nothing else here blocks on it — secondary indexes are pure additive scope. Defer to a later
phase (§3, P10-later) gated on real workload evidence that PK-only access is insufficient
*and* the primary-key path being production-validated first. (B) is the answer if that gate is
ever met; (C) is worth a cheap look before (B) if it is.

### 2.7 Compaction / write-amplification at per-row cadence, including DV-based collapse

**Existing:** `DeltaSink::compact` is fully replace-based — read every active file fully into
memory, compute latest-per-PK, rewrite the entire survivor set as new file(s) in one commit
(`sink.rs:535-666`), triggered by `LTAP_COMPACT_ROWS` (default 1,000,000 accumulated change-log
rows). Fine at that cadence — amortized over a million rows, nobody minds the write-amp.
Nothing should run *that* on every write; the real question is what compaction *strategy*
keeps the hot tier's reads cheap without paying full-table-rewrite cost constantly.

**Design space:**
- **(A) Tiered/leveled compaction (classic LSM).** Today's ~750ms flushes are already
  small "L0" files by construction. Add a background job that periodically merges a *bounded*
  subset of the oldest unmerged files into fewer, larger, PK-sorted "L1" files — the same
  latest-per-PK-and-drop-tombstones logic `compact()` already implements, just applied to a
  window instead of the whole table, with level/tier bookkeeping (which files belong to which
  level, size ratios between levels) layered on top. Write-amp becomes
  O(log_fanout(total size)) instead of O(total size) per pass — the standard LSM result.
- **(B) Deletion-vector (DV) based collapse.** Already flagged as unblocked in CLAUDE.md:
  *"DV-based collapse now feasible (`buoyant_kernel` `deletion_vector_writer` is
  DataFusion-free) — would cut write amp."* Instead of rewriting data files, write a small DV
  file per compaction pass marking superseded/tombstoned *rows* as logically removed from
  their original data file, leaving the data file's bytes untouched. This is Delta's (and,
  via positional/equality deletes, Iceberg's — §2.4 option C) native mechanism for exactly
  this problem: write-amp drops from "rewrite N MB of surviving data" to "write a few KB of
  bit-vector markers" — a step change, not an incremental one. Point-reads (§2.2) then read a
  data file together with its DV (one extra small-file GET per candidate file), a fine trade.
- **(C) Hybrid — DV for the fast path, today's replace-based compact for the slow path.**
  Mark superseded/tombstoned rows via DV on (or shortly after) every flush; periodically run
  the existing, unchanged `compact()` to physically reclaim DV-marked space and bound file
  count. This is exactly how production DV-capable systems actually operate (Delta's DV
  feature is designed to be periodically "checkpointed" back into rewritten files).

**Recommendation: (C).** DV-marking on the existing ~750ms flush cadence becomes the new fast
path — every UPDATE/DELETE marks its superseded row's prior position immediately, so
current-state reads are cheap without waiting for the 1M-row compaction threshold — while
`compact()` is kept **unchanged** as the slow-path physical reclaim on its existing cadence.
This turns "per-row cadence" into "per-flush cadence" (750ms, already proven fine in
production), which is honest: no real LSM system — RocksDB included — does anything
compaction-adjacent on literally every write either; they batch into the memtable first, same
as this project already does. **Directional, not verified:** the DV write path
(`buoyant_kernel`) has not been prototyped end-to-end against delta-rs's own DV read support —
first thing to check before committing to (C) (see §4).

### 2.8 Composing with vs. replacing the Neon pageserver Oracle

**Existing:** `Oracle` is a page-level `(RelTag, blkno, LSN) → 8 KB page bytes` client over the
pageserver's `pagestream_v3` protocol (`engine.rs:238-293`, `pgwire.rs:85-360`), used
exclusively today as a pre-image fallback for the transcoder's *own* WAL decoding — DELETE
tombstone content and UPDATE prefix/suffix reconstruction (`engine.rs:619-624`, `:834-844`),
degrading gracefully to mirror-only on failure.

**The hot tier this document designs is PK-keyed and row-shaped; the Oracle is block-keyed and
page-shaped.** They answer different questions and neither replaces the other:

- The Oracle does **not** get replaced — it answers "what are the raw bytes of block N of
  relation R at LSN X," which the hot tier's memtable/Delta lookup has no way to answer once a
  row has been decoded away from Postgres's physical layout.
- The hot tier does **not** take over the Oracle's existing job — reconstructing
  prefix/suffix-compressed UPDATE tuples and DELETE tombstone content during WAL decode is an
  internal need of the decode pipeline, unrelated to serving an external point-read client.
- **Where they genuinely compose:** both are the same underlying pattern — a keyed cache in
  front of an expensive-to-reconstruct source of truth, populated from commit-order WAL,
  falling back to a page-oracle on miss. In principle the hot tier's memtable *could* use the
  Oracle as a third-tier fallback for a narrow race window right at a flush boundary — but
  §2.1's synchronous mirror-on-commit update means a committed row is never in neither the
  memtable nor Delta, so this should be scoped as a later refinement if a real gap is found in
  practice, not built speculatively now.
- **A real, worth-watching dependency:** `docs/v2-scope.md`'s V2a "Unit D" (native
  `Timeline::get` reads replacing the external pagestream client) would turn the Oracle from an
  RPC into an in-process call — which matters far more for this document's per-write-cadence
  ambitions than for the existing sparse pre-image-fallback use case. Worth revisiting once
  V2a lands; not a prerequisite for anything in this document.

**Recommendation:** keep the two systems architecturally separate — different keyspace,
different purpose, different failure domain — while flagging Unit D as a future accelerant for
the hot tier specifically, once it exists.

---

## 3. Phased plan

- **P10a — PK-keyed memtable + point-read (buildable today, no new dependencies).** Re-key
  `Mirror` by primary key (§2.1, also fixes the pre-existing ctid-staleness note from Unit
  E1); make DELETE write an explicit tombstone entry instead of a bare `remove()` (§2.5's
  correctness fix); sort compacted output by PK and read Delta `Add` stats for file pruning on
  the fallback path (§2.2). Expose this as an in-process function
  (`Engine::point_read(table, pk) -> Option<Row>`), not yet a wire protocol — matching how
  every M0–M4 mechanism was proven in-process before growing a network surface. "Current
  state" only; no AS-OF-LSN yet.
- **P10b — Snapshot-at-LSN reads (mostly free given existing watermarks, §2.3).** Expose
  `point_read_at(table, pk, lsn)`; correlate target LSN → Delta version via the existing
  `open-ltap.commit` txn action; verify with a synthetic test in the style the `tests/` suite
  already uses for WAL-level correctness.
- **P10c — DV-based fast-path compaction (§2.7).** Prototype `buoyant_kernel`'s DV writer
  against a real delta-rs read-back path first (this is the one item in this whole document
  that is genuinely unverified against real crate behavior); if it round-trips, wire it in as
  the default flush-adjacent marking mechanism, `compact()` unchanged as the slow path.
- **P10d (later, gated) — a real wire protocol + secondary indexes.** Today's `serve.rs` is
  Parquet-blob-only (§1 table); a network-facing point-get API is new scope, gated on P10a–c
  proving out. Secondary indexes (§2.6 option B) only if workload evidence justifies them.

---

## 4. Open questions

- **Delta stats coverage** — does delta-rs's writer populate useful min/max stats for this
  project's full type matrix (`uuid`-as-string, `bytea`, `timestamptz`)? Unverified; check
  before relying on §2.2's pruning.
- **DV write/read round-trip** — `buoyant_kernel`'s DV writer against delta-rs's own DV read
  path has not been prototyped end-to-end. §2.7's recommendation is directional, not
  code-verified.
- **Memtable memory bound at scale** — re-keying `Mirror` by PK doesn't solve or worsen the
  open memory-bound question CLAUDE.md's "M2 leftovers" already carries for the ctid-keyed
  mirror; worth solving once, for both uses, together, rather than twice.
- **Delta vs. Iceberg for this specific work** — `docs/v2-scope.md` §5 P7 already flags
  Iceberg's branch/tag model as "materially better" for V2c's PITR/branching, and
  positional/equality deletes (§2.4 option C) are more mature in Iceberg's ecosystem than
  Delta's DVs today. This document doesn't relitigate the M0 thin-sink-over-Delta decision
  (CLAUDE.md: "do not relitigate for the product") — but if a hot tier is ever built, Iceberg's
  delete-file model may be the more natural fit than Delta's DV mechanism. Worth a spike
  before committing, not assumed either way here.
- **Read-side concurrency** — this document assumes the existing single-writer-per-table model
  continues (CLAUDE.md: "Single writer per Delta table"). A point-read/write serving tier
  meant to answer many concurrent OLTP-shaped clients needs a read-scaling story (memtable
  replicas? a read-only query fleet fed by the same WAL stream?) that isn't addressed here —
  flagged as unscoped, not solved.

---

## 5. Relationship to P0 → V2a → V2b → V2c

This document is orthogonal to, not a stage within, the staged v2 plan (`docs/v2-scope.md`
§4). It targets audience (2) from §0 above — a lake-native point-read/write API — which needs
none of V2a's in-pageserver embedding, V2b's image-layer transcode, or V2c's byte-exact page
reconstruction to be useful; P10a is buildable against the external M0–M5 product as it stands
today. Where the two tracks do intersect: V2a's Unit D (native `Timeline::get` reads) would
improve the Oracle's cost profile in a way that matters more here than there (§2.8); and if
V2c's reverse path (P5) ever does ship, this document's memtable becomes one more legitimate
input to it, not a competing design. Neither blocks the other.
