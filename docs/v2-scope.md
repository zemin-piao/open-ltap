# v2 scoping — transcoding inside the pageserver

*Status: scoping document, no code. Written 2026-07-10.*
*Grounded against `neondatabase/neon` @ `8f60b04` (main, 2026-05-25) and Databricks' public
LTAP material (June 2026). File/line references below are to that Neon commit.*

---

## 0. TL;DR

v2 ("Parquet becomes the only durable copy") is not one project — it decomposes into **three
stages with independent value and a go/no-go gate between each**, plus a set of **fork-free
probe experiments** that should happen first:

| Stage | What ships | Copies of data | Fork? | Risk class |
|---|---|---|---|---|
| **P0 — probes** | layer-file reader harness, catalog-from-pages spike, cadence measurements | — | no | days each |
| **V2a — embedded engine** | the existing M0–M5 engine runs *inside* the pageserver, fed at WAL ingest; the pre-image mirror is deleted (native `page@LSN` reads replace it) | 2 (layers + Parquet) | yes, additive | engineering |
| **V2b — page-driven transcode** | image-layer creation emits Parquet fragments (`key_range@LSN`); analytics reads = Parquet + delta-layer tail merge | 2, converging | yes | design |
| **V2c — demotion** | heap main-fork pages become a rebuildable cache; Parquet is canonical; GC/PITR/branching re-based on the lake | 1 (heap) | yes, deep | **research** |

The recommendation at the end of this doc: run the P0 probes (they also finish the last open
M5 item, the GetPage oracle), then decide V2a. Do **not** start by "forking the pageserver"
as a monolithic act — start by proving the three load-bearing assumptions cheaply.

---

## 1. What Databricks has now confirmed publicly

The June 2026 LTAP launch came with an architecture blog that removes a lot of our guesswork.
Direct claims from [the Databricks blog](https://www.databricks.com/blog/lakebase-ltap-rethinking-database-storage):

1. **Transcoding happens at page materialization**: "As the PageServer materializes pages
   into object storage, it transcodes Postgres data from a row format into Parquet's columnar
   layout as it lands in the lake." — i.e. the hook is the image-layer/materialization path,
   not WAL ingest. (Our V2b, not our V2a.)
2. **Bit-exact value preservation**: "We preserve the exact Postgres representation of every
   value, down to the bits, so any Postgres-compatible engine can reinterpret it without
   losing information." — they do *not* do a semantic type mapping like ours; they keep raw
   datums. This is forced by the reverse path (see P6 below).
3. **The reverse path is real and rides physical addresses**: "Every row materialized to
   columnar carries its physical heap address (block and offset), so heap pages remain fully
   reconstructible." — our `_ltap_ctid` column is the same idea; we accidentally built the
   prerequisite in M2d.
4. **Indexes are not transcoded**: "Postgres indexes aren't transcoded into columns; they are
   served and rebuilt from that hot cache tier." — so "one copy" applies to the **heap main
   fork only**. Index forks, VM/FSM, and SLRUs stay page-native forever. This bounds the
   endgame honestly (The Register's coverage — "depending on what counts as a copy" — makes
   the same point).
5. **Row pages demote to cache**: "the PageServer still materializes traditional row-based
   pages in a local cache, but this is strictly a performance cache."

The announced open-source **LTAP Writer Library is still unreleased** as of 2026-07-10 (searched;
launch press from June 16, no repo, no license, no timeline). Plan as if it never ships; treat
it as a possible V2c accelerant if it does.

Sources: [Databricks blog](https://www.databricks.com/blog/lakebase-ltap-rethinking-database-storage) ·
[press release](https://www.databricks.com/company/newsroom/press-releases/databricks-launches-ltap-first-lake-transactionalanalytical) ·
[The Register analysis](https://www.theregister.com/databases/2026/07/03/databricks-unifies-oltp-and-olap-depending-on-what-counts-as-a-copy/5265733)

---

## 2. Where v2 plugs into the actual pageserver

Facts from the Neon source that shape the whole design (all verified at `8f60b04`):

- **Storage model**: each timeline is a custom two-level LSM over `page@LSN` keys
  (`docs/pageserver-compaction.md`). WAL ingest appends to an ephemeral layer; at
  `checkpoint_distance` (256 MB) it flushes to an **L0 delta layer** (whole keyspace, one LSN
  band). L0→L1 compaction merge-sorts ~10–20 L0s into keyed **L1 delta layers** (128 MB).
  **Image layers** — every page of a key range materialized at a single LSN — are created when
  `image_creation_threshold` (3) delta layers stack over a range.
- **Delta layers store raw WAL records** (`storage_layer/delta_layer.rs` header: "a collection
  of WAL records or page images"). Our WAL decoder's input format literally sits inside their
  layer files, keyed by page instead of by commit order.
- **The image-layer hook point**: `Timeline::create_image_layers`
  (`pageserver/src/tenant/timeline.rs:5856`) → per-relation `create_image_layer_for_rel_blocks`,
  which walks the partition, `get_vectored`s fully-materialized 8 KB pages at one LSN, and
  feeds `ImageLayerWriter::put_image`. **A transcoder tee here receives (RelTag, block, page
  bytes, LSN) for a whole relation range** — exactly `decode_tuple_from_page`'s input, already
  exercised in both WAL dialects since M2b/M5.
- **Visibility is resolvable in-process**: CLOG and MultiXact SLRUs are first-class keys in
  the keyspace (`libs/pageserver_api/src/key.rs:605-662`), readable at any LSN like any page.
  No compute connection needed to decide "xmin committed as of LSN X".
- **The relmapper is ingested** (`walingest.rs:1187`, `ingest_relmap_update` /
  `put_relmap_file`) — so mapped relations (`pg_class`, `pg_attribute`; the exact trap fixed
  in `81796b3`) can be resolved to filenodes without SQL.
- **Materialization uses walredo** (a sandboxed Postgres sidecar) — but transcode-from-page
  runs *after* redo, so we inherit correct pages without touching that machinery.
- **Sharding** stripes the block space across tenant shards
  (`shard_identity.is_key_disposable`, visible in `create_image_layers`) — a shard sees an
  interleaved subset of any relation's blocks. All v2 stages below assume **shard count = 1**
  until V2c; multi-shard transcode is explicitly out of scope (P8).
- **Operational envelope**: compaction runs on a 20 s loop under a concurrency semaphore, with
  backpressure to compute when L0 debt builds, and a **circuit breaker that disables compaction
  for 24 h after 5 failures**. Any transcoding hook must fail *open* (skip transcode, never
  block ingest or trip the breaker) — a transcoder bug must not take down the OLTP read path.
- **License**: Neon is Apache-2.0, same as open-ltap. Forking is clean.

---

## 3. The central design fork: WAL-driven vs page-driven

Everything in M0–M5 is **WAL-driven**: decode records in commit order, buffer per-xid, emit
commit-ordered rows. Lakebase's published design is **page-driven**: transcode materialized
pages when the storage engine materializes them anyway. These have different consistency
shapes and both belong in the plan — as successive stages, not competitors.

**WAL-driven (V2a)** — commit-ordered change log, globally consistent at every emitted LSN.
Freshness is bounded by flush cadence (seconds). Transcoding work is proportional to change
volume, paid once per change. Weakness: it needs commit-time context (that's what the mirror,
TOAST cache, and txbuf provide today) — but *inside* the pageserver all of that context is a
native `page@LSN` read away.

**Page-driven (V2b)** — transcode rides image-layer creation. Zero extra read of history, and
the output naturally *replaces* a storage tier instead of duplicating it. But image layers for
different key ranges are cut at **different LSNs**, and only when delta-stack depth triggers
them — so the Parquet side is a **patchwork of `key_range@LSN` fragments**, not a consistent
snapshot, and can lag arbitrarily on cold ranges. Consistent reads require the pageserver's
own trick generalized to columnar: scan fragment at its LSN + **apply the WAL tail** (the
delta layers above it) up to the read LSN. Freshness and consistency both live in the tail
merge — which is M4's architecture, relocated.

The stages connect: V2a proves the engine runs in-process and gives a commit-ordered log
(which the product audience already wants); V2b makes Parquet structurally an *image tier of
the LSM*; V2c then deletes the row-page image tier because Parquet + tail can serve both
readers. Each stage's output is the next stage's substrate.

---

## 4. Staged plan

### P0 — probes (no fork, days each, all independently valuable)

1. **`examples/layerscan.rs`** — read image/delta layer files straight from neon-compose's
   MinIO bucket (formats documented in `image_layer.rs`/`delta_layer.rs`; disk-btree indexed
   blobs). Proves we can consume layer files *without any fork at all*, and gives an offline
   harness for every later stage — the `walscan.rs` of v2. Also immediately useful: a
   fork-free "snapshot from layer files" backfill path that touches neither compute nor
   pageserver.
2. **Catalog-from-pages spike** — decode `pg_class` + `pg_attribute` heap pages (from
   layerscan) into a `TableDesc`, resolving mapped relations via the relmapper file. This is
   the hardest *engineering* unknown of V2a (P1 below) and needs zero Neon changes to prove.
3. **M5 GetPage oracle** (already on the roadmap) — speak the page_service pagestream protocol
   from the external transcoder for pre-images/TOAST/backfill. Everything learned (protocol,
   `RelTag`/key addressing, LSN semantics) transfers 1:1 into V2a, and it fixes the shipping
   product's mirror-memory limitation now.
4. **Cadence measurement** — instrument neon-compose: how stale do image layers actually run
   under OLTP-ish load? This number decides how much V2b's tail merge must carry, and whether
   `image_creation_threshold` needs tuning in the fork.

**Gate to V2a**: probes 1–2 succeed; we can name the maintenance budget for a fork.

#### P0 results (2026-07-10/11) — all four probes ran; the V2a gate is met

1. **layerscan** ✅ (`examples/layerscan.rs`, commit `7200003`): both layer formats parse
   offline; delta layers verified to carry raw WAL records (rmid 134) our decoder already
   reads, plus embedded page images; a forced image layer decoded a test table byte-exact
   from zstd blobs. One upstream doc bug found: `blob_io.rs` says the zstd bit pattern is
   0b011, its own constants say 0b001 (`0x90`) — the code is right.
2. **Catalog-from-pages** ✅ (`layerscan table=<name> db=<oid>`, commit `1594664`): relmapper
   → pg_class/pg_attribute by PG17 FormData layout → TableDesc (types, dropped slots, toast
   filenode, fast defaults), validated by decoding the table byte-exact from the same layer —
   including an out-of-line TOAST value resolved from the toast rel's pages (md5 match), zero
   SQL. Open ends for V2a: visibility used the xmax==0 spike heuristic (P2 owns the real
   answer), and pk discovery (pg_index) wasn't done.
3. **GetPage oracle** ✅ (commits `235392d`, `ae2f955`): pagestream_v3 client + engine
   integration; pre-images now come from the pageserver on the safekeeper path and the
   mirror's pageinspect dependency is gone. Closed the M5 remainder in the same stroke.
4. **Cadence** ✅ measured on neon-compose (defaults: checkpoint_distance 256 MB,
   checkpoint_timeout 10 m, compaction_period 20 s, compaction_threshold 10 L0s,
   image_creation_threshold 3): a 41 MB write burst was still entirely in the ephemeral
   layer 45 s (2+ compaction periods) later — no L0, no image layer. The organic freshness
   ladder is ephemeral (≤256 MB / ≤10 min) → L0 (10 L0s ≈ 2.5 GB before L1) → image layers
   (3 stacked deltas); this timeline produced **zero organic image layers across days of
   light writing**. Conclusion for V2b: transcode-at-image-creation is a *throughput* path,
   never a freshness path — LSN-exact reads must come from the tail merge (or V2a's
   commit-ordered stream), and any threshold tuning that forces images faster buys freshness
   with write amplification.

### V2a — embed the M0–M5 engine at WAL ingest (fork, additive)

Run today's engine as a task inside the pageserver, fed from the ingest path (the same decoded
records `walingest.rs` handles), writing Delta exactly as today. Nothing about Neon's storage
semantics changes; the fork is a tee plus a task, behind a config flag, failing open.

What this buys over M5-external:
- **The mirror dies.** Pre-images become `get(page, lsn-1)` against the timeline — the native
  read the pageserver exists to serve. The single biggest memory/complexity item in the
  codebase (mirror seeding, pageinspect, rebuild-on-restart, overlays for long rows) is
  replaced by one call. TOAST resolution and snapshot/backfill likewise become internal reads
  at exact LSNs — no `EXCLUSIVE` lock, no COPY, no racing writers.
- **No wire client, no slot, no compute involvement at all** (M5 still needed the compute's
  SQL port for pre-images and catalog).
- Catalog comes from pages (P0 probe 2) instead of SQL.

Deliverable: a `neon` fork branch (patch set, not divergence — see §7) where
`docker compose up` yields Postgres → safekeepers → pageserver → Delta on MinIO with no
transcoder process. Two copies still exist; that's fine — V2a is an *engineering* milestone
with user-visible value (zero-footprint CDC on Neon), not the endgame.

**Gate to V2b**: V2a survives the existing gauntlet (M1–M3 scenarios: kill -9, DDL, TOAST,
relfilenode rewrites) inside the pageserver's restart/failure model.

#### V2a execution plan (recon done 2026-07-11)

- **Version pin decision**: the compose stack's `neon:latest` image was built **2025-08-26** —
  ~9 months older than the source this doc cites (`8f60b04`, 2026-05-25). Either pin the fork
  to a release tag near the image (and re-verify layer formats against it) or upgrade the
  stack to a current image first. Prefer the upgrade: everything verified so far (pagestream
  V3, layer formats incl. the 0b001 zstd bit) was validated against the *running* old image
  AND read from May-2026 source, so both ends already agree; upgrading narrows rather than
  widens the gap.
- **Build logistics (this dev box)**: `postgres_ffi` needs built vendored Postgres trees
  (`POSTGRES_INSTALL_DIR`, bindgen) — so `make postgres-v14..v17` precedes any cargo build;
  cmake and pkg-config are missing and Homebrew is read-only (drop official binary releases
  into `~/.local/bin`); protoc exists. A macOS-built pageserver **cannot run in the compose
  stack** — runtime validation requires building neon's Linux image (hours, in the Docker
  VM), so plan two loops: fast local `cargo check` for patch iteration, slow image build for
  the live gauntlet.
- **Patch shape** (small series, rebase-friendly): (1) `pageserver/src/transcode.rs` — a
  `TranscodeSink` trait + a bounded tokio channel tee; (2) one call site in the walreceiver
  ingest path sending `(lsn, raw record bytes)` when a tenant-conf flag is on, dropping (with
  a counter) when the channel is full — fail-open, never backpressure ingest (P4); (3) config
  plumbing + the engine task. The engine side needs an open-ltap refactor first: Engine moves
  out of `main.rs` into the lib with a pluggable record source (today it's welded to the
  pgwire stream loop), SQL catalog swapped for the P0-2 catalog-from-pages path (productize
  from `examples/layerscan.rs`, + pg_index for pk discovery), pre-images via native
  timeline reads instead of the pagestream client.
- **Sequencing within V2a**: (a) open-ltap refactor (engine-as-lib, no neon dependency —
  independently shippable and testable against the existing safekeeper path); (b) stack
  upgrade + local neon build proving the toolchain; (c) the tee patch + engine embedding;
  (d) gauntlet in the compose stack with the forked image.
- **Empirical footnote**: the P0-4 ladder's checkpoint_timeout bound was confirmed live —
  the 41 MB burst rolled ephemeral→L0 at exactly the 10-minute mark.

#### V2a step (b) results (2026-07-12) — version pin + toolchain proven

- **There is no newer public image than the one already running.** The "prefer the upgrade"
  branch of the version-pin decision is moot: the local `neon:latest` digest (`7a4f1249…`)
  equals the remote `latest`; the full ghcr tag list (20,186 tags, paginated to exhaustion)
  ends at CI runs from late Aug 2025; the newest release tag (`release-9129`, 2025-07-25) is
  *older* than `latest`. The public repo effectively froze after Aug 2025: the running
  binaries report commit `77e22e4bf` (2025-08-25), which is an **ancestor of `main` only 10
  commits behind `8f60b04`** — and those 10 commits are a GCS remote-storage provider,
  direct-IO alignment config, README/typo/proxy fixes: nothing touching WAL ingest, layer
  formats, or pagestream. **Decision: fork base = `main` @ `8f60b04`** (the exact commit this
  doc cites); the running stack is fully representative of it. Local clone: `~/neon`.
- **Toolchain proven on this box** (fast loop works): pkgconf 2.3.0 built from source +
  cmake 3.31.9 official binary into `~/.local/bin` (`pkg-config` symlinked to pkgconf);
  read-only Homebrew supplies openssl@3, icu4c (pkg-config finds icu 78.2), protobuf. Rust
  1.88.0 auto-pinned via neon's `rust-toolchain.toml`. `make postgres-headers-install`
  (configure ×4 + header install, no bison needed — full postgres builds NOT required for
  bindgen) then `cargo check -p pageserver` passes clean. Fast patch loop =
  `PATH=~/.local/bin:$PATH cargo check -p pageserver` in `~/neon`.
- **Patch-shape correction (load-bearing for step c)**: at this version the
  safekeeper→pageserver protocol is **hardcoded `Interpreted` (protobuf + zstd)** at
  `pageserver/src/tenant/timeline.rs:3490` — the pageserver walreceiver never sees raw
  XLogData. The *safekeeper* decodes raw records (`safekeeper/src/send_interpreted_wal.rs:444`
  → `InterpretedWalRecord::from_bytes_filtered`) and ships `InterpretedWalRecords`; raw DML
  record bytes still reach the pageserver, but *inside* `SerializedValueBatch` values as
  `NeonWalRecord::Postgres { rec }` per modified block (exactly the raw records P0-1 found
  stored in delta layers), while commit/abort arrive as decoded `MetadataRecord`s (not raw).
  Two viable tee placements, both pageserver-side (the engine must stay in the pageserver for
  page@LSN reads): **(i)** tee at interpreted-batch ingest in
  `walreceiver_connection.rs` — engine consumes `(next_record_lsn, rec)` from batch values
  (dedupe per record via first block) + decoded commit metadata instead of raw XACT records
  (engine adaptation needed: commit/abort handling already lives in `wal/heap.rs` and could
  accept a pre-decoded form); **(ii)** a config knob restoring
  `PostgresClientProtocol::Vanilla` (the enum + Vanilla arm still exist in
  `walreceiver_connection.rs:290`), which hands the engine a true raw stream with the
  originally planned one-call-site tee — smallest patch, but diverges from the prod-default
  protocol path. Decide at step (c); (i) is truer to "transcode where the data already is".

#### V2a step (c) phase 1 (2026-07-12) — TranscodeSink tee scaffold, committed on the fork

- **The tee-placement decision made itself**: at `8f60b04` the walreceiver's Vanilla arm
  *returns an error* ("Vanilla WAL receiver protocol is no longer supported for ingest",
  `walreceiver_connection.rs`) — option (ii) is dead; option (i), consuming interpreted
  records, is the only path.
- **Fork branch `openltap/v2a` in `~/neon`, one commit (`104be78da`) atop `8f60b04`**:
  8 files, 263 insertions, only 44 lines outside the new `pageserver/src/transcode.rs`
  (the P9 "small patch series" shape). Contents: `TranscodeSink` trait (`offer(&record)`,
  non-blocking, infallible) + `ChannelTee` (bounded mpsc, `try_send`, offered/dropped
  counters, throttled drop logging) + a stub consumer task (per-timeline, spawned at
  `launch_wal_receiver` when enabled, gate-guarded + cancel-tied to the timeline; logs
  progress every 10k records — this is where the engine embeds in phase 2). Config:
  `[transcode] enabled = false` / `channel_capacity = 8192` in pageserver.toml
  (`TranscodeConfig` in `pageserver_api`). Plumbing: tee created in `launch_wal_receiver`,
  rides `WalReceiverConf`, offered one line before `walingest.ingest_record` consumes each
  record. Verified: `cargo check -p pageserver` (+ `pageserver_api`, `control_plane`) clean;
  3 unit tests (ordering, drop-on-full, drop-on-closed) pass. Branch is local-only — pushing
  requires creating a public neon fork repo (user decision).
- **Fail-open semantics settled** (P4): drops surface downstream as `next_record_lsn` gaps
  and poison the transcoded stream until the engine re-seeds (re-snapshot path) — correctness
  never depends on the tee keeping up; ingest never waits. Walreceiver reconnects replay
  records at-or-below already-seen LSNs; the engine's existing Delta-watermark dedupe handles
  that.
- **Phase 2 (engine embedding) — the adapter contract, from reading the shapes**: per
  `InterpretedWalRecord`, DML raw record bytes arrive in `batch` values as
  `Value::WalRecord(NeonWalRecord::Postgres { rec })` (dedupe per record: take the first
  block's copy — `rec` is the *whole* raw record, parseable by `parse_record` as-is);
  records whose blocks carry FPIs may instead arrive as `Value::Image(page)` → route through
  the engine's existing `decode_tuple_from_page` path (needs an adapter — the engine keys
  that off `parse_record` block structures today); commit/abort arrive *decoded* as
  `MetadataRecord::Xact(XactRecord::Commit/Abort(XactCommon { parsed, .. }))` where `parsed:
  XlXactParsedRecord` has xid + subxacts (what `txbuf` commit/abort needs — engine grows a
  pre-decoded commit entry point); `MetadataRecord::Smgr(Create)` + `record.xid` covers the
  M3b suspect-xid flow; `MetadataRecord::Relmap` is a bonus signal for mapped-rel catalog
  invalidation. Remaining phase-2 work beyond the adapter: catalog-from-pages productization
  (P0-2 → lib) and pre-images via native timeline reads (`Timeline::get` at
  `record-start LSN`), replacing the pagestream client; then step (d) = Linux image build +
  compose gauntlet with `[transcode] enabled = true`.
- **Embedding-viability probe (same day) — in-process embedding is viable as a plain cargo
  dependency.** With open-ltap as a path dep of the fork's pageserver crate: cargo
  resolution succeeds with *zero* version conflicts (233 packages added; neon's `parquet 53`
  coexists with deltalake's arrow/parquet — separate majors, no `links` collisions), and the
  full tree compiles. The one obstacle was MSRV: neon pins rustc 1.88.0 but deltalake 0.32
  needs ≥1.91.1 and aws-types ≥1.94.1 → **fork commit #2 (`5c2b75d2f`) bumps
  `rust-toolchain.toml` to 1.96.1** (covers step (d) too: build-tools' rustup respects
  `rust-toolchain.toml`, though `build-tools/Dockerfile`'s `RUSTC_VERSION=1.88.0` env is
  worth aligning when we get there) **and fixes the single thing newer rustc rejects in
  existing neon code** (`SchedulingResult` pub(super) returned by a pub(crate) trait, E0446 —
  fix is 1.88-compatible). Verified under pinned 1.96.1: `cargo check -p pageserver` clean +
  tee unit tests pass. The probe's Cargo.toml/lock changes were reverted — the dep lands
  with the engine in phase 2.

#### V2a step (c) phase 2, units A+B (2026-07-12) — engine seams + the interpreted adapter

- **Unit A (open-ltap `e07c713`) — engine seams for pre-decoded events**: public
  `Engine::handle_commit(lsn, xid, &subxids)` / `handle_abort` / `handle_smgr_create(xid, db)`
  extracted from `handle_record`'s XACT/SMGR arms; the raw-record path delegates to them, so
  both record sources share one implementation. Pure code motion; the 21-test suite stays
  green. `handle_record(lsn, &[u8])` was already source-agnostic — with these three seams the
  engine's ingest API is complete for interpreted mode.
- **Unit B (fork `3aaca63ce`) — `RecordEvent` + `events_from()` in `transcode.rs`**: the
  translation from `InterpretedWalRecord` to engine calls, pure and unit-tested (7 tests).
  Key facts encoded: (1) the decoder clones the *complete* original record into every
  `NeonWalRecord::Postgres` value it emits for a record (`serialized_batch.rs`), so one
  `Raw{lsn, rec}` per record is canonical and `Value::Image` FPI blocks are redundant
  whenever any Postgres value exists — the engine re-decodes all blocks itself, images
  included; (2) only an **all-image record** (FPI applied on every touched block) loses the
  raw bytes → surfaces as `PageImages{(RelTag, blkno, page)..}`, counted + warned by the stub
  consumer, not yet decodable — *measure in the gauntlet* before building a mitigation
  (expected rare: Neon computes run wal_level=logical which keeps tuple data alongside FPIs,
  and the value is only Image when `blk.apply_image`); (3) commits/aborts map from
  `XlXactParsedRecord` (xid + subxacts — prepared-txn records ignored, parity with the raw
  path); (4) `SmgrCreate` only for `forknum == 0`, matching `parse_smgr_create`. The stub
  consumer now counts events by kind, so a compose-stack run shows the full translation
  working before the engine is wired in.
- **Remaining for phase 2**: unit C = catalog-from-pages productized (P0-2
  `examples/layerscan.rs` → lib module + `pg_index` PK discovery for compaction); unit D =
  pre-images via native `Timeline::get` at record-start LSN (fork-side `Oracle`
  replacement); unit E = engine construction/config inside the consumer task (open-ltap as
  fork dep — probe already validated the dep tree) + Delta sink credentials story in the
  pageserver process; then step (d) gauntlet.

### V2b — page-driven transcode at image-layer creation

Tee `create_image_layer_for_rel_blocks`: for main-fork heap relations, additionally decode
every materialized page and emit a Parquet fragment covering `(rel, key_range, lsn)`, with
per-row `(block, offnum)` — the Databricks layout. Fragment metadata (range + LSN) rides Delta
commit metadata the same way `open-ltap.commit`/`restart`/`filenode` txn actions do today.

New machinery:
- **Visibility at the image LSN** (P2): per tuple, resolve xmin/xmax against CLOG *at the same
  LSN* (SLRU pages from the keyspace). Emit "committed as of LSN"; in-progress txns are simply
  not yet in the fragment and arrive via the tail.
- **HOT-chain collapse with root preservation** (P3): pick the visible version, record the
  chain root's line pointer for index fidelity.
- **The read path**: an LSN-exact scan = fragments + tail merge from delta layers above each
  fragment's LSN. V2a's commit-ordered log *is* a valid tail feed, so V2b's reader can be
  M4's `serve.rs` merged with fragment coverage tracking.

**Gate to V2c** (this is the research gate): a written design, validated by prototype, for
byte-placement-exact heap page reconstruction (P5) *and* an answer for GC/PITR/branching (P7).
If either fails, V2b is still a shippable end state — "Parquet image tier, row layers retained"
— which already de-duplicates most storage *cost* (image layers dominate bytes at rest;
delta layers within the PITR window are comparatively small).

### V2c — demote heap pages (research)

Only after the V2b gate: stop uploading image layers for heap main forks; serve GetPage misses
below the fragment horizon by rebuilding the page from Parquet (+ delta-layer tail), and gate
layer GC on Parquet coverage instead of image-layer coverage. PITR and branches re-base onto
lake-format time travel. Every open problem here is in the register below (P5–P9); scoping
further than that today would be fiction.

---

## 5. Hard-problem register

**P1 — Catalog without SQL.** `schema.rs` today shells out to SQL. In-pageserver we must
decode `pg_class`/`pg_attribute`/`pg_type` from their own heap pages (fixed, per-major
layouts; we already decode arbitrary heap pages) with relmapper resolution (ingested, see §2)
and pick a consistent catalog LSN (= the DDL txn's commit LSN — same suspect-xid flow as M3b/c).
*Risk: engineering, not research. P0 probe 2 retires it.*

**P2 — Visibility at a page-driven horizon.** A materialized page carries uncommitted, aborted,
and dead tuples; hint bits can't be trusted (not WAL-logged by default). Resolution: CLOG@LSN
from the keyspace per distinct xmin/xmax (cache per fragment; a fragment sees few distinct
xids). Sub-cases: multixact xmax (members SLRU is also in the keyspace), frozen tuples,
`xmin == xmax` same-txn churn. *Risk: fiddly but fully specified by the Postgres visibility
rules; the synthetic-WAL test style pins each case.*

**P3 — HOT chains and ctid identity.** On-page chains mean "the row at (block,offnum)" is not
one tuple. Fragment emit must walk chains to the visible version but record the **root** lp
(what indexes point at) as the row's address. V2c's rebuild must then re-materialize redirect
line pointers. *Risk: moderate; well-documented in `heapam` internals.*

**P4 — Fail-open transcoding under the compaction SLOs.** The tee must never block ingest,
never trip the compaction circuit breaker, and tolerate lagging behind (fragments are best-
effort freshness; correctness rides the tail). Needs its own error budget, metrics, and a
kill switch. *Risk: engineering discipline, not novelty.*

**P5 — The reverse path (V2c's core).** Rebuild an 8 KB heap page such that every index-visible
(block,offnum) resolves correctly: exact lp placement, redirects for HOT roots, plausible
xmin (FrozenTransactionId below the horizon is legal and simplest), recomputed checksum, page
LSN ≤ read LSN. Hint bits and free-space layout may differ — that's legal. **Requires bit-exact
datum round-trip (P6).** Nobody outside Databricks has shipped this; it is *the* research
question. *Approach: prototype as a pure function `(fragments, tail) → page` validated against
`pg_filedump`/amcheck on real clusters long before it serves a live GetPage.*

**P6 — Bit-exact vs semantic encoding.** Our Arrow mapping is semantic (readable by DuckDB/Spark
directly) but not provably round-trippable for every type; Databricks stores raw datums.
Options for V2c: (a) raw-datum binary columns alongside semantic ones (storage cost, both
audiences served), (b) canonical re-encode with proof-of-round-trip per supported type,
(c) restrict demotion to relations whose columns are round-trip-safe. Decide at the V2b→V2c
gate; V2a/V2b need no change. *Note: numeric — unsupported today — becomes unavoidable here.*

**P7 — GC, PITR, branching.** Today layer GC is gated by the PITR window; branches are CoW
references into ancestor layer stacks. If Parquet is canonical: PITR = lake-format time travel
addressed by LSN (fragment metadata already carries LSN), and a branch at LSN maps to reading
the parent's fragments ≤ LSN plus the branch's own tail. Delta has no native branching;
**Iceberg's branch/tag model is a materially better fit for V2c specifically** — flag for
reconsideration at that gate only (the thin-sink decision from M0 stands; do not relitigate
for the product). *Risk: design-heavy; V2c-gated.*

**P8 — Sharding.** Shards see interleaved block stripes; per-shard fragments would shred any
table scan. Options: transcode on shard 0 only (re-centralizes I/O), or a merge tier. *Scoped
out: v2 research assumes 1 shard; stated as a limitation.*

**P9 — The fork treadmill.** Neon moves fast (compaction was re-noted 2025-03; gc-compaction,
sharding, timeline offload all recent). Mitigations: keep the fork a **small patch series**
(tee + task + config) rebased on tags, not a divergent branch; define an internal trait
boundary (`TranscodeSink`) so patches stay mechanical; and pursue an upstream RFC
(`docs/rfcs/` is an active, numbered process — a "materialization tee" hook is arguably
upstreamable since Databricks has legitimized the pattern). If the LTAP Writer Library ships
with a usable license, re-evaluate everything above it.

---

## 6. What carries over from M0–M5 (4.3 kLOC audit)

| Component | v2 fate |
|---|---|
| `wal/heap.rs` decode (tuples, varlena, pglz, TOAST, both dialects) + `wal/mod.rs` framing | **carries whole** — V2a feeds it from ingest; V2b feeds `decode_tuple_from_page` from image materialization; the synthetic-WAL suite pins it throughout |
| `sink.rs` (Delta writes, txn-action watermarks, compaction, vacuum) | **carries whole** into V2a; V2b adds fragment metadata; V2c revisits (P7) |
| `txbuf.rs`, commit-ordered emit, exactly-once watermarks | carries into V2a; V2b's page-driven path doesn't need it (visibility replaces it) |
| M3 catalog tracking (suspect xids, remap, evolve, lifecycle) | logic carries; the SQL layer under it is replaced by catalog-from-pages (P1) |
| M4 `serve.rs` tail merge | becomes V2b's read path skeleton |
| **Mirror** (M2d), `snapshot.rs`, pageinspect | **deleted** in V2a — replaced by native `page@LSN` reads. The largest net simplification in the plan |
| `pgwire.rs` | dies in V2a (no wire); lives on in the external M0–M5 product, which **remains the product** — v2 never replaces it |

---

## 7. Posture, non-goals, and the gates restated

- **The M0–M4 product against vanilla Postgres stays the product.** v2 is a research track for
  Neon-platform operators; nothing in it may regress or gate the external transcoder.
- **Non-goals for all of v2**: transcoding index forks (page-native forever, per Databricks
  too); multi-shard tenants (P8); logical replication compatibility; supporting the fork as a
  managed service.
- **Gates**: P0→V2a = probes 1–2 pass. V2a→V2b = gauntlet passes in-process. V2b→V2c =
  reverse-path prototype validates against amcheck **and** a written GC/PITR/branching design.
  V2b is an acceptable terminal state if the V2c gate fails.
- **Sequencing note**: P0 probe 3 *is* the last open M5 item. Start there — it pays down both
  tracks at once.

## 8. First concrete steps (ordered)

1. `examples/layerscan.rs` against neon-compose's bucket: parse one image layer + one delta
   layer, dump keys/pages, decode one heap page with the existing decoder. (P0-1)
2. Decode `pg_class`/`pg_attribute` pages from those layers into a `TableDesc`; diff against
   `schema.rs`'s SQL answer for the same table. (P0-2)
3. Pagestream client for GetPage@LSN in the external transcoder; wire pre-images/TOAST through
   it; delete the mirror's pageinspect dependency. (P0-3 / closes M5)
4. Instrument image-layer cadence under load; write the number down. (P0-4)
5. Only then: fork branch, `TranscodeSink` trait, V2a tee behind a config flag.
