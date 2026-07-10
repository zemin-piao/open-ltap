# CLAUDE.md — open-ltap

Postgres physical-WAL → Delta Lake transcoder. An open-source (Apache-2.0) take on
Databricks' Lakebase LTAP. Repo: https://github.com/zemin-piao/open-ltap ·
Architecture deep-dive: https://zemin-piao.github.io/open-ltap/ (source: `docs/index.html`).

## Settled decisions — do not relitigate

- **Delta over Iceberg**, via the `deltalake` (delta-rs) crate. delta-kernel-rs writes were too
  immature (blind appends only). The sink is intentionally thin so an Iceberg backend could be
  added later.
- **Physical WAL, not logical replication.** No slots, no publications, no `REPLICA IDENTITY FULL`.
  Same WAL bytes as any standby → the decoder can later attach to Neon safekeepers.
- **The product is M0–M4 against vanilla Postgres** (RDS/self-hosted/Supabase/Neon compute).
  Audience: Postgres + S3 + Spark/Delta shops replacing Debezium/Kafka pipelines.
  **M5 (Neon safekeeper source) and v2 (in-pageserver transcoding) are future work** — mention as
  research track, never as a prerequisite.
- Moonlink rejected as a dependency (BSL 1.1). Watch for Databricks' announced open-source
  "LTAP Writer Library" (announced June 2026, unreleased as of July 2026) — relevant to M5/v2.

## State

- **M0 shipped & verified 2026-07-02**: single table, INSERT-only, fixed schema; end-to-end
  Postgres → Delta on MinIO → DuckDB `delta_scan` confirmed (NULLs, multi-row atomic commits,
  rollback exclusion, varlena decoding).
- **M1 shipped & verified 2026-07-03**: replication slot; exactly-once resume (commit + restart
  LSN persisted as Delta `txn` actions, app ids `open-ltap.commit`/`open-ltap.restart`; restart =
  oldest in-flight txn's first record; standby status reports Delta-durable restart as flushed);
  CRC32C; COPY (`XLOG_HEAP2_MULTI_INSERT`); batched Delta commits (`LTAP_FLUSH_ROWS`/`LTAP_FLUSH_MS`).
  Verified: kill -9 with txn in flight across the crash, 50k-row bulk over a segment boundary,
  zero dupes.
- **M2 partial shipped & verified 2026-07-03**: subtransactions (subxact lists parsed from
  commit/abort main data, savepoint rollback excluded); pglz inline-compressed varlenas;
  types now bool/int2/int4/int8/float4/float8/text/varchar/bpchar/bytea/uuid/date/timestamp/
  timestamptz (timestamp→Delta timestampNtz, timestamptz→timestamp UTC, uuid→string).
- **M2b shipped & verified 2026-07-03**: FPI handling (tuples extracted from full-page images —
  dev compose now runs `full_page_writes=on`); out-of-line TOAST (chunks buffered per xid,
  resolved eagerly at pointer decode; pglz-externalized too); resume falls back to the slot's
  restart_lsn when Delta has no watermark. Verified byte-identical whole-table md5 vs PG.
- **M2d shipped & verified 2026-07-04 — UPDATE/DELETE**: append-only change log; Delta schema
  gains `_ltap_seq` (intra-commit order), `_ltap_deleted` (tombstones), `_ltap_ctid` (mirror
  rebuild key). Pre-images from an in-memory ctid→(row, on-page attr bytes) mirror: seeded by
  snapshot (raw bytes via pageinspect when available), maintained from WAL ops, rebuilt from the
  Delta change log on restart (+ pageinspect refresh for long rows — safe because replay
  refreshes any reused ctid before an update can reference it). Update records with
  XLH_UPDATE_PREFIX/SUFFIX_FROM_OLD are reconstructed against the old attr bytes; unchanged
  toast values carry over from the old decoded row. Per-txn overlays give intra-txn chains and
  cross-subxact visibility (safe: row locks). Current-state read = QUALIFY latest (lsn,seq) per
  key, NOT _ltap_deleted. Verified byte-identical vs PG across updates/HOT/toast-kept/
  toast-changed/double-update-one-txn/savepoint-rollback/delete + kill-9 restart mid-scenario.
- **M2c shipped & verified 2026-07-04**: initial snapshot + consistent cutover (`snapshot.rs`):
  binary COPY under `LOCK TABLE IN EXCLUSIVE MODE`, cutover = `pg_current_wal_insert_lsn()` under
  the lock, snapshot ships as one Delta commit with both watermarks = cutover; stream dedupes
  everything ≤ cutover. `LTAP_SNAPSHOT=off` disables. Verified against concurrent writers
  racing the lock and restart-after-kill (no re-snapshot, no dupes).
- **M3a shipped & verified 2026-07-04 — multi-table**: one slot + one stream feed N tables
  (`Engine` in main.rs); auto-discovery of public tables (unsupported types skipped with warn),
  routing by relfilenode (toast filenodes in a set), per-table sink/mirror/dedupe/pending, ops
  tagged with table index, per-table snapshots at their own cutovers, flush rounds flush every
  non-empty batch and stamp all with one GLOBAL restart LSN (min over in-flight txns, floor =
  last processed commit); startup resume = min over tables' restarts. Verified: 4 tables incl.
  toast + cross-table txns (committed, aborted, and spanning a kill -9), per-table md5 identical.
  Note: mirror rebuild from Delta includes stale ctid versions (log has no old→new link) —
  correct (WAL refreshes reused ctids) but memory grows with change-log size until compaction.
- **M3b shipped & verified 2026-07-04 — relfilenode changes**: XLOG_SMGR_CREATE (main fork, our
  db) marks the xid suspect; at that txn's commit the catalog is re-read (SQL) and any tracked
  table whose filenode changed is remapped: tombstone-all from the mirror (at the DDL commit
  LSN, flushed under the OLD filenode watermark) + re-snapshot at a fresh cutover (committed
  under the NEW filenode, dedupe=cutover) — one mechanism covers TRUNCATE, TRUNCATE+INSERT
  same-txn, VACUUM FULL, CLUSTER, and ALTER rewrites. `open-ltap.filenode` txn action per Delta
  commit; startup mismatch vs live catalog = offline truncate → same remap (idempotent: the
  filenode watermark only advances with the snapshot commit). Schema-changing rewrites detach
  the table with a warning (M3c). Verified: all of the above live + offline + aborted TRUNCATE
  no-op, per-table md5 identical after each.
- **M3c shipped & verified 2026-07-04 — column DDL**: TableDesc gains `phys` (all attribute
  slots; dropped columns keep attlen/attalign as skip-entries) vs `cols` (live). Decode walks
  phys; tuples shorter than the descriptor read NULL for trailing cols; natts > phys raises a
  typed SchemaDrift error → needs_resnapshot + catalog check (self-healing). DDL detection =
  heap writes to pg_class/pg_attribute filenodes mark the xid suspect (same flow as smgr).
  remap_check: flush-all FIRST (old shapes/mappings), then per table evolve Delta schema
  additively by name (dropped cols stay, NULLs onward; type change → detach), reshape mirror
  rows, re-snapshot if fast defaults (atthasmissing) or drift. Offline DDL: sink open unions
  existing Delta schema with fresh desc; fast-default-while-down → re-snapshot at startup.
  **delta-rs trap**: RecordBatchWriter MergeSchema silently DROPS the new column's values in
  the first batch (merged batch puts new col after meta cols; parquet ArrowWriter keeps the old
  schema) — fix: schema evolution is a separate metadata-only commit BEFORE the data write.
  Verified: live ADD (drift self-heal for same-batch DML), DROP mid-row (walk over dropped
  slot), ADD DEFAULT 42 materialized via re-snapshot, offline plain ADD + offline ADD DEFAULT,
  per-table md5 identical throughout.
- **M3d shipped & verified 2026-07-04 — table lifecycle**: auto mode attaches new tables at the
  catalog check (attach_table = sink open + snapshot at fresh cutover + dedupe, same reasoning
  as startup); DROP detaches (Delta frozen); RENAME followed via relfilenode→name lookup before
  declaring a table vanished (Delta path keeps the first-seen name). Unattachable tables
  (type conflicts, unsupported types) warn once and are skipped (attach_failed set); auto-mode
  startup skips tables whose Delta can't open instead of dying. M3 COMPLETE.
- **M4 core shipped & verified 2026-07-04 — freshness read path** (`serve.rs`): TailStore
  (std RwLock, guards never held across await) fed by the engine — per-commit RecordBatches
  (sink.make_batch), flushed batches retained LTAP_TAIL_RETAIN_MS so Delta+tail merges are
  gap-free in any query order (overlap collapses via the latest-(lsn,seq) dedupe). Hand-rolled
  HTTP/1.1: GET /tail/<table>.parquet (?min_lsn long-poll → 200/408; 204 = empty tail),
  /status JSON, X-Ltap-Applied-Lsn header; applied_lsn = last_recv after each batch. DuckDB
  needs `SET force_download=true` + httpfs (no Range support served). Verified: 2-min flush lag
  where delta_scan misses rows the merged read sees; min_lsn RYW (200 instant / 408 timeout);
  flush-overlap dedupe (7/7 distinct). `scripts/verify-fresh.sh` = demo reader.
- **M4b shipped & verified 2026-07-06 — change-log compaction** (`sink.rs::compact`): inline in
  the single writer (so NO commit coordinator / Unity Catalog / conditional-put needed), collapse
  the append-only log to latest-(lsn,seq)-per-PK, drop tombstoned + superseded, rewrite as
  remove-all-adds-in-one-commit preserving the commit/restart/filenode txn actions. Works in
  Arrow space (conform each file to current schema + concat + group-by-PK + `take`) so retired
  (PG-dropped, Delta-retained) columns survive byte-for-byte. Trigger: per-table rows-written
  counter >= LTAP_COMPACT_ROWS (default 1e6, 0=off), checked after flush; needs a PK (pg_index
  indisprimary) else skipped. Mirror rebuild off a compacted table is CLEANER (no stale ctids).
  Verified: 1811→189 rows, current-state md5 identical, post-compaction UPDATE/DELETE/INSERT +
  kill-9 restart all exact; keyless table skipped without error. NOTE: DV-based collapse now
  feasible (buoyant_kernel `deletion_vector_writer` is DataFusion-free) — would cut write amp.
- **M4 wrapped 2026-07-06**: vacuum after compaction (delta-rs VacuumBuilder, DataFusion-free;
  LTAP_VACUUM_MINS default 1440, "off" disables, enforce_retention_duration(false)) — verified
  orphans physically deleted, one parquet file left, md5 intact; tail memory bounded
  (LTAP_TAIL_MAX_ROWS default 100k, evicts oldest FLUSHED batches only — unflushed are the only
  copy outside PG; batch-granular) — verified merged reads stay complete through eviction.
  Arrow Flight deliberately skipped (gRPC stack for marginal gain over HTTP-Parquet). M4 DONE —
  the M0–M4 product is complete.
- **M5 validated end-to-end 2026-07-08 — safekeeper source**: dialect split threaded through the
  existing decoder rather than a second one — `HeapFmt::{Vanilla,Neon}` (`wal/heap.rs`) carries
  the `t_cid: u32` offset shift through every tuple-header parser (`wal_heap_header`,
  `parse_update_main`, `multi_insert_offsets`, `decode_insert_tuple`, `decode_update_new_tuple`,
  `decode_toast_chunk_from_wal`); `rmgr::NEON` (134) opcodes are normalized onto the vanilla
  `(rmid, op)` space at the top of `Engine::handle_record` (`main.rs`) so the rest of decode
  stays dialect-agnostic. `pgwire.rs` gained safekeeper startup params, `AuthenticationCleartextPassword`
  handling (JWT), and `start_replication_safekeeper` (bare `START_REPLICATION PHYSICAL <lsn>`,
  no SLOT/TIMELINE clause). `WalSource::{Postgres,Safekeeper}` (`LTAP_SOURCE`) skips slot
  creation on the safekeeper path — Delta watermarks are the only resume authority, there being
  no slot concept on a safekeeper. `schema::neon_ids()` reads tenant/timeline off the compute's
  `neon.tenant_id`/`neon.timeline_id` GUCs when not given via env. `neon-compose/` +
  `scripts/neon-init.sh` vendor upstream neondatabase/neon's docker-compose (pageserver, 3
  safekeepers, storage broker, compute wrapper) for a local stack, with safekeeper1's pg port
  published so the transcoder on the host can stream from it.
  **Bug found + fixed during validation**: connecting to a real safekeeper failed
  (`IDENTIFY_SYSTEM failed: tenantid is required`, ttid logged as all-zero) — safekeepers don't
  read `tenant_id`/`timeline_id` as top-level startup params; they must be packed into the
  standard libpq `options` param as whitespace-separated `key=value` tokens
  (`safekeeper/src/handler.rs` `startup()`, via `pq_proto`'s `options_raw()`). Fixed by sending
  `options="tenant_id=... timeline_id=..."`.
  **Verified against a live neon-compose stack** (real Neon compute, `server_version 17.5`):
  INSERT (2-row), UPDATE, DELETE, multi-insert/COPY (3-row, incl. a longer string), and a forced
  post-checkpoint INSERT (`full_page_writes=on`) all decoded byte-exact via DuckDB `delta_scan`;
  the tombstoned DELETE was correctly excluded from the current-state QUALIFY view. The
  `HeapFmt::Neon` offset math was also cross-verified field-by-field against `neon_xlog.h` pulled
  from `neondatabase/postgres@REL_17_STABLE_neon_17_5` (matching the compute's actual PG major):
  `xl_neon_heap_header` (9B, `t_cid` after the two infomasks), `xl_neon_heap_update` (`new_offnum`
  at byte 16 vs. 12 vanilla), `xl_neon_heap_multi_insert` (offsets array at byte 8 vs. 4),
  `xl_neon_heap_delete` (`offnum` unmoved at byte 4 — `t_cid` is appended at the end), and
  `xl_neon_multi_insert_tuple` (the *per-tuple* struct inside a multi-insert has no `t_cid` at
  all — confirms `decode_multi_insert` correctly needed no `fmt` param) all match the
  implementation exactly. Commits `76c0912`, `273ba74`, `7af1a03` (the options-param fix), all
  pushed to `origin/main`.
  **Honest gaps remaining**: the true FPI-restore path (`img.restore()` →
  `decode_tuple_from_page`) never actually fired — every record obtained, including the forced
  post-checkpoint insert, carried block data alongside the image (matches `neon_xlog.h`'s
  documented behavior that tuple data is included even when an FPI is taken); since page-image
  decode reads the real on-page tuple layout, which has no `t_cid` (that's WAL-only), it's
  provably dialect-independent and already exercised under vanilla Postgres since M2b — but it
  wasn't triggered under the Neon dialect specifically. Safekeeper `XLogData` framing/CRC wasn't
  byte-diffed against a vanilla walsender (inferred correct — no CRC errors across many records —
  but not a rigorous comparison). TOAST and DDL under the safekeeper path are untested. The
  pageserver `GetPage@LSN` oracle (pre-images/TOAST/backfill) still hasn't been started —
  pre-images still route through the compute's SQL port, same as M2d.
- **M5 synthetic-WAL regression suite 2026-07-09** (`tests/` — the repo's first tests): byte-exact
  record/page/tuple builders (`tests/common/mod.rs`, layouts per xlogrecord.h + the
  field-verified neon_xlog.h offsets above) drive the real `WalReader`/`parse_record`/decoders.
  Closes two M5 gaps deterministically, without a stack: (1) **FPI-restore under the Neon
  dialect** (`tests/neon_dialect.rs`) — rmgr-134 records carrying an image and NO block data now
  exercise `img.restore()` → `decode_tuple_from_page` (raw-with-hole, pglz-compressed, hole-less;
  multi-insert with Neon offsets array and INIT_PAGE; toast chunk from page; plus one framed
  end-to-end through the reader); (2) **framing/CRC parity** (`tests/wal_framing.rs`) —
  reassembly proven chunk-size-invariant and byte-identical to the built records,
  header-split-across-pages, long segment headers, mid-stream join via xlp_rem_len, CRC/magic
  corruption rejected (our CRC construction matching `parse_record`'s check = both match
  xloginsert.c). Every Neon `t_cid` shift is asserted against its vanilla twin AND asserted to
  misdecode under the wrong dialect (the shifts are load-bearing, not cosmetic). TOAST chunk
  decode + pointer resolution under Neon headers covered at the WAL-decode layer (live
  safekeeper TOAST/DDL E2E still open). Refactor: the Neon opcode normalization moved from
  `Engine::handle_record` into `heap::normalize_dml` (pure, tested).
- **M5 fully validated live 2026-07-09 — closes what the synthetic suite above left open**:
  re-ran FPI-restore and framing/CRC *live* against neon-compose to confirm the synthetic
  suite's coverage holds for real (forced a genuine Neon-rmgr FPI-only record via `VACUUM
  FREEZE` + `CHECKPOINT` + `UPDATE` — needed because Neon's compute defaults to
  `wal_level=logical`, whose `RelationIsLogicallyLogged`/`REGBUF_KEEP_DATA` normally keeps tuple
  data alongside every FPI, same as vanilla — `img.restore()` → `decode_tuple_from_page` fired
  and decoded byte-exact, independently confirmed via `pg_waldump`'s `FPW` flag on the
  safekeeper's durable WAL; and an independent from-scratch Python client, own CRC32C table and
  page-header/continuation parser, validated 2327 safekeeper records incl. rmgr 134 and 2582
  vanilla-walsender records with zero mismatches). Then closed the two items the suite
  explicitly left open — **TOAST and DDL over a live safekeeper stream** — which surfaced two
  real, pre-existing, dialect-independent bugs (reproduced identically on vanilla Postgres, not
  caught by the synthetic suite since it drives neither real pg_toast compression nor a live
  catalog query):
  - `ToastCache::resolve()` (`wal/heap.rs`) fed the whole reassembled out-of-line chunk buffer to
    `pglz_decompress`, but `toast_save_datum` (`toast_internals.c`) chunks a compressed datum
    starting at `VARDATA(dval)`, which includes the 4-byte compressed-varlena `tcinfo` header
    (the same one an inline-compressed value carries) before the real pglz stream. Fixed by
    skipping those 4 bytes (`85dd280`).
  - `schema::catalog_filenodes()` read `pg_class.relfilenode` directly, but `pg_class`/
    `pg_attribute` are themselves mapped relations — that column reads 0 for them on *any*
    Postgres, vanilla included — so `catalog_rels` was always `{0}` and DDL was never proactively
    detected (masked previously by the reactive `SchemaDrift` fallback, which only fires once a
    mismatched row is decoded, i.e. only after subsequent DML). Fixed with
    `pg_relation_filenode(oid)`, which resolves the relmapper indirection (`81796b3`).
  Both fixes verified end-to-end over the safekeeper source: `ADD COLUMN`, `DROP COLUMN`, and
  `TRUNCATE` (relfilenode rewrite) all correctly detected/handled; TOAST (incompressible and
  highly-compressible external values) decoded byte-exact (md5 match).
- **GetPage@LSN oracle client shipped & verified 2026-07-10** (P0-3 of `docs/v2-scope.md`):
  `pgwire.rs` speaks the pageserver's `pagestream_v3` sub-protocol — `connect_pageserver`
  (plain non-replication startup, `pagestream_v3 <tenant> <timeline>` into CopyBoth), then
  `get_page`/`rel_nblocks`/`rel_exists` as CopyData request/response frames (layouts per
  neon's `libs/pageserver_api/src/pagestream_api.rs`; V3 echoes the request header — reqid
  checked; not_modified_since = request_lsn, so only pass safekeeper-committed LSNs).
  `examples/getpage.rs` = harness; neon-compose now publishes pageserver port 6400. Verified
  live: 302/302 tuples decoded byte-exact vs SQL across 3 pages (pruned dead versions
  correctly skipped), and **time travel** — `LTAP_AT_LSN` pinned before an UPDATE+DELETE
  returned the exact pre-mutation state (old tuple values at same offnums) while the
  current-LSN read matched post-mutation SQL.
- **Oracle wired into the engine & verified 2026-07-10**: `Oracle` (main.rs) = lazy pagestream
  connection, auto-on for the safekeeper source (`LTAP_PS=off` disables; `LTAP_PS_HOST`/`_PORT`
  default localhost:6400, `LTAP_PS_TOKEN` for JWT); connect failure warns once and degrades to
  mirror-only, per-request failure drops the conn and retries next need. Pre-image fallback
  when the mirror can't answer: UPDATE fetches the old tuple's page at the record's **start**
  LSN (page versions are keyed by record-END LSN, so start LSN = state just before the record)
  via the old block's own BlockRef reltag — raw attr bytes via new `heap::raw_attrs_from_page`
  (same slice decode_tuple_payload returns, toast-free) for prefix/suffix, decoded row
  (tolerated failure — old toast chunks are gone) for unchanged-toast carry-over; DELETE
  fetches the old row for tombstone content. Mirror rebuild at restart **skips the pageinspect
  sweep** when the oracle is on (long-row attrs come lazily). handle_record/handle_update are
  now async. Verified live on neon-compose: 500-char-payload table (attrs unfaithful by
  construction), kill -9, restart (refreshed=0 — no pageinspect), prefix-compressed UPDATE
  replayed through the oracle + live batch of 10 more + DELETE — md5 identical to PG at every
  step, zero decode failures. Vanilla path untouched (oracle=None). **Remaining for M5-oracle**:
  snapshot/backfill still uses SQL COPY (visibility from pages = the v2b P2 problem); TOAST
  chunk backfill for pre-toast-update rows still unwired (old_row decode tolerates, carries
  from Delta-rebuilt mirror instead). M5 oracle = functionally complete for pre-images.
- `examples/walscan.rs` — offline WAL reader harness (feeds a raw segment file, compares against
  `pg_waldump`; supports chunked feeding to simulate streaming). Invaluable for reader bugs.
- **P0-1 layerscan shipped & verified 2026-07-10** (`examples/layerscan.rs`): offline pageserver
  layer-file reader — no pageserver, no fork, no S3 SDK. Parses both layer kinds (bincode-BE
  Summary on block 0, magics 0x5A60/0x5A61 v3; fixed-width disk-btree index, root/child blocks
  relative to index_start_blk, 5-byte values = 0x80+child-blk inner / 40-bit offsets leaf;
  blobs 1-byte len <0x80 else 4-byte BE high-bit + compression bits — **zstd is 0b001 (0x90),
  blob_io.rs's own doc comment saying 0b011 is wrong**; image keys 18B, delta keys 18B+LSN,
  delta leaf = BlobRef(pos<<1|will_init), delta values = bincode Value: 0=Image, 1=WalRecord,
  WalRecord tag 0 = Postgres{will_init,rec}=raw WAL record). `rel=<node> cols=<ty,..>` decodes
  pages with a synthetic TableDesc via decode_tuple_from_page. Verified live: delta layer (842
  entries, rmid-134 records + embedded page images decoded), forced image layer
  (`compact?force_image_layer_creation=true&force_l0_compaction=true` after a pageserver
  restart — the `checkpoint` API needs a testing build; image LSN only covers flushed L0s) —
  long_t decoded byte-exact from zstd image blobs, 20/20 rows, id-sum matching SQL.
- **P0-2 catalog-from-pages shipped & verified 2026-07-11** (`layerscan table=<name> db=<oid>`):
  derives a TableDesc from a single image layer with zero SQL — relmapper blob at key
  (0,spc,db,0,0,0) (512B pg_filenode.map, LE, magic 0x592717; pg_class/pg_attribute are mapped
  so their pg_class.relfilenode is useless) → pg_class/pg_attribute heap pages parsed by the
  PG17 FormData fixed layouts (fetched from REL_17_STABLE headers, offsets in the example;
  **attcacheoff still exists in PG17** — relfilenode@88, relkind@115, relnatts@116; attname@4,
  atttypid@68, attlen@72, attnum@74, attalign@87, atthasmissing@92, attisdropped@95). Spike
  visibility heuristic = keep xmax==0 catalog tuples (real answer = CLOG@LSN, v2-scope P2).
  Toast chunks preloaded from the toast rel's pages in the same layer feed the ToastCache.
  Verified vs live SQL: gnarly table (int8/bool/text/timestamptz/uuid + DROPped float4 column
  + ADD int4 DEFAULT 42) — derived desc exact (filenode 41019, toast 41022, 7 phys slots,
  fast_defaults=true), rows decoded byte-exact incl. a 6400-char incompressible out-of-line
  TOAST value (md5 match); pre-ADD rows read score=NULL as WAL semantics dictate (that's what
  the fast-defaults re-snapshot is for). **The P0→V2a gate (probes 1–2) is met.**
- **P0-4 cadence measured 2026-07-11** (results recorded in `docs/v2-scope.md` §P0 results):
  41 MB burst still 100% in the ephemeral layer 45 s later (defaults: checkpoint_distance
  256 MB, checkpoint_timeout 10 m, compaction_threshold 10 L0s, image_creation_threshold 3);
  zero organic image layers over days of light writing. V2b conclusion: image-creation
  transcode is a throughput path, never a freshness path — freshness must come from the tail
  merge / V2a's commit-ordered stream. **All four P0 probes complete.**
- **V2a step (a) shipped 2026-07-11** (`410554f`): the engine moved out of `main.rs` into the
  lib (`src/engine.rs` — Config/Table/Engine/Oracle/PendingBatch/Mirror + attach/remap helpers,
  public seams); `main.rs` is now a 284-line pgwire embedder; lib.rs exports
  engine/serve/sink/snapshot. Pure code motion; verified by the test suite + a live safekeeper
  smoke run (oracle pre-images, md5 match). Next V2a steps per `docs/v2-scope.md` §V2a
  execution plan: (b) stack upgrade + local neon build, (c) TranscodeSink tee patch + engine
  embedding, (d) gauntlet on the forked image.
- Working tree = `main`. GitHub Pages serves `/docs` on `main`.

## Next: milestone plan

- **M2 leftovers (nice-to-have)** — lz4/zstd decompression; mirror memory bounds.
- **Compaction leftovers** — DV-based collapse (less write amp than replace-based); streaming
  compaction for tables too big to hold in memory; optional VACUUM with a safe retention floor.
- **M3 leftovers (nice-to-have)** — discovery re-keyed by table OID instead of name (rename of
  A→B followed by CREATE A would confuse name-based tracking); attach_failed retry policy.
  Notes: rewrites are handled by re-snapshot, not by decoding XLOG_FPI page loads; rapid
  consecutive DDL on one table remains the known race window (mitigated by drift self-healing).
- **M4 leftovers (explicitly deferred)** — Arrow Flight endpoint (only if ADBC clients demand
  it); HTTP Range support if force_download ever hurts; DV-based compaction; streaming
  compaction for tables larger than memory.
- **M5 (fully validated end-to-end against neon-compose, see State above)** — the only remaining
  piece is the pageserver as a `GetPage@LSN` oracle (pre-images, TOAST, backfill); not yet
  started.
- **v2 (future work)** — transcoding inside pageserver compaction (canonical columnar).
  **Scoped 2026-07-10 in `docs/v2-scope.md`** (grounded in neon @ 8f60b04 + Databricks' June-2026
  LTAP blog): stages P0 (fork-free probes: layerscan.rs, catalog-from-pages, GetPage oracle =
  the open M5 item, cadence measurement) → V2a (embed the engine at WAL ingest; mirror dies,
  replaced by native page@LSN reads) → V2b (transcode at image-layer creation; fragments +
  tail merge) → V2c (heap-page demotion; research gate: reverse path + GC/PITR/branching).
  Key confirmed facts: Databricks transcodes at page materialization, keeps bit-exact datums,
  stores (block,offset) per row, does NOT transcode indexes; Neon delta layers store raw WAL
  records; CLOG/multixact + relmapper are in the pageserver keyspace (visibility + mapped-rel
  catalog decode need no SQL). LTAP Writer Library still unreleased as of 2026-07-10.

## Code map (src/)

- `pgwire.rs` — hand-rolled replication wire client (frontend protocol v3, trust auth only):
  IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT (idempotent), START_REPLICATION SLOT, standby status
  (flushed = Delta-durable restart LSN). Deliberately not libpq/tokio-postgres: official crate
  lacks replication mode, and the M5 safekeeper source will reuse this shape.
- `wal/mod.rs` — `WalReader` (record reassembly across 8KB pages: page headers, continuation,
  alignment, padding; record *headers* may split across pages — only xl_tot_len is guaranteed
  on-page) + `parse_record` (block headers per xlogrecord.h, CRC32C validated).
- `wal/heap.rs` — heap INSERT + multi-insert (COPY) tuple decode (null bitmap, alignment,
  varlena per varatt.h incl. pglz); XACT opcodes + subxact list parsing also here; `normalize_dml`
  maps rmgr-NEON opcodes onto the vanilla `(rmid, op)` space + `HeapFmt` dialect tag.
- `tests/` — synthetic-WAL regression suite (`cargo test`, no Postgres/Docker needed):
  `common/mod.rs` builds byte-exact records/pages/tuples in both dialects; `wal_framing.rs`
  covers reader reassembly + CRC; `neon_dialect.rs` covers Neon offset shifts, FPI-restore,
  TOAST decode. Extend it whenever a decode bug is found — cheapest place to pin a layout.
- `schema.rs` — "catalog lite": table descriptor via SQL at startup (M3 replaces this).
- `txbuf.rs` — per-xid op buffering (Insert/Update/Delete with ctids + RowVersion) + per-txn
  overlays for intra-txn pre-images; commit merges subxacts LSN-sorted, abort discards.
- `snapshot.rs` — initial snapshot (binary COPY with ctid under EXCLUSIVE lock, cutover LSN)
  + pageinspect raw-attr capture (also used standalone at restart).
- `sink.rs` — Delta create-if-absent + `RecordBatchWriter` write, committed via `CommitBuilder`
  with `open-ltap.commit`/`open-ltap.restart` txn actions; `_ltap_lsn` column = row's commit LSN.
  Uses `AWS_S3_ALLOW_UNSAFE_RENAME` (single writer, dev).
- Little-endian only, 64-bit maxalign assumed. **PG17 + PG18 verified** (2026-07-04: full M2
  gauntlet incl. FPI/COPY/TOAST/restart passed identically on 18.4; every layout we parse is
  unchanged between 17 and 18). `XLOG_PAGE_MAGICS` in `wal/mod.rs` allowlists verified majors
  (0xD116=17, 0xD118=18) — checked on every page header, which doubles as a desync guard.
  New major = run the gauntlet, add the magic. Dev compose: `LTAP_PG=18 docker compose up`.

## Dev loop

```sh
docker compose up -d && ./scripts/dev-init.sh   # PG17 (full_page_writes=off) + MinIO + table t
cargo run -- t                                  # transcode table t
docker exec -i openltap-pg psql -U postgres -d app -c "INSERT INTO t VALUES (..)"
./scripts/verify.sh t                           # DuckDB reads the Delta table
```

Machine notes (this dev box): Docker Desktop needs `DOCKER_HOST=unix://$HOME/.docker/run/docker.sock`
(the default socket belongs to another user; `open -a Docker` to start it). Homebrew is not
writable — DuckDB CLI lives at `~/.duckdb/cli/latest/duckdb`, gh at `~/.local/bin/gh`, cargo needs
`PATH=$HOME/.cargo/bin:$PATH`. `docker exec` needs `-i` for heredocs.

Git identity for commits: `zemin-piao <pzm6391@gmail.com>`.

## Gotchas learned the hard way

- The stock postgres image's `pg_hba` `host all` line does NOT match replication connections —
  `scripts/dev-init.sh` appends `host replication all all trust` post-start (initdb-mount scripts
  hit a Docker Desktop exec-permission quirk).
- Dev now runs `full_page_writes=on`; FPI-carried tuples are decoded from the page image.
  `wal_compression`/`default_toast_compression` must stay off/pglz (no lz4/zstd).
- WAL record *headers* can split across page boundaries — only xl_tot_len is guaranteed
  on-page. Never "skip to next page" when a header doesn't fit.
- Docker Desktop VM clock jumps on Mac sleep can trip `wal_sender_timeout` on idle streams —
  a dead transcoder after an idle stretch is usually that, not a code bug.
- Delta has no unsigned types — LSNs stored as `long` (`_ltap_lsn`).
- `deltalake` API drifts between minor versions; writes go through `RecordBatchWriter`
  (not `DeltaOps.write`, which needs the heavy `datafusion` feature).
