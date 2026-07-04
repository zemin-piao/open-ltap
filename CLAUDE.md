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
- `examples/walscan.rs` — offline WAL reader harness (feeds a raw segment file, compares against
  `pg_waldump`; supports chunked feeding to simulate streaming). Invaluable for reader bugs.
- Working tree = `main`. GitHub Pages serves `/docs` on `main`.

## Next: milestone plan

- **M2 leftovers (nice-to-have)** — lz4/zstd decompression; change-log compaction (rewrite to
  deletion vectors or periodic OPTIMIZE); mirror memory bounds.
- **M3** — WAL-driven catalog tracking (DDL mid-stream, relfilenode changes from
  TRUNCATE/rewrite, add/drop column), multi-table, every-table-automatically.
- **M4** — freshness read path: serve "Delta ≤ LSN + in-memory tail" merged reads
  (Arrow Flight or DuckDB table function). Headline feature; no Apache-licensed competitor has it.
- **M5 / v2 (future work)** — Neon safekeeper source; pageserver as GetPage@LSN oracle;
  eventually transcoding inside pageserver compaction (canonical columnar).

## Code map (src/)

- `pgwire.rs` — hand-rolled replication wire client (frontend protocol v3, trust auth only):
  IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT (idempotent), START_REPLICATION SLOT, standby status
  (flushed = Delta-durable restart LSN). Deliberately not libpq/tokio-postgres: official crate
  lacks replication mode, and the M5 safekeeper source will reuse this shape.
- `wal/mod.rs` — `WalReader` (record reassembly across 8KB pages: page headers, continuation,
  alignment, padding; record *headers* may split across pages — only xl_tot_len is guaranteed
  on-page) + `parse_record` (block headers per xlogrecord.h, CRC32C validated).
- `wal/heap.rs` — heap INSERT + multi-insert (COPY) tuple decode (null bitmap, alignment,
  varlena per varatt.h incl. pglz); XACT opcodes + subxact list parsing also here.
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
