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
- Working tree = `main`, pushed. GitHub Pages serves `/docs` on `main`.

## Next: milestone plan

- **M1 (next up)** — resume from persisted LSN (store last-committed LSN in Delta commit
  metadata → exactly-once across restarts); `XLOG_HEAP2_MULTI_INSERT` (COPY); CRC32C validation
  of records; batch commits (currently 1 Delta commit per PG commit = small files). Also: create
  a replication slot so PG retains WAL while we're down.
- **M2** — UPDATE/DELETE via Delta deletion vectors (needs pre-image strategy: PK from new tuple
  for updates; for deletes keep a ctid→key map or read old page); subtransactions (parse subxact
  lists from commit records); TOAST + inline-compressed varlenas; FPI handling
  (`full_page_writes=on`); initial snapshot + consistent cutover.
- **M3** — WAL-driven catalog tracking (DDL mid-stream, relfilenode changes from
  TRUNCATE/rewrite, add/drop column), multi-table, every-table-automatically.
- **M4** — freshness read path: serve "Delta ≤ LSN + in-memory tail" merged reads
  (Arrow Flight or DuckDB table function). Headline feature; no Apache-licensed competitor has it.
- **M5 / v2 (future work)** — Neon safekeeper source; pageserver as GetPage@LSN oracle;
  eventually transcoding inside pageserver compaction (canonical columnar).

## Code map (src/)

- `pgwire.rs` — hand-rolled replication wire client (frontend protocol v3, trust auth only).
  Deliberately not libpq/tokio-postgres: official crate lacks replication mode, and the M5
  safekeeper source will reuse this shape.
- `wal/mod.rs` — `WalReader` (record reassembly across 8KB pages: page headers, continuation,
  alignment, padding) + `parse_record` (block headers per xlogrecord.h).
- `wal/heap.rs` — heap INSERT tuple decode (null bitmap, alignment, varlena per varatt.h);
  bool/int4/int8/text/varchar only so far. XACT opcodes also here.
- `schema.rs` — "catalog lite": table descriptor via SQL at startup (M3 replaces this).
- `txbuf.rs` — per-xid row buffering; commit ships, abort discards.
- `sink.rs` — Delta create-if-absent + `RecordBatchWriter` append; `_ltap_lsn` column = commit LSN.
  Uses `AWS_S3_ALLOW_UNSAFE_RENAME` (single writer, dev).
- Little-endian only, 64-bit maxalign assumed. Postgres 17 WAL format.

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
- Dev runs `full_page_writes=off`; with it on, insert records carry FPIs instead of tuple data
  (decoder skips them with a warning until M2).
- Delta has no unsigned types — LSNs stored as `long` (`_ltap_lsn`).
- `deltalake` API drifts between minor versions; writes go through `RecordBatchWriter`
  (not `DeltaOps.write`, which needs the heavy `datafusion` feature).
