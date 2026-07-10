# open-ltap

Point it at the Postgres you already run — RDS, self-hosted, Supabase, Neon — and get
**fresh Delta Lake tables on your own S3**, readable by the Spark / DuckDB / Trino you already
have. One Rust binary. No Kafka, no Debezium, no logical replication slots, no
`REPLICA IDENTITY FULL`.

Inspired by [Databricks' Lakebase LTAP](https://www.databricks.com/blog/lakebase-ltap-rethinking-database-storage):
open-ltap consumes **physical WAL** — the same bytes any standby receives — and transcodes
committed transactions straight into Delta commits.

```
Postgres ──physical WAL──► open-ltap ──Arrow──► Delta table on S3/MinIO ──► Spark / DuckDB / anything
                              │
                              ├── reassembles XLogRecords across WAL pages
                              ├── decodes heap tuples (catalog-aware)
                              ├── buffers per-xid, ships only committed txns
                              └── appends with `_ltap_lsn` = commit LSN
```

**Who this is for:** teams running lots of Postgres next to an S3 + Spark + Delta stack who are
tired of babysitting a Debezium → Kafka → streaming-job pipeline (three copies of data, snapshot
cutover ceremony, small-file cleanup, per-table config) just to get operational data into the lake.

📊 **[Architecture deep-dive](https://zemin-piao.github.io/open-ltap/)** — how reads and writes
flow through each stage, with pros/cons of the product track (M0–M4) vs. the storage-level
future work (M5, v2). Source: [`docs/index.html`](docs/index.html).

## Status: M0–M4 complete (the product); M5 safekeeper source validated

- ✅ Hand-rolled replication wire client (trust auth, dev) + physical replication slot, so
  Postgres retains WAL while the transcoder is down
- ✅ WAL record reassembly (page headers, records and headers spanning pages, padding/alignment)
  with CRC32C validation of every record
- ✅ Heap `INSERT` + `COPY` (multi-insert) decode: bool / int2 / int4 / int8 / float4 / float8 /
  text / varchar / bpchar / bytea / uuid / date / timestamp / timestamptz; NULL bitmaps; short,
  4-byte, and pglz-compressed varlenas
- ✅ **`full_page_writes=on` works** (the production default): tuples are extracted from
  full-page images via line pointers when WAL carries an FPI instead of tuple data
- ✅ **TOAST**: out-of-line values reassembled from same-transaction toast-chunk inserts,
  including compressed-then-externalized values (pglz)
- ✅ **Multi-table**: one replication slot and one WAL stream feed every table (auto-discovered
  from `public`, or `LTAP_TABLES=a,b,c` / CLI args); records route by relfilenode; a transaction
  spanning tables lands in each table's Delta log under the same commit LSN. Tables with
  unsupported column types are skipped with a warning
- ✅ **Tables come and go**: in auto mode, `CREATE TABLE` attaches automatically (snapshot at a
  fresh cutover — inserts racing the attach are never lost or doubled), `DROP TABLE` detaches
  (the Delta table stays, frozen), and `ALTER TABLE RENAME` is followed (the Delta path keeps
  the original name)
- ✅ **ADD/DROP COLUMN mid-stream**: DDL is detected from the WAL itself (catalog heap writes
  mark the transaction; its commit triggers a re-read); the Delta schema evolves additively by
  name (dropped columns stay, receiving NULLs), dropped slots keep tuples walkable, and
  `ADD COLUMN ... DEFAULT` (fast defaults) or any decode drift converge via automatic
  re-snapshot. Column type changes still detach the table with a warning
- ✅ **TRUNCATE and VACUUM FULL / CLUSTER survive**: a relfilenode change is detected from the
  WAL (smgr-create + catalog re-check at that commit), the old state is tombstoned and the
  table re-snapshotted at a fresh cutover — including TRUNCATE+INSERT in one transaction, and
  truncates that happen while the transcoder is down (a filenode watermark in each Delta
  commit catches the mismatch at startup). Schema-changing rewrites (`ALTER TABLE ... TYPE`)
  detach the table with a warning until M3c
- ✅ **UPDATE and DELETE** transcoded as an append-only change log: updates append the new row
  version, deletes append a tombstone (`_ltap_deleted`); current state is one `QUALIFY
  latest-per-key` view away (see `scripts/verify.sh`). Pre-images come from an in-memory mirror
  maintained from the WAL itself (prefix/suffix-compressed update records are reconstructed
  against it), seeded by the snapshot and rebuilt from the Delta table on restart
- ✅ Transactional correctness: rows buffered per xid; only `COMMIT`ed transactions reach the
  lake; aborts and `ROLLBACK TO SAVEPOINT` subtransactions are discarded
- ✅ **Exactly-once across restarts (kill -9 included)**: every Delta commit carries the commit-LSN
  watermark and a WAL restart position as Delta `txn` actions; on startup the transcoder resumes
  from the restart LSN and dedupes replayed transactions
- ✅ Batched sink: many Postgres commits per Delta commit (`LTAP_FLUSH_ROWS` / `LTAP_FLUSH_MS`),
  each row still tagged with its own commit LSN in `_ltap_lsn`
- ✅ **Initial snapshot + consistent cutover**: on first run the existing table contents are
  copied (binary COPY under a brief write lock) as one Delta commit, and the WAL stream takes
  over at exactly the cutover LSN — no gap, no overlap, crash-safe
- ✅ Readable from DuckDB (`delta_scan`) — see `scripts/verify.sh`
- ✅ **Change-log compaction + vacuum**: the append-only log is periodically collapsed to
  current state (latest version per primary key, tombstones and superseded versions dropped)
  in one atomic remove-all + add-compacted commit that preserves the exactly-once watermarks,
  then orphaned files are reclaimed after a retention window. Runs inline in the single writer
  (no commit coordinator needed) and bounds read cost, restart time, AND storage.
  `LTAP_COMPACT_ROWS` sets the threshold; tables without a primary key are left as a full log
- ✅ **Freshness read path**: the transcoder serves its in-memory tail (committed in Postgres,
  not yet flushed to Delta) over HTTP as Parquet; `delta_scan + tail` merged reads see every
  committed transaction seconds after commit, with `?min_lsn=` long-polling for hard
  read-your-writes — see `scripts/verify-fresh.sh` and `GET /status`
- ✅ **Neon safekeeper source (M5, research track)**: `LTAP_SOURCE=safekeeper` streams the same
  physical WAL from a Neon safekeeper instead of a walsender (slot-less, JWT auth, Neon's
  custom heap rmgr normalized onto the vanilla decode path) — validated end-to-end against a
  live neon-compose stack; see the M5 section below for what's still open
- ✅ **Synthetic-WAL regression suite** (`cargo test` — no Postgres or Docker needed): byte-exact
  record/page builders drive the reader and decoders, covering WAL framing + CRC edge cases,
  every Neon `t_cid` offset shift (asserted to misdecode under the wrong dialect), the
  full-page-image restore path in both dialects, and TOAST chunk decode + pointer resolution

## Quickstart

```sh
docker compose up -d          # Postgres 17 + MinIO
./scripts/dev-init.sh         # replication trust (dev!) + demo table `t`
cargo run                     # transcode EVERY public table (or: cargo run -- t)

# in another shell:
docker exec -i openltap-pg psql -U postgres -d app \
  -c "INSERT INTO t VALUES (1, 'hello lakehouse')"
./scripts/verify.sh t         # read the Delta table back via DuckDB

cargo test                    # offline regression suite (WAL framing, Neon dialect, FPI, TOAST)
```

Config via env: `PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DB`, `LTAP_TABLES` (csv; default: all
public tables), `LTAP_SLOT` (default `ltap_<db>`), `LTAP_LAKE` (default `s3://lake`; each table
lands at `{lake}/{table}`), `LTAP_FLUSH_ROWS`/`LTAP_FLUSH_MS` (batching), `LTAP_SNAPSHOT=off`
(skip initial snapshot), `LTAP_HTTP_PORT` (freshness endpoint, default 8088, 0 = off),
`LTAP_TAIL_RETAIN_MS` (served-tail retention, default 60000), `LTAP_TAIL_MAX_ROWS`
(served-tail ceiling per table, default 100000), `LTAP_COMPACT_ROWS` (change-log rows before
a table is compacted, default 1000000, 0 = off), `LTAP_VACUUM_MINS` (orphaned-file retention
before deletion after compaction, default 1440, `off` = never),
`S3_ENDPOINT/S3_ACCESS_KEY/S3_SECRET_KEY`.

## Roadmap — the product is M0 → M4, against vanilla Postgres

- **M0 (done)** — single table, INSERT-only, fixed schema, any Postgres
- **M1 (done)** — restart/resume from LSN watermarks persisted as Delta `txn` actions
  (exactly-once), replication slot, multi-insert (`COPY`), CRC32C validation, batched Delta
  commits
- **M2 (done)** — UPDATE/DELETE (append-only change log with tombstones + LSN/seq ordering);
  subtransactions; pglz-compressed values (inline and TOAST); out-of-line TOAST;
  full-page-image handling (`full_page_writes=on`); initial snapshot + consistent cutover;
  wider type matrix
- **M3 (done)** — WAL-driven catalog tracking against a live stream: multiple tables
  every-table-automatically (one slot, one stream); relfilenode changes (TRUNCATE / VACUUM
  FULL / CLUSTER, online and offline); ADD/DROP COLUMN with Delta schema evolution;
  CREATE/DROP/RENAME table lifecycle
- **M4 (done)** — the LTAP freshness read path: the transcoder serves "Delta + in-memory
  tail" merged reads over HTTP/Parquet with bounded tail memory, so analytics get
  read-your-writes (`?min_lsn=` long-poll) without touching Postgres; plus inline compaction
  and vacuum keeping the log, restart time, and storage bounded. (An Arrow Flight endpoint was
  considered and deliberately skipped: HTTP-Parquet is readable by every engine already, and
  Flight would add a gRPC stack for marginal gain.)

At M4 the tool is complete for its primary audience: existing Postgres, existing lake, no new
database platform to adopt.

## M5 — Neon safekeeper source (fully validated) & future work (v2)

- **M5 — Neon safekeeper source.** The WAL format is identical, so the same decoder can attach to
  a [Neon](https://github.com/neondatabase/neon) safekeeper stream instead of a walsender: zero
  load on compute, and the pageserver becomes a random-access oracle (`GetPage@LSN`) for
  pre-images, TOAST chunks, and consistent backfill. **Done and verified end-to-end:** the
  safekeeper wire protocol (tenant/timeline packed into the libpq `options` startup param —
  safekeepers reject them as top-level params — JWT auth, slot-less `START_REPLICATION`) and
  decoding of Neon's custom heap rmgr (compute nodes log DML with a spliced-in `t_cid`,
  normalized back onto the vanilla decode path) were run against a live neon-compose stack (real
  Neon compute, PG 17.5): INSERT, UPDATE, DELETE, multi-insert/COPY, and a forced post-checkpoint
  INSERT all decoded byte-exact, and the `t_cid` offset math was independently cross-checked
  against `neon_xlog.h` field-by-field. A synthetic-WAL regression suite (`tests/`, `cargo test`
  — no Postgres or Docker needed) pins the decode layer deterministically: WAL framing/CRC
  reassembly edge cases, every Neon `t_cid` offset shift (asserted to misdecode under the wrong
  dialect), the full-page-image restore path in both dialects, and TOAST chunk decode. Every
  gap that suite flagged as still needing a live run has since closed too: a genuine full-page-
  image *restore* fired live and decoded correctly (confirmed independently via `pg_waldump`);
  an independent from-scratch client cross-validated WAL framing and CRC32C with zero mismatches
  against both the safekeeper and a vanilla walsender; and TOAST/DDL under the safekeeper path
  uncovered **two real bugs that turned out to be pre-existing and dialect-independent**
  (reproduced identically on vanilla Postgres) — a TOAST decompression bug (the reassembled
  out-of-line buffer needed its embedded 4-byte compression header skipped) and a DDL-detection
  bug (`pg_class`/`pg_attribute` are mapped relations, so their `relfilenode` column reads 0 on
  any Postgres, which silently defeated proactive DDL detection). Both are fixed and verified.
  The only piece of M5 left is the pageserver `GetPage@LSN` oracle itself —
  pre-images/TOAST/backfill still go through the compute's SQL port (same as M2d), and that
  integration hasn't started. Honest caveats once complete: you must run a Neon stack, and table
  data still exists twice on S3 (Neon layer files + Parquet). Interesting mainly for platform
  teams already invested in Neon.
- **v2 — transcoding inside pageserver compaction.** The Lakebase endgame: Parquet becomes the
  *only* durable copy, row pages demote to a rebuildable cache. Requires forking the pageserver
  and solving the reverse path (rebuilding byte-addressed 8KB pages from Parquet). Research-grade;
  also watching for Databricks' announced open-source **LTAP Writer Library**, which would cover
  a large piece of this. **Scoped in detail in [`docs/v2-scope.md`](docs/v2-scope.md)**: staged
  as fork-free probes → embedded engine (V2a) → page-driven transcode at image-layer creation
  (V2b) → page demotion (V2c), each with a go/no-go gate and a hard-problem register.

## Known limitations (deliberate, tracked by milestone)

- Compaction rewrites the whole table in memory (matches the mirror's in-memory assumption)
  and is replace-based; a deletion-vector variant (now feasible — the kernel exposes a
  DataFusion-free DV writer) would cut write amplification
- The pre-image mirror lives in memory: one entry per live row (decoded values + on-page
  bytes). Very large tables need RAM to match; the M5 pageserver oracle is the real fix
- Pre-image bytes for rows with long (>126 B) values come from `pageinspect` when available
  (in-tree extension, superuser); without it, the first prefix-compressed UPDATE of such a
  row after a snapshot/restart may be skipped with a warning
- lz4/zstd compression unsupported (`wal_compression` and `default_toast_compression`
  must be `off`/`pglz`)
- Column type changes (`ALTER TABLE ... TYPE`) detach the table (Delta cannot retype a
  column); renamed tables keep their original Delta path
- An idle stream doesn't advance the slot's restart position, so a quiet database
  retains WAL until the next transcoded commit
- Little-endian hosts only (WAL is server-native-endian)
- Postgres 17 and 18 supported (WAL page magic is validated per page; other majors are
  rejected up front rather than misparsed). Test another major with `LTAP_PG=18 docker compose up`
- Single writer per Delta table (`AWS_S3_ALLOW_UNSAFE_RENAME`)

## Design notes

WAL formats follow postgres `src/include/access/xlogrecord.h`,
`htup_details.h`, `varatt.h`, `heapam_xlog.h`. The wire client speaks the
frontend protocol v3 directly (`src/pgwire.rs`) — small enough to own, and it keeps the door
open for non-libpq sources (Neon safekeepers) behind the same interface.

License: Apache-2.0.
