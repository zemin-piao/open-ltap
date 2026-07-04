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

## Status: M1 done, M2 in progress

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

## Quickstart

```sh
docker compose up -d          # Postgres 17 + MinIO
./scripts/dev-init.sh         # replication trust (dev!) + demo table `t`
cargo run -- t                # start transcoding table `t`

# in another shell:
docker exec -i openltap-pg psql -U postgres -d app \
  -c "INSERT INTO t VALUES (1, 'hello lakehouse')"
./scripts/verify.sh t         # read the Delta table back via DuckDB
```

Config via env: `PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DB`, `LTAP_TABLE`, `LTAP_SLOT`,
`LTAP_FLUSH_ROWS`/`LTAP_FLUSH_MS` (batching), `LTAP_SNAPSHOT=off` (skip initial snapshot),
`DELTA_URI`, `S3_ENDPOINT/S3_ACCESS_KEY/S3_SECRET_KEY`.

## Roadmap — the product is M0 → M4, against vanilla Postgres

- **M0 (done)** — single table, INSERT-only, fixed schema, any Postgres
- **M1 (done)** — restart/resume from LSN watermarks persisted as Delta `txn` actions
  (exactly-once), replication slot, multi-insert (`COPY`), CRC32C validation, batched Delta
  commits
- **M2 (done)** — UPDATE/DELETE (append-only change log with tombstones + LSN/seq ordering);
  subtransactions; pglz-compressed values (inline and TOAST); out-of-line TOAST;
  full-page-image handling (`full_page_writes=on`); initial snapshot + consistent cutover;
  wider type matrix
- **M3** — WAL-driven catalog tracking (DDL mid-stream, relfilenode changes, add/drop column),
  multiple tables, every-table-automatically
- **M4** — the LTAP freshness read path: serve "Delta up to LSN X + in-memory tail" merged reads,
  so analytics get read-your-writes without touching Postgres. This is the feature no
  Apache-licensed alternative has.

At M4 the tool is complete for its primary audience: existing Postgres, existing lake, no new
database platform to adopt.

## Future work — storage-level integration (research track)

- **M5 — Neon safekeeper source.** The WAL format is identical, so the same decoder can attach to
  a [Neon](https://github.com/neondatabase/neon) safekeeper stream instead of a walsender: zero
  load on compute, and the pageserver becomes a random-access oracle (`GetPage@LSN`) for
  pre-images, TOAST chunks, and consistent backfill. Honest caveats: you must run a Neon stack,
  and table data still exists twice on S3 (Neon layer files + Parquet). Interesting mainly for
  platform teams already invested in Neon.
- **v2 — transcoding inside pageserver compaction.** The Lakebase endgame: Parquet becomes the
  *only* durable copy, row pages demote to a rebuildable cache. Requires forking the pageserver
  and solving the reverse path (rebuilding byte-addressed 8KB pages from Parquet). Research-grade;
  also watching for Databricks' announced open-source **LTAP Writer Library**, which would cover
  a large piece of this.

## Known limitations (deliberate, tracked by milestone)

- The change log grows forever (no compaction yet); readers use the latest-per-key view.
  Delta deletion-vector compaction is future work
- The pre-image mirror lives in memory: one entry per live row (decoded values + on-page
  bytes). Very large tables need RAM to match; the M5 pageserver oracle is the real fix
- Pre-image bytes for rows with long (>126 B) values come from `pageinspect` when available
  (in-tree extension, superuser); without it, the first prefix-compressed UPDATE of such a
  row after a snapshot/restart may be skipped with a warning
- lz4/zstd compression unsupported (`wal_compression` and `default_toast_compression`
  must be `off`/`pglz`)
- Schema read once at startup; DDL during streaming will corrupt decoding (M3)
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
