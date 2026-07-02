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

## Status: M0 — working vertical slice

- ✅ Hand-rolled replication wire client (trust auth, dev)
- ✅ WAL record reassembly (page headers, records spanning pages, padding/alignment)
- ✅ Heap `INSERT` decode: bool / int4 / int8 / text / varchar, NULL bitmaps, short + 4-byte varlenas
- ✅ Transactional correctness: rows buffered per xid; only `COMMIT`ed transactions reach the lake, aborts are discarded
- ✅ Delta append per committed transaction with `_ltap_lsn` (commit LSN) column
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

Config via env: `PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DB`, `LTAP_TABLE`,
`DELTA_URI`, `S3_ENDPOINT/S3_ACCESS_KEY/S3_SECRET_KEY`.

## Roadmap — the product is M0 → M4, against vanilla Postgres

- **M0 (done)** — single table, INSERT-only, fixed schema, any Postgres
- **M1** — restart/resume from a persisted LSN (stored in Delta commit metadata → exactly-once),
  multi-insert (`COPY`), CRC validation, batching (currently one Delta commit per Postgres
  commit = small files)
- **M2** — UPDATE/DELETE via Delta deletion vectors; subtransactions; TOAST + inline-compressed
  values; full-page-image handling (`full_page_writes=on`); initial snapshot + consistent cutover
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

## Known M0 limitations (deliberate)

- Dev containers run `full_page_writes=off` so WAL carries plain tuple data;
  FPI-carried tuples are skipped with a warning
- Schema read once at startup; DDL during streaming will corrupt decoding (M3)
- No CRC validation, no restart position (starts at current flush LSN)
- Little-endian hosts only (WAL is server-native-endian)
- Single writer per Delta table (`AWS_S3_ALLOW_UNSAFE_RENAME`)

## Design notes

WAL formats follow postgres `src/include/access/xlogrecord.h`,
`htup_details.h`, `varatt.h`, `heapam_xlog.h`. The wire client speaks the
frontend protocol v3 directly (`src/pgwire.rs`) — small enough to own, and it keeps the door
open for non-libpq sources (Neon safekeepers) behind the same interface.

License: Apache-2.0.
