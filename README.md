# open-ltap

An open-source take on [Databricks' Lakebase LTAP](https://www.databricks.com/blog/lakebase-ltap-rethinking-database-storage):
transcode Postgres **physical WAL** into Delta Lake tables on object storage —
no logical replication slots, no publications, no CDC pipeline in between.

```
Postgres ──physical WAL──► open-ltap ──Arrow──► Delta table on S3/MinIO ──► DuckDB / Spark / anything
          (START_REPLICATION            │
           PHYSICAL, same WAL           ├── reassembles XLogRecords across WAL pages
           the Neon safekeepers         ├── decodes heap tuples (catalog-aware)
           ship)                        ├── buffers per-xid, commits only committed txns
                                        └── appends with `_ltap_lsn` = commit LSN
```

Because the input is *physical* WAL, the same decoder can later attach to a
[Neon](https://github.com/neondatabase/neon) safekeeper stream instead of a
vanilla Postgres — that's the road to storage-level LTAP: every table,
automatically, with zero load on the transactional compute.

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

## Milestones

- **M0 (done)** — single table, INSERT-only, fixed schema, vanilla PG source
- **M1** — multi-insert (`COPY`), CRC validation, restart/resume from a
  persisted LSN (stored in Delta commit metadata → exactly-once), batching
  (currently one Delta commit per Postgres commit = small files)
- **M2** — UPDATE/DELETE via Delta deletion vectors; subtransactions;
  TOAST + inline-compressed values; full-page-image handling
  (`full_page_writes=on`); initial snapshot + consistent cutover
- **M3** — WAL-driven catalog tracking (DDL mid-stream, relfilenode changes,
  add/drop column), multiple tables, every-table-automatically
- **M4** — the LTAP freshness read path: serve "Delta up to LSN X + in-memory
  tail" merged reads so analytics see read-your-writes without touching PG
- **M5** — Neon safekeeper source: attach the same decoder to Neon's WAL
  service; evaluate Databricks' announced LTAP Writer Library when it lands

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
frontend protocol v3 directly (`src/pgwire.rs`) — small enough to own, and
the second source implementation (safekeepers) won't look like libpq anyway.

License: Apache-2.0.
