#!/bin/bash
# Read the transcoded Delta table from MinIO with DuckDB.
#   verify.sh [table] [pk-column]
# Shows change-log stats, then the current state (latest version per key,
# tombstones removed).
set -euo pipefail

TABLE="${1:-t}"
PK="${2:-id}"
DUCKDB="${DUCKDB:-$(command -v duckdb || echo "$HOME/.duckdb/cli/latest/duckdb")}"

"$DUCKDB" -c "
INSTALL delta; LOAD delta;
CREATE OR REPLACE SECRET minio (
    TYPE s3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
);
-- change log (all versions, tombstones included)
SELECT count(*) AS changelog_rows,
       sum(_ltap_deleted::int) AS tombstones,
       max(_ltap_lsn) AS max_commit_lsn
FROM delta_scan('s3://lake/${TABLE}');
-- current state
SELECT * EXCLUDE (_ltap_lsn, _ltap_seq, _ltap_deleted, _ltap_ctid)
FROM delta_scan('s3://lake/${TABLE}')
QUALIFY row_number() OVER (PARTITION BY ${PK} ORDER BY _ltap_lsn DESC, _ltap_seq DESC) = 1
    AND NOT _ltap_deleted
ORDER BY ${PK};
"
