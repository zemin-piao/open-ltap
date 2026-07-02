#!/bin/bash
# Read the transcoded Delta table from MinIO with DuckDB.
set -euo pipefail

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
SELECT * FROM delta_scan('s3://lake/${1:-t}') ORDER BY _ltap_lsn;
SELECT count(*) AS rows, max(_ltap_lsn) AS max_commit_lsn FROM delta_scan('s3://lake/${1:-t}');
"
