#!/bin/bash
# Freshness read: Delta table + the transcoder's in-memory tail, merged.
# Shows committed-but-not-yet-flushed rows that delta_scan alone can't see.
#   verify-fresh.sh [table] [pk-column] [min_lsn]
set -euo pipefail

TABLE="${1:-t}"
PK="${2:-id}"
MIN_LSN="${3:-}"
TAIL_URL="http://localhost:${LTAP_HTTP_PORT:-8088}/tail/${TABLE}.parquet"
[ -n "$MIN_LSN" ] && TAIL_URL="${TAIL_URL}?min_lsn=${MIN_LSN}"
DUCKDB="${DUCKDB:-$(command -v duckdb || echo "$HOME/.duckdb/cli/latest/duckdb")}"

# 204 = empty tail; substitute an impossible filter instead of a parquet read
if curl -sf -o /tmp/ltap-tail-$$.parquet "$TAIL_URL" && [ -s /tmp/ltap-tail-$$.parquet ]; then
    TAIL_SRC="SELECT * FROM read_parquet('/tmp/ltap-tail-$$.parquet')"
else
    TAIL_SRC="SELECT * FROM delta_scan('s3://lake/${TABLE}') WHERE 1=0"
fi

"$DUCKDB" -c "
INSTALL delta; LOAD delta;
CREATE OR REPLACE SECRET minio (
    TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin',
    ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false
);
WITH log AS (
    SELECT * FROM delta_scan('s3://lake/${TABLE}')
    UNION ALL BY NAME
    ${TAIL_SRC}
)
SELECT * EXCLUDE (_ltap_lsn, _ltap_seq, _ltap_deleted, _ltap_ctid)
FROM log
QUALIFY row_number() OVER (PARTITION BY ${PK} ORDER BY _ltap_lsn DESC, _ltap_seq DESC) = 1
    AND NOT _ltap_deleted
ORDER BY ${PK};
"
rm -f /tmp/ltap-tail-$$.parquet
