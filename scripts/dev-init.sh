#!/bin/bash
# One-time dev setup after `docker compose up -d`:
#  - allow replication connections with trust auth (dev only!)
#  - create the demo table
set -euo pipefail

docker exec openltap-pg bash -c '
  grep -q "host replication all all trust" "$PGDATA/pg_hba.conf" ||
    echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"
  psql -U postgres -q -c "SELECT pg_reload_conf()" > /dev/null
'
docker exec openltap-pg psql -U postgres -d app -c \
  'CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY, body TEXT);'

echo "dev init done: replication trust enabled, table 't' ready"
