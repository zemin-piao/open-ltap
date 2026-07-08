#!/bin/bash
# Bring up the local Neon stack (neon-compose/) and prepare it for the
# transcoder: db `app`, demo table `t`, and the env the safekeeper source
# needs. The vanilla dev stack must be down first (both publish 5432≠ but
# share 9000 for MinIO).
#
#   ./scripts/neon-init.sh          # up + init + print env
#
# Then:  eval "$(./scripts/neon-init.sh env)" && cargo run -- t
set -euo pipefail
cd "$(dirname "$0")/../neon-compose"

PSQL=(docker compose exec -T compute1 psql -U cloud_admin -h localhost -p 55433)

if [ "${1:-}" = env ]; then
  TENANT=$("${PSQL[@]}" -d postgres -Atc "SHOW neon.tenant_id")
  TIMELINE=$("${PSQL[@]}" -d postgres -Atc "SHOW neon.timeline_id")
  cat <<EOF
export LTAP_SOURCE=safekeeper
export LTAP_SK_HOST=localhost LTAP_SK_PORT=5454
export LTAP_TENANT_ID=$TENANT LTAP_TIMELINE_ID=$TIMELINE
export PG_HOST=localhost PG_PORT=55433 PG_USER=cloud_admin PG_PASSWORD=cloud_admin PG_DB=app
export S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=minio S3_SECRET_KEY=password
EOF
  exit 0
fi

docker compose up -d --build

echo "waiting for compute..."
until "${PSQL[@]}" -d postgres -Atc "SELECT 1" >/dev/null 2>&1; do sleep 2; done

"${PSQL[@]}" -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname='app'" | grep -q 1 ||
  "${PSQL[@]}" -d postgres -c "CREATE DATABASE app"
"${PSQL[@]}" -d app -c 'CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY, body TEXT);'

echo
echo "neon init done. Tenant/timeline:"
"${PSQL[@]}" -d postgres -Atc "SHOW neon.tenant_id"
"${PSQL[@]}" -d postgres -Atc "SHOW neon.timeline_id"
echo
echo 'run:  eval "$(./scripts/neon-init.sh env)" && cargo run -- t'
