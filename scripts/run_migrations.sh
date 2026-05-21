##!/bin/bash

set -e

### ============================================
### Load env vars from demo .env
### ============================================
ENV_FILE="$(dirname "$0")/../production/snowplow/.env"

if [ -f "$ENV_FILE" ]; then
    export $(cat "$ENV_FILE" | grep -v '^##' | grep -v '^$' | xargs)
else
    echo "Error: .env file not found at $ENV_FILE"
    echo "Please copy production/snowplow/.env.example to production/snowplow/.env"
    exit 1
fi

### ============================================
### Check required vars
### ============================================
required_vars=(
    CLICKHOUSE_HOST
    CLICKHOUSE_PORT
    CLICKHOUSE_USER
    CLICKHOUSE_PASSWORD
    CLICKHOUSE_CLUSTER
    DATABASE_NAME
    KAFKA_BROKERS
    KAFKA_TOPIC_ENRICHED_GOOD
    KAFKA_GROUP_NAME
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: required variable $var is not set"
        exit 1
    fi
done

### ============================================
### Migration runner function
### ============================================
MIGRATIONS_DIR="$(dirname "$0")/../clickhouse/migrations"

run_migration() {
    local file=$1
    echo "--------------------------------------------"
    echo "Running migration: $(basename $file)"
    echo "--------------------------------------------"

    envsubst < "$file" | clickhouse-client \
        --host="${CLICKHOUSE_HOST}" \
        --port="${CLICKHOUSE_PORT}" \
        --user="${CLICKHOUSE_USER}" \
        --password="${CLICKHOUSE_PASSWORD}" \
        --multiquery

    echo "Done: $(basename $file)"
}

### ============================================
### Run migrations in order
### ============================================
echo "============================================"
echo "Starting ClickHouse migrations"
echo "Host:     ${CLICKHOUSE_HOST}"
echo "Database: ${DATABASE_NAME}"
echo "Cluster:  ${CLICKHOUSE_CLUSTER}"
echo "============================================"

run_migration "${MIGRATIONS_DIR}/001_create_database.sql"
run_migration "${MIGRATIONS_DIR}/002_snowplow_events.sql"
run_migration "${MIGRATIONS_DIR}/003_snowplow_kafka.sql"
run_migration "${MIGRATIONS_DIR}/004_snowplow_mv.sql"

echo "============================================"
echo "All migrations completed successfully!"
echo "============================================"
