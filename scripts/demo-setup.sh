##!/bin/bash

set -e

#### ============================================
#### Colors for output
#### ============================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

#### ============================================
#### Load env vars from demo .env
#### ============================================
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../demo/.env"
DEMO_DIR="${SCRIPT_DIR}/../demo"

echo -e "${YELLOW}============================================${NC}"
echo -e "${YELLOW}  Snowplow Demo Setup${NC}"
echo -e "${YELLOW}============================================${NC}"

if [ -f "$ENV_FILE" ]; then
    export $(cat "$ENV_FILE" | grep -v '^##' | grep -v '^$' | xargs)
    echo -e "${GREEN}✅ .env file loaded${NC}"
else
    echo -e "${RED}❌ Error: .env file not found at $ENV_FILE${NC}"
    echo "Please copy demo/.env.example to demo/.env"
    exit 1
fi

#### ============================================
#### Validate required vars
#### ============================================
echo ""
echo -e "${YELLOW}Validating environment variables...${NC}"

required_vars=(
    KAFKA_BROKERS
    KAFKA_TOPIC_GOOD
    KAFKA_TOPIC_BAD
    KAFKA_TOPIC_ENRICHED_GOOD
    KAFKA_TOPIC_ENRICHED_BAD
    KAFKA_GROUP_NAME
    COLLECTOR_PORT
    IGLU_PORT
    IGLU_SUPER_API_KEY
    IGLU_VENDOR_PREFIX
    IGLU_DB_HOST
    IGLU_DB_PORT
    IGLU_DB_NAME
    IGLU_DB_USER
    IGLU_DB_PASSWORD
    POSTGRES_USER
    POSTGRES_PASSWORD
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo -e "${RED}❌ Error: required variable $var is not set${NC}"
        exit 1
    fi
done

echo -e "${GREEN}✅ All required variables present${NC}"

#### ============================================
#### Check required clients installed
#### ============================================
echo ""
echo -e "${YELLOW}Checking required clients...${NC}"

if ! command -v psql &> /dev/null; then
    echo -e "${RED}❌ psql client not found${NC}"
    echo "Please install: apt-get install -y postgresql-client"
    exit 1
fi
echo -e "${GREEN}✅ psql client found${NC}"

if ! command -v envsubst &> /dev/null; then
    echo -e "${RED}❌ envsubst not found${NC}"
    echo "Please install: apt-get install -y gettext-base"
    exit 1
fi
echo -e "${GREEN}✅ envsubst found${NC}"

#### ============================================
#### Generate config files from templates
#### ============================================
echo ""
echo -e "${YELLOW}Generating config files from templates...${NC}"

CONFIG_DIR="${DEMO_DIR}/config"

envsubst < "${CONFIG_DIR}/iglu/iglu.hocon.template" \
    > "${CONFIG_DIR}/iglu/iglu.hocon"
echo -e "${GREEN}✅ iglu.hocon generated${NC}"

envsubst < "${CONFIG_DIR}/collector/config.hocon.template" \
    > "${CONFIG_DIR}/collector/config.hocon"
echo -e "${GREEN}✅ config.hocon generated${NC}"

envsubst < "${CONFIG_DIR}/enrich/enrich.hocon.template" \
    > "${CONFIG_DIR}/enrich/enrich.hocon"
echo -e "${GREEN}✅ enrich.hocon generated${NC}"

envsubst < "${CONFIG_DIR}/enrich/resolver.json.template" \
    > "${CONFIG_DIR}/enrich/resolver.json"
echo -e "${GREEN}✅ resolver.json generated${NC}"

#### ============================================
#### Start infrastructure containers
#### ============================================
echo ""
echo -e "${YELLOW}Starting demo containers...${NC}"

cd "${DEMO_DIR}"
docker compose up -d kafka clickhouse postgresql redis

echo -e "${GREEN}✅ Infrastructure containers started${NC}"

#### ============================================
#### Wait for Postgres to be ready
#### ============================================
echo ""
echo -e "${YELLOW}Waiting for PostgreSQL to be ready...${NC}"

MAX_RETRIES=30
COUNT=0

until PGPASSWORD="${POSTGRES_PASSWORD}" psql \
    -h localhost \
    -p 5432 \
    -U "${POSTGRES_USER}" \
    -c "SELECT 1" &>/dev/null; do
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}❌ PostgreSQL failed to start${NC}"
        exit 1
    fi
    echo "PostgreSQL not ready yet... (${COUNT}/${MAX_RETRIES})"
    sleep 3
done

echo -e "${GREEN}✅ PostgreSQL is ready${NC}"

#### ============================================
#### Create Iglu database
#### ============================================
echo ""
echo -e "${YELLOW}Setting up Iglu database...${NC}"

DB_EXISTS=$(PGPASSWORD="${POSTGRES_PASSWORD}" psql \
    -h localhost \
    -p 5432 \
    -U "${POSTGRES_USER}" \
    -tAc "SELECT 1 FROM pg_database WHERE datname='${IGLU_DB_NAME}'" 2>/dev/null || echo "")

if [ "$DB_EXISTS" = "1" ]; then
    echo -e "${GREEN}✅ Database ${IGLU_DB_NAME} already exists${NC}"
else
    PGPASSWORD="${POSTGRES_PASSWORD}" psql \
        -h localhost \
        -p 5432 \
        -U "${POSTGRES_USER}" \
        -c "CREATE DATABASE ${IGLU_DB_NAME};"
    echo -e "${GREEN}✅ Database ${IGLU_DB_NAME} created${NC}"
fi

#### ============================================
#### Run Iglu DB migrations
#### ============================================
echo ""
echo -e "${YELLOW}Running Iglu database migrations...${NC}"

docker run --rm \
    --network demo_demo-net \
    -v "${CONFIG_DIR}/iglu/iglu.hocon:/iglu/iglu.hocon" \
    snowplow/iglu-server:0.12.0 \
    setup --config /iglu/iglu.hocon

echo -e "${GREEN}✅ Iglu database migrations complete${NC}"

#### ============================================
#### Wait for Kafka to be ready
#### ============================================
echo ""
echo -e "${YELLOW}Waiting for Kafka to be ready...${NC}"

COUNT=0
until docker exec demo-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server kafka:9092 &>/dev/null; do
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}❌ Kafka failed to start${NC}"
        exit 1
    fi
    echo "Kafka not ready yet... (${COUNT}/${MAX_RETRIES})"
    sleep 3
done

echo -e "${GREEN}✅ Kafka is ready${NC}"

#### ============================================
#### Create Kafka topics
#### ============================================
echo ""
echo -e "${YELLOW}Creating Kafka topics...${NC}"

for topic in ${KAFKA_TOPIC_GOOD} ${KAFKA_TOPIC_BAD} ${KAFKA_TOPIC_ENRICHED_GOOD} ${KAFKA_TOPIC_ENRICHED_BAD}; do
    docker exec demo-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:9092 \
        --create \
        --if-not-exists \
        --topic $topic \
        --partitions 1 \
        --replication-factor 1
    echo -e "${GREEN}✅ Topic ${topic} created/exists${NC}"
done

#### ============================================
#### Run ClickHouse migrations
#### ============================================
echo ""
echo -e "${YELLOW}Waiting for ClickHouse to be ready...${NC}"

COUNT=0
until docker exec demo-clickhouse clickhouse-client --query "SELECT 1" &>/dev/null; do
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}❌ ClickHouse failed to start${NC}"
        exit 1
    fi
    echo "ClickHouse not ready yet... (${COUNT}/${MAX_RETRIES})"
    sleep 3
done

echo -e "${GREEN}✅ ClickHouse is ready${NC}"

echo ""
echo -e "${YELLOW}Running ClickHouse migrations...${NC}"

MIGRATIONS_DIR="${SCRIPT_DIR}/../clickhouse/migrations/demo"

for migration in ${MIGRATIONS_DIR}/*.sql; do
    echo "Running: $(basename $migration)"
    envsubst < "$migration" | docker exec -i demo-clickhouse \
        clickhouse-client \
        --user "${CLICKHOUSE_USER:-default}" \
        --password "${CLICKHOUSE_PASSWORD:-}" \
        --multiquery
    echo -e "${GREEN}✅ $(basename $migration) done${NC}"
done

#### ============================================
#### Start remaining containers
#### ============================================
echo ""
echo -e "${YELLOW}Starting Snowplow and Temporal containers...${NC}"

cd "${DEMO_DIR}"
docker compose up -d

echo -e "${GREEN}✅ All containers started${NC}"

#### ============================================
#### Wait for Iglu to be ready
#### ============================================
echo ""
echo -e "${YELLOW}Waiting for Iglu server to be ready...${NC}"

IGLU_URL="http://localhost:${IGLU_PORT}"
COUNT=0

until curl -s "${IGLU_URL}/api/meta/health" | grep -q "OK"; do
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}❌ Iglu server failed to start${NC}"
        exit 1
    fi
    echo "Iglu not ready yet... (${COUNT}/${MAX_RETRIES})"
    sleep 3
done

echo -e "${GREEN}✅ Iglu server is ready${NC}"

#### ============================================
#### Generate vendor API key
#### ============================================
echo ""
echo -e "${YELLOW}Generating API key for ${IGLU_VENDOR_PREFIX}...${NC}"

KEYGEN_RESPONSE=$(curl -s \
    -X POST \
    "${IGLU_URL}/api/auth/keygen" \
    -H "apikey: ${IGLU_SUPER_API_KEY}" \
    -H "Content-Type: application/json" \
    -d "{\"vendorPrefix\":\"${IGLU_VENDOR_PREFIX}\"}")

WRITE_KEY=$(echo $KEYGEN_RESPONSE | grep -o '"write":"[^"]*"' | cut -d'"' -f4)

if [ -z "$WRITE_KEY" ]; then
    echo -e "${RED}❌ Failed to get write key from Iglu${NC}"
    echo "Response was: ${KEYGEN_RESPONSE}"
    exit 1
fi

echo -e "${GREEN}✅ Write key obtained${NC}"

#### ============================================
#### Register user_context schema
#### ============================================
echo ""
echo -e "${YELLOW}Registering user_context schema...${NC}"

USER_CONTEXT_SCHEMA=$(cat << 'SCHEMA'
{"$schema":"http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#","description":"Schema for user context","self":{"vendor":"com.acme","name":"user_context","format":"jsonschema","version":"1-0-0"},"type":"object","properties":{"clid":{"type":"string","description":"Client ID"}},"required":["clid"],"additionalProperties":false}
SCHEMA
)

SCHEMA_RESPONSE=$(curl -s \
    -X POST \
    "${IGLU_URL}/api/schemas/" \
    -H "Content-Type: application/json" \
    -H "apikey: ${WRITE_KEY}" \
    -d "${USER_CONTEXT_SCHEMA}")

if echo "$SCHEMA_RESPONSE" | grep -q '"status":201\|created'; then
    echo -e "${GREEN}✅ user_context schema registered${NC}"
else
    echo -e "${YELLOW}⚠️  Schema may already exist or unexpected response${NC}"
    echo "Response: ${SCHEMA_RESPONSE}"
fi

#### ============================================
#### Save write key to .env
#### ============================================
if grep -q "IGLU_WRITE_KEY=" "${ENV_FILE}"; then
    sed -i "s/IGLU_WRITE_KEY=.*/IGLU_WRITE_KEY=${WRITE_KEY}/" "${ENV_FILE}"
else
    echo "IGLU_WRITE_KEY=${WRITE_KEY}" >> "${ENV_FILE}"
fi
echo -e "${GREEN}✅ Write key saved to .env${NC}"

#### ============================================
#### Wait for Collector to be ready
#### ============================================
echo ""
echo -e "${YELLOW}Waiting for Snowplow collector to be ready...${NC}"

COLLECTOR_URL="http://localhost:${COLLECTOR_PORT}"
COUNT=0

until curl -s -o /dev/null -w "%{http_code}" "${COLLECTOR_URL}/health" | grep -q "200"; do
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}❌ Collector failed to start${NC}"
        exit 1
    fi
    echo "Collector not ready yet... (${COUNT}/${MAX_RETRIES})"
    sleep 3
done

echo -e "${GREEN}✅ Collector is ready${NC}"

#### ============================================
#### Final status
#### ============================================
echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  Demo Started Successfully!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo "Services running:"
echo "  Collector:    http://localhost:${COLLECTOR_PORT}"
echo "  Iglu:         http://localhost:${IGLU_PORT}"
echo "  Temporal UI:  http://localhost:8200"
echo "  ClickHouse:   http://localhost:8123"
echo "  PostgreSQL:   localhost:5432"
echo "  Redis:        localhost:6379"
echo ""
echo "Next steps:"
echo "  Send test event: curl -X POST http://localhost:${COLLECTOR_PORT}/com.snowplowanalytics.snowplow/tp2 ..."
echo "  Check Temporal:  http://localhost:8200"
echo -e "${GREEN}============================================${NC}"
