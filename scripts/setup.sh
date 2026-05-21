##!/bin/bash

set -e

### ============================================
### Colors for output
### ============================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

### ============================================
### Load env vars from snowplow .env
### ============================================
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../production/snowplow/.env"
SNOWPLOW_DIR="${SCRIPT_DIR}/../production/snowplow"

echo -e "${YELLOW}============================================${NC}"
echo -e "${YELLOW}  Snowplow Pipeline Setup${NC}"
echo -e "${YELLOW}============================================${NC}"

if [ -f "$ENV_FILE" ]; then
    export $(cat "$ENV_FILE" | grep -v '^###' | grep -v '^$' | xargs)
    echo -e "${GREEN}✅ .env file loaded${NC}"
else
    echo -e "${RED}❌ Error: .env file not found at $ENV_FILE${NC}"
    echo "Please copy production/snowplow/.env.example to production/snowplow/.env"
    exit 1
fi

### ============================================
### Validate required vars
### ============================================
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
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo -e "${RED}❌ Error: required variable $var is not set${NC}"
        exit 1
    fi
done

echo -e "${GREEN}✅ All required variables present${NC}"

### ============================================
### Check required clients installed
### ============================================
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

### ============================================
### Generate hocon files from templates
### ============================================
echo ""
echo -e "${YELLOW}Generating config files from templates...${NC}"

TEMPLATES_DIR="${SNOWPLOW_DIR}/config"

envsubst < "${TEMPLATES_DIR}/iglu/iglu.hocon.template" \
    > "${TEMPLATES_DIR}/iglu/iglu.hocon"
echo -e "${GREEN}✅ iglu.hocon generated${NC}"

envsubst < "${TEMPLATES_DIR}/collector/config.hocon.template" \
    > "${TEMPLATES_DIR}/collector/config.hocon"
echo -e "${GREEN}✅ config.hocon generated${NC}"

envsubst < "${TEMPLATES_DIR}/enrich/enrich.hocon.template" \
    > "${TEMPLATES_DIR}/enrich/enrich.hocon"
echo -e "${GREEN}✅ enrich.hocon generated${NC}"

### ============================================
### Create Postgres database for Iglu
### ============================================
echo ""
echo -e "${YELLOW}Setting up Postgres database for Iglu...${NC}"

DB_EXISTS=$(PGPASSWORD="${IGLU_DB_PASSWORD}" psql \
    -h "${IGLU_DB_HOST_EXTERNAL}" \
    -p "${IGLU_DB_PORT}" \
    -U "${IGLU_DB_USER}" \
    -tAc "SELECT 1 FROM pg_database WHERE datname='${IGLU_DB_NAME}'" 2>/dev/null || echo "")

if [ "$DB_EXISTS" = "1" ]; then
    echo -e "${GREEN}✅ Database ${IGLU_DB_NAME} already exists${NC}"
else
    echo "Creating database ${IGLU_DB_NAME}..."
    PGPASSWORD="${IGLU_DB_PASSWORD}" psql \
        -h "${IGLU_DB_HOST_EXTERNAL}" \
        -p "${IGLU_DB_PORT}" \
        -U "${IGLU_DB_USER}" \
        -c "CREATE DATABASE ${IGLU_DB_NAME};"
    echo -e "${GREEN}✅ Database ${IGLU_DB_NAME} created${NC}"
fi

### ============================================
### Run Iglu DB migrations
### ============================================
echo ""
echo -e "${YELLOW}Running Iglu database migrations...${NC}"

docker run --rm \
    --network infrastructure_default \
    -v "${TEMPLATES_DIR}/iglu/iglu.hocon:/iglu/iglu.hocon" \
    snowplow/iglu-server:0.12.0 \
    setup --config /iglu/iglu.hocon

echo -e "${GREEN}✅ Iglu database migrations complete${NC}"

### ============================================
### Start Snowplow containers
### ============================================
echo ""
echo -e "${YELLOW}Starting Snowplow containers...${NC}"

cd "${SNOWPLOW_DIR}"
docker compose up -d --force-recreate

echo -e "${GREEN}✅ Containers started${NC}"

### ============================================
### Wait for Iglu to be ready
### ============================================
echo ""
echo -e "${YELLOW}Waiting for Iglu server to be ready...${NC}"

IGLU_URL="http://${DOCKER_BIND_IP:-localhost}:${IGLU_PORT}"
MAX_RETRIES=30
COUNT=0

until curl -s "${IGLU_URL}/api/meta/health" | grep -q "OK"; do
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}❌ Iglu server failed to start after ${MAX_RETRIES} retries${NC}"
        echo "Check logs with: docker logs iglu-server"
        exit 1
    fi
    echo "Iglu not ready yet, retrying in 3 seconds... (${COUNT}/${MAX_RETRIES})"
    sleep 3
done

echo -e "${GREEN}✅ Iglu server is ready${NC}"

### ============================================
### Generate vendor API key
### ============================================
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

### ============================================
### Register user_context schema
### ============================================
echo ""
echo -e "${YELLOW}Registering user_context schema...${NC}"

USER_CONTEXT_SCHEMA=$(cat << 'SCHEMA'
{
    "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
    "description": "Schema for user context",
    "self": {
        "vendor": "com.acme",
        "name": "user_context",
        "format": "jsonschema",
        "version": "1-0-0"
    },
    "type": "object",
    "properties": {
        "test": {
            "type": "string",
            "description": "Test field"
        }
    },
    "required": ["test"],
    "additionalProperties": false
}
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

### ============================================
### Save write key to .env
### ============================================
if grep -q "IGLU_WRITE_KEY=" "${ENV_FILE}"; then
    sed -i "s/IGLU_WRITE_KEY=.*/IGLU_WRITE_KEY=${WRITE_KEY}/" "${ENV_FILE}"
else
    echo "IGLU_WRITE_KEY=${WRITE_KEY}" >> "${ENV_FILE}"
fi
echo -e "${GREEN}✅ Write key saved to .env${NC}"

### ============================================
### Wait for collector to be ready
### ============================================
echo ""
echo -e "${YELLOW}Waiting for Snowplow collector to be ready...${NC}"

COLLECTOR_URL="http://${DOCKER_BIND_IP:-localhost}:${COLLECTOR_PORT}"
COUNT=0

until curl -s -o /dev/null -w "%{http_code}" "${COLLECTOR_URL}/health" | grep -q "200"; do
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo -e "${RED}❌ Collector failed to start after ${MAX_RETRIES} retries${NC}"
        echo "Check logs with: docker logs scala-stream-collector"
        exit 1
    fi
    echo "Collector not ready yet, retrying in 3 seconds... (${COUNT}/${MAX_RETRIES})"
    sleep 3
done

echo -e "${GREEN}✅ Collector is ready${NC}"

### ============================================
### Final status
### ============================================
echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  Snowplow Pipeline Started Successfully!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo "Services running:"
echo "  Collector: http://localhost:${COLLECTOR_PORT}"
echo "  Iglu:      http://localhost:${IGLU_PORT}"
echo ""
echo "Next steps:"
echo "  Test pipeline:     ./scripts/test_snowplow.sh"
echo "  Run migrations:    ./scripts/run_migrations.sh"
echo "  Check logs:        docker logs scala-stream-collector"
echo "                     docker logs iglu-server"
echo "                     docker logs snowplow-enrich"
echo -e "${GREEN}============================================${NC}"
