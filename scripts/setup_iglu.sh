##!/bin/bash

set -e

### ============================================
### Load env vars from snowplow .env
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
    IGLU_SUPER_API_KEY
    IGLU_PORT
    IGLU_VENDOR_PREFIX
    IGLU_DB_HOST
    IGLU_DB_PORT
    IGLU_DB_USER
    IGLU_DB_PASSWORD
    IGLU_DB_NAME
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: required variable $var is not set"
        exit 1
    fi
done

### ============================================
### Create igludb in Postgres if not exists
### ============================================
echo "============================================"
echo "Setting up Postgres database for Iglu"
echo "============================================"

DB_EXISTS=$(PGPASSWORD="${IGLU_DB_PASSWORD}" psql \
    -h "${IGLU_DB_HOST}" \
    -p "${IGLU_DB_PORT}" \
    -U "${IGLU_DB_USER}" \
    -tAc "SELECT 1 FROM pg_database WHERE datname='${IGLU_DB_NAME}'")

if [ "$DB_EXISTS" = "1" ]; then
    echo "Database ${IGLU_DB_NAME} already exists, skipping"
else
    echo "Creating database ${IGLU_DB_NAME}..."
    PGPASSWORD="${IGLU_DB_PASSWORD}" psql \
        -h "${IGLU_DB_HOST}" \
        -p "${IGLU_DB_PORT}" \
        -U "${IGLU_DB_USER}" \
        -c "CREATE DATABASE ${IGLU_DB_NAME};"
    echo "Database ${IGLU_DB_NAME} created!"
fi

### ============================================
### Wait for Iglu to be ready
### ============================================
IGLU_URL="http://localhost:${IGLU_PORT}"

echo "============================================"
echo "Waiting for Iglu server to be ready..."
echo "============================================"

until curl -s "${IGLU_URL}/api/meta/health" | grep -q "OK"; do
    echo "Iglu not ready yet, retrying in 3 seconds..."
    sleep 3
done

echo "Iglu server is ready!"

### ============================================
### Generate vendor API key
### ============================================
echo "--------------------------------------------"
echo "Generating API key for ${IGLU_VENDOR_PREFIX}"
echo "--------------------------------------------"

KEYGEN_RESPONSE=$(curl -s \
    -X POST \
    "${IGLU_URL}/api/auth/keygen" \
    -H "apikey: ${IGLU_SUPER_API_KEY}" \
    -H "Content-Type: application/json" \
    -d "{\"vendorPrefix\":\"${IGLU_VENDOR_PREFIX}\"}")

echo "Response: ${KEYGEN_RESPONSE}"

WRITE_KEY=$(echo $KEYGEN_RESPONSE | grep -o '"write":"[^"]*"' | cut -d'"' -f4)

if [ -z "$WRITE_KEY" ]; then
    echo "Error: failed to get write key from Iglu"
    exit 1
fi

echo "Write key obtained: ${WRITE_KEY}"

### ============================================
### Register user_context schema
### ============================================
echo "--------------------------------------------"
echo "Registering user_context schema"
echo "--------------------------------------------"

SCHEMA_RESPONSE=$(curl -s \
    -X POST \
    "${IGLU_URL}/api/schemas/" \
    -H "Content-Type: application/json" \
    -H "apikey: ${WRITE_KEY}" \
    -d '{
        "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0##",
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
    }')

echo "Response: ${SCHEMA_RESPONSE}"

if echo "$SCHEMA_RESPONSE" | grep -q '"status":201\|created'; then
    echo "user_context schema registered successfully!"
else
    echo "Warning: unexpected response for user_context schema"
fi

### ============================================
### Save write key to .env
### ============================================
echo "--------------------------------------------"
echo "Saving write key to .env"
echo "--------------------------------------------"

sed -i "s/IGLU_WRITE_KEY=/IGLU_WRITE_KEY=${WRITE_KEY}/" "$ENV_FILE"
echo "Write key saved to .env"

### ============================================
### Verify schemas registered
### ============================================
echo "--------------------------------------------"
echo "Verifying registered schemas"
echo "--------------------------------------------"

curl -s \
    "${IGLU_URL}/api/schemas/com.acme" \
    -H "apikey: ${IGLU_SUPER_API_KEY}" | python3 -m json.tool

echo "============================================"
echo "Iglu setup complete!"
echo "============================================"
