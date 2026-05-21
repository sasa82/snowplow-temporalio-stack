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

COLLECTOR_URL="http://localhost:${COLLECTOR_PORT}"

### ============================================
### Wait for collector to be ready
### ============================================
echo "============================================"
echo "Waiting for Snowplow collector to be ready..."
echo "============================================"

until curl -s "${COLLECTOR_URL}/health" | grep -q "OK"; do
    echo "Collector not ready yet, retrying in 3 seconds..."
    sleep 3
done

echo "Collector is ready!"

### ============================================
### Test 1 - Simple pageview event
### ============================================
echo "--------------------------------------------"
echo "Test 1: Sending pageview event"
echo "--------------------------------------------"

RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    "${COLLECTOR_URL}/com.snowplowanalytics.snowplow/tp2?\
aid=demo-app-id\
&tv=test-tracker-1.0\
&e=pv\
&url=https%3A%2F%2Fdemo.example.com%2Fhome\
&page=Home+Page\
&refr=https%3A%2F%2Fgoogle.com\
&lang=en-US\
&res=1920x1080\
&vp=1280x720" \
    --cookie "clid=demo-test-clid-123456789")

if [ "$RESPONSE" = "200" ] || [ "$RESPONSE" = "204" ]; then
    echo "✅ Test 1 passed - pageview event accepted (HTTP $RESPONSE)"
else
    echo "❌ Test 1 failed - unexpected response (HTTP $RESPONSE)"
fi

### ============================================
### Test 2 - Pageview with user_context
### ============================================
echo "--------------------------------------------"
echo "Test 2: Sending pageview with user_context"
echo "--------------------------------------------"

CONTEXT=$(python3 -c "
import json, base64
ctx = {
    'schema': 'iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1',
    'data': [{
        'schema': 'iglu:com.acme/user_context/jsonschema/1-0-0',
        'data': {
            'test': 'test_value'
        }
    }]
}
print(base64.urlsafe_b64encode(json.dumps(ctx).encode()).decode())
")

RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    "${COLLECTOR_URL}/com.snowplowanalytics.snowplow/tp2?\
aid=demo-app-id\
&tv=test-tracker-1.0\
&e=pv\
&url=https%3A%2F%2Fdemo.example.com%2Fproduct\
&page=Product+Page\
&cx=${CONTEXT}" \
    --cookie "clid=demo-test-clid-987654321")

if [ "$RESPONSE" = "200" ] || [ "$RESPONSE" = "204" ]; then
    echo "✅ Test 2 passed - pageview with context accepted (HTTP $RESPONSE)"
else
    echo "❌ Test 2 failed - unexpected response (HTTP $RESPONSE)"
fi

### ============================================
### Test 3 - Structured event
### ============================================
echo "--------------------------------------------"
echo "Test 3: Sending structured event"
echo "--------------------------------------------"

RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
    "${COLLECTOR_URL}/com.snowplowanalytics.snowplow/tp2?\
aid=demo-app-id\
&tv=test-tracker-1.0\
&e=se\
&se_ca=button\
&se_ac=click\
&se_la=signup_button\
&se_va=1\
&url=https%3A%2F%2Fdemo.example.com%2Fsignup" \
    --cookie "clid=demo-test-clid-111222333")

if [ "$RESPONSE" = "200" ] || [ "$RESPONSE" = "204" ]; then
    echo "✅ Test 3 passed - structured event accepted (HTTP $RESPONSE)"
else
    echo "❌ Test 3 failed - unexpected response (HTTP $RESPONSE)"
fi

### ============================================
### Summary
### ============================================
echo "============================================"
echo "Snowplow collector tests complete!"
echo ""
echo "Next steps to verify full pipeline:"
echo "1. Check Kafka topic has messages:"
echo "   kafka-console-consumer --topic ${KAFKA_TOPIC_ENRICHED_GOOD} \\"
echo "   --bootstrap-server ${KAFKA_BROKERS} --from-beginning"
echo ""
echo "2. Check ClickHouse has rows:"
echo "   SELECT count(*) FROM ${DATABASE_NAME}.snowplow_events"
echo "============================================"
