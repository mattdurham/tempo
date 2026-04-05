#!/usr/bin/env bash
# Test vector search end-to-end:
# 1. Send database-error spans with varying specificity
# 2. Wait for block builder to flush + compact
# 3. Run VECTOR_ALL queries and verify ranking
set -euo pipefail

TEMPO_URL="http://localhost:13201"
OTLP_URL="http://localhost:14320"
OTLP_ENDPOINT="${OTLP_URL}/v1/traces"

echo "=== Step 1: Send database-error traces ==="

# Helper: send a trace with one span
send_span() {
  local svc="$1" span_name="$2" status_code="$3" attrs="$4"
  local trace_id
  trace_id=$(python3 -c "import os; print(os.urandom(16).hex())")
  local span_id
  span_id=$(python3 -c "import os; print(os.urandom(8).hex())")
  local now_ns
  now_ns=$(python3 -c "import time; print(int(time.time()*1e9))")
  local end_ns=$((now_ns + 50000000))  # 50ms duration

  local body
  body=$(cat <<EOJSON
{
  "resourceSpans": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "$svc"}}
      ]
    },
    "scopeSpans": [{
      "spans": [{
        "traceId": "$trace_id",
        "spanId": "$span_id",
        "name": "$span_name",
        "kind": 3,
        "startTimeUnixNano": "$now_ns",
        "endTimeUnixNano": "$end_ns",
        "status": {"code": $status_code},
        "attributes": [$attrs]
      }]
    }]
  }]
}
EOJSON
  )
  curl -s -X POST "$OTLP_ENDPOINT" \
    -H "Content-Type: application/json" \
    -d "$body" > /dev/null
  echo "  Sent: svc=$svc name='$span_name'"
}

# --- Broad: generic database errors across services ---
for i in $(seq 1 5); do
  send_span "api-gateway" "SELECT * FROM users" 2 \
    '{"key":"db.system","value":{"stringValue":"postgresql"}},{"key":"db.statement","value":{"stringValue":"SELECT * FROM users WHERE id = ?"}},{"key":"error.message","value":{"stringValue":"connection refused"}}'
  send_span "payment-svc" "INSERT INTO orders" 2 \
    '{"key":"db.system","value":{"stringValue":"mysql"}},{"key":"db.statement","value":{"stringValue":"INSERT INTO orders (user_id, amount) VALUES (?, ?)"}},{"key":"error.message","value":{"stringValue":"deadlock detected"}}'
  send_span "auth-svc" "SELECT FROM sessions" 2 \
    '{"key":"db.system","value":{"stringValue":"postgresql"}},{"key":"db.statement","value":{"stringValue":"SELECT * FROM sessions WHERE token = ?"}},{"key":"error.message","value":{"stringValue":"too many connections"}}'
done

# --- Medium: database errors on prod-01 ---
for i in $(seq 1 5); do
  send_span "prod-01" "SELECT FROM inventory" 2 \
    '{"key":"db.system","value":{"stringValue":"postgresql"}},{"key":"db.statement","value":{"stringValue":"SELECT * FROM inventory WHERE sku = ?"}},{"key":"error.message","value":{"stringValue":"connection timeout after 5s"}},{"key":"host.name","value":{"stringValue":"prod-01"}}'
  send_span "prod-01" "UPDATE inventory SET stock" 2 \
    '{"key":"db.system","value":{"stringValue":"postgresql"}},{"key":"db.statement","value":{"stringValue":"UPDATE inventory SET stock = stock - 1 WHERE sku = ?"}},{"key":"error.message","value":{"stringValue":"query cancelled: statement timeout"}},{"key":"host.name","value":{"stringValue":"prod-01"}}'
done

# --- Specific: database errors on prod-01 with timeout ---
for i in $(seq 1 5); do
  send_span "prod-01" "SELECT FROM large_table" 2 \
    '{"key":"db.system","value":{"stringValue":"postgresql"}},{"key":"db.statement","value":{"stringValue":"SELECT * FROM large_table WHERE created_at > now() - interval 30 day"}},{"key":"error.message","value":{"stringValue":"canceling statement due to statement timeout"}},{"key":"host.name","value":{"stringValue":"prod-01"}},{"key":"db.operation","value":{"stringValue":"SELECT"}},{"key":"timeout.duration_ms","value":{"intValue":30000}}'
  send_span "prod-01" "JOIN query timeout" 2 \
    '{"key":"db.system","value":{"stringValue":"postgresql"}},{"key":"db.statement","value":{"stringValue":"SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id WHERE o.created_at > ?"}},{"key":"error.message","value":{"stringValue":"canceling statement due to statement timeout"}},{"key":"host.name","value":{"stringValue":"prod-01"}},{"key":"db.operation","value":{"stringValue":"SELECT"}},{"key":"timeout.duration_ms","value":{"intValue":30000}}'
done

# --- Non-database spans (noise) ---
for i in $(seq 1 10); do
  send_span "frontend" "GET /api/health" 0 \
    '{"key":"http.method","value":{"stringValue":"GET"}},{"key":"http.route","value":{"stringValue":"/api/health"}},{"key":"http.status_code","value":{"intValue":200}}'
  send_span "cache-svc" "redis GET" 0 \
    '{"key":"db.system","value":{"stringValue":"redis"}},{"key":"db.operation","value":{"stringValue":"GET"}}'
done

echo ""
echo "=== Step 2: Wait for block builder to flush ==="
echo "  Waiting 30s for block builder cycle + embedding..."
sleep 30

# Check if Tempo is ready
until curl -sf "$TEMPO_URL/ready" > /dev/null 2>&1; do
  echo "  Tempo not ready, waiting..."
  sleep 5
done
echo "  Tempo is ready."

# Wait a bit more for compaction
echo "  Waiting 30s more for compaction..."
sleep 30

echo ""
echo "=== Step 3: Run vector search queries ==="
echo ""

run_query() {
  local label="$1" query="$2"
  echo "--- Query: $label ---"
  echo "  TraceQL: $query"
  local result
  result=$(curl -s -G "$TEMPO_URL/api/search" \
    --data-urlencode "q=$query" \
    --data-urlencode "start=1700000000" \
    --data-urlencode "end=2147483647" \
    --data-urlencode "limit=5" 2>&1)

  local count
  count=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('traces',[])))" 2>/dev/null || echo "ERROR")
  echo "  Results: $count traces"

  if [ "$count" != "0" ] && [ "$count" != "ERROR" ]; then
    echo "$result" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for t in d.get('traces', [])[:5]:
    svc = t.get('rootServiceName', '?')
    name = t.get('rootTraceName', '?')
    dur = t.get('durationMs', 0)
    print(f'    {svc} | {name} | {dur}ms')
" 2>/dev/null || echo "  (could not parse results)"
  else
    echo "  Raw: $(echo "$result" | head -c 200)"
  fi
  echo ""
}

# Broad query — should match all database errors
run_query "BROAD: database errors" '{ VECTOR_ALL("database errors") }'

# Medium query — should rank prod-01 higher
run_query "MEDIUM: database errors prod-01" '{ VECTOR_ALL("database errors prod-01") }'

# Specific query — should rank prod-01 timeout highest
run_query "SPECIFIC: database errors prod-01 timeout" '{ VECTOR_ALL("database errors prod-01 timeout") }'

echo "=== Done ==="
