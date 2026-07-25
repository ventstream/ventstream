#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONTROL_PLANE_DIR="${VENTSTREAM_CONTROL_PLANE_DIR:-$(cd "$ROOT_DIR/.." && pwd)/ventstream-control-plane}"
STATE_DIR="$ROOT_DIR/demo/fleet/.state"
PROFILE="local-fleet-demo"
CONTROL_PROJECT="ventstream-full-demo-control"
ENGINE_PROJECT="ventstream-full-demo-engine"
CONTROL_API="http://localhost:18080"
MAILPIT_PORT="18025"
PG_PORT="15544"
OS_PORT="19200"
WORKER_ORGANIZATIONS="01900000-0000-7000-8000-000000000001"

control_compose() {
  FLEET_DEV_POSTGRES_PORT=55437 FLEET_DEV_CONTROL_API_PORT=18080 \
    FLEET_DEV_MAILPIT_PORT="$MAILPIT_PORT" FLEET_DEV_MAILPIT_SMTP_PORT=11025 \
    FLEET_DEV_WORKER_ORGANIZATIONS="$WORKER_ORGANIZATIONS" \
    FLEET_DEV_GATEWAY_HEALTH_PORT=18081 FLEET_DEV_GATEWAY_ENROLLMENT_PORT=18444 \
    FLEET_DEV_GATEWAY_CONTROL_PORT=18445 \
    docker compose -p "$CONTROL_PROJECT" -f "$CONTROL_PLANE_DIR/compose.yaml" "$@"
}

engine_compose() {
  VENTSTREAM_DEMO_PG_PORT="$PG_PORT" VENTSTREAM_DEMO_OS_PORT="$OS_PORT" \
    docker compose -p "$ENGINE_PROJECT" -f "$ROOT_DIR/demo/stack/docker-compose.yml" "$@"
}

ctl() {
  VENTSTREAMCTL_CONFIG="$STATE_DIR/ventstreamctl.json" \
    "$CONTROL_PLANE_DIR/target/debug/ventstreamctl" --profile "$PROFILE" "$@"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

require_demo() {
  [[ -f "$STATE_DIR/deployment-id" ]] || {
    echo "The demo is not initialized. Run: $0 start" >&2
    exit 1
  }
}

stop_agent() {
  [[ -f "$STATE_DIR/agent.pid" ]] || return 0
  local pid command
  pid="$(cat "$STATE_DIR/agent.pid")"
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  if [[ "$command" == *ventstream-fleet-agent* ]]; then
    kill "$pid" 2>/dev/null || true
    for _ in {1..20}; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.25
    done
  fi
  rm -f "$STATE_DIR/agent.pid"
}

wait_for_document() {
  for _ in {1..90}; do
    if curl -fsS "http://localhost:$OS_PORT/fleet-demo-orders/_search?q=order_id:ord-0001" 2>/dev/null \
      | jq -e '.hits.total.value > 0' >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for ord-0001. Inspect $STATE_DIR/agent.log" >&2
  return 1
}

wait_for_verification_token() {
  local message_id token
  for _ in {1..60}; do
    message_id="$(
      curl -fsS "http://localhost:$MAILPIT_PORT/api/v1/messages" 2>/dev/null \
        | jq -r '.messages[0].ID // empty' 2>/dev/null || true
    )"
    if [[ -n "$message_id" ]]; then
      token="$(
        curl -fsS "http://localhost:$MAILPIT_PORT/api/v1/message/$message_id" 2>/dev/null \
          | jq -r '.Text // empty' 2>/dev/null \
          | grep -Eo 'vsev1\.[0-9a-fA-F-]{36}\.[0-9a-fA-F-]{36}\.[0-9]+\.[A-Za-z0-9_-]+' \
          | head -n 1 || true
      )"
      if [[ -n "$token" ]]; then
        printf '%s' "$token"
        return 0
      fi
    fi
    sleep 1
  done
  echo "Timed out waiting for the local verification email in Mailpit" >&2
  return 1
}

write_engine_config() {
  local destination="$1" joins_path="$2"
  cat > "$destination" <<EOF
schema_version: 1
roles: [cdc]
source:
  kind: postgres
  postgres:
    host_ref: env:VS_PG_HOST
    port: $PG_PORT
    user: ventstream
    password_ref: env:VS_PG_PASSWORD
    database: shop
    publication: ventstream_shop
    slot: ventstream_fleet_demo_slot
    bootstrap: { mode: snapshot, chunk_size: 10000 }
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    index_routing: { strategy: fixed, name: fleet-demo-orders }
specs:
  joins: $joins_path
runtime:
  health_listen: 127.0.0.1:14043
  dlq_path: $STATE_DIR/runtime/dlq.jsonl
  joins:
    state_dir: $STATE_DIR/join-state
EOF
  chmod 600 "$destination"
}

launch_agent() {
  local pipeline_id deployment_id instance_id pid
  pipeline_id="$(cat "$STATE_DIR/pipeline-id")"
  deployment_id="$(cat "$STATE_DIR/deployment-id")"
  instance_id="$(cat "$STATE_DIR/instance-id")"
  if [[ -f "$STATE_DIR/agent.pid" ]]; then
    pid="$(cat "$STATE_DIR/agent.pid")"
    kill -0 "$pid" 2>/dev/null && return 0
  fi
  (
    cd "$STATE_DIR"
    nohup env RUST_LOG=info VS_ROLES=cdc VS_PG_HOST=127.0.0.1 \
      VS_PG_PASSWORD=ventstream VS_OS_ENDPOINT="http://127.0.0.1:$OS_PORT" \
      VS_FLEET_GATEWAY_URL="https://localhost:18445" \
      VS_FLEET_PIPELINE_ID="$pipeline_id" VS_FLEET_DEPLOYMENT_ID="$deployment_id" \
      VS_FLEET_INSTANCE_ID="$instance_id" \
      VS_FLEET_CREDENTIAL_STATE_PATH="$STATE_DIR/identity.json" \
      VS_FLEET_STATE_PATH="$STATE_DIR/management.json" \
      VS_FLEET_ENGINE_BIN="$ROOT_DIR/target/debug/ventstream" \
      VS_FLEET_ENGINE_CONFIG_PATH="$STATE_DIR/applied-configuration.json" \
      VS_FLEET_ENGINE_HEALTH_URL="http://127.0.0.1:14043/readyz" \
      "$CONTROL_PLANE_DIR/target/debug/ventstream-fleet-agent" \
      >> "$STATE_DIR/agent.log" 2>&1 &
    echo $! > "$STATE_DIR/agent.pid"
  )
}

ensure_agent() {
  require_demo
  launch_agent
  sleep 3
}

start() {
  for command in cargo curl docker jq openssl python3; do require_command "$command"; done
  [[ -f "$CONTROL_PLANE_DIR/Cargo.toml" ]] || {
    echo "Clone ventstream-control-plane next to ventstream, or set VENTSTREAM_CONTROL_PLANE_DIR." >&2
    exit 1
  }
  [[ ! -e "$STATE_DIR/deployment-id" ]] || {
    echo "The demo is already initialized. Run '$0 status' or '$0 reset'." >&2
    exit 1
  }

  install -d -m 700 "$STATE_DIR" "$STATE_DIR/runtime" "$STATE_DIR/join-state"
  echo "[1/6] Starting isolated Postgres and OpenSearch fixtures..."
  engine_compose up -d --wait postgres opensearch
  echo "[2/6] Starting the isolated Fleet control plane..."
  control_compose up -d --build --wait
  echo "[3/6] Building the CLI, Fleet agent, and engine..."
  cargo build --manifest-path "$CONTROL_PLANE_DIR/Cargo.toml" -p ventstreamctl -p ventstream-fleet-agent
  cargo build --manifest-path "$ROOT_DIR/Cargo.toml" -p ventstream

  local password verification_token organization_json organization_id
  local pipeline_json deployment_json pipeline_id deployment_id instance_id
  local configuration_json configuration_revision
  password="$(openssl rand -hex 24)"
  printf '%s' "$password" > "$STATE_DIR/password"
  chmod 600 "$STATE_DIR/password"
  export FLEET_DEMO_PASSWORD="$password"

  echo "[4/6] Creating the account, organization, pipeline, and deployment..."
  VENTSTREAMCTL_CONFIG="$STATE_DIR/ventstreamctl.json" \
    "$CONTROL_PLANE_DIR/target/debug/ventstreamctl" auth signup \
      --control-plane "$CONTROL_API" --profile "$PROFILE" \
      --email admin@ventstream.local --display-name "Local Demo Admin" \
      --password-env FLEET_DEMO_PASSWORD --allow-insecure-localhost >/dev/null
  verification_token="$(wait_for_verification_token)"
  export FLEET_DEMO_VERIFICATION_TOKEN="$verification_token"
  ctl auth verify-email --token-env FLEET_DEMO_VERIFICATION_TOKEN >/dev/null
  unset FLEET_DEMO_VERIFICATION_TOKEN
  organization_json="$(ctl orgs create local-demo --display-name "Local Demo" \
    --environment development:Development --output json)"
  organization_id="$(jq -er '.data.id' <<<"$organization_json")"
  printf '%s\n' "$organization_id" > "$STATE_DIR/organization-id"
  WORKER_ORGANIZATIONS="$organization_id"
  control_compose up -d --no-deps --force-recreate --wait control-worker >/dev/null
  pipeline_json="$(ctl pipelines create orders-cdc \
    --description "Local Postgres to OpenSearch demo" --workload-kind cdc \
    --source-kind postgres --sink-kind opensearch --output json)"
  pipeline_id="$(jq -er '.data.id' <<<"$pipeline_json")"
  deployment_json="$(ctl agents create orders-worker --pipeline "$pipeline_id" --output json)"
  deployment_id="$(jq -er '.data.id' <<<"$deployment_json")"
  printf '%s\n' "$pipeline_id" > "$STATE_DIR/pipeline-id"
  printf '%s\n' "$deployment_id" > "$STATE_DIR/deployment-id"

  control_compose exec -T agent-gateway cat /var/run/ventstream-gateway/local-ca.crt \
    > "$STATE_DIR/local-ca.crt"
  ctl agents enroll-token create "$deployment_id" --pipeline "$pipeline_id" \
    > "$STATE_DIR/enrollment-token"
  chmod 600 "$STATE_DIR/enrollment-token"

  write_engine_config "$STATE_DIR/managed-ventstream.yaml" "orders.yaml"

  configuration_json="$(ctl pipelines configurations create "$pipeline_id" \
    --engine-config "$STATE_DIR/managed-ventstream.yaml" \
    --file "orders.yaml=$ROOT_DIR/demo/stack/specs/orders.yaml" --output json)"
  configuration_revision="$(jq -er '.data.revision' <<<"$configuration_json")"
  ctl pipelines configurations validate "$pipeline_id" "$configuration_revision" \
    --reason "local demo" >/dev/null
  ctl pipelines configurations select "$pipeline_id" "$configuration_revision" \
    --reason "local demo" >/dev/null

  echo "[5/6] Enrolling a real managed engine agent..."
  VS_FLEET_ENROLLMENT_URL="https://localhost:18444" \
  VS_FLEET_ENROLLMENT_TOKEN_PATH="$STATE_DIR/enrollment-token" \
  VS_FLEET_ENROLLMENT_TRUST_BUNDLE_PATH="$STATE_DIR/local-ca.crt" \
  VS_FLEET_DEPLOYMENT_ID="$deployment_id" \
  VS_FLEET_CREDENTIAL_STATE_PATH="$STATE_DIR/identity.json" \
    "$CONTROL_PLANE_DIR/target/debug/ventstream-fleet-agent" enroll >/dev/null
  rm -f "$STATE_DIR/enrollment-token"

  instance_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  printf '%s\n' "$instance_id" > "$STATE_DIR/instance-id"
  : > "$STATE_DIR/agent.log"
  launch_agent

  echo "[6/6] Applying configuration and starting the managed engine..."
  ctl pipelines configurations apply "$pipeline_id" "$configuration_revision" \
    --reason "local demo" --wait >/dev/null
  ctl pipelines resume "$pipeline_id" --wait >/dev/null
  wait_for_document
  echo
  echo "Demo ready. Postgres changes are flowing through a Fleet-managed engine."
  echo "Run '$0 status', '$0 change', '$0 pause', or '$0 resume'."
}

status() {
  ensure_agent
  ctl pipelines list
  echo
  ctl agents status --pipeline "$(cat "$STATE_DIR/pipeline-id")" \
    --deployment "$(cat "$STATE_DIR/deployment-id")"
  echo
  curl -fsS "http://localhost:$OS_PORT/fleet-demo-orders/_count" | jq .
}

change() {
  ensure_agent
  local marker="fleet-demo-$(date +%s)"
  engine_compose exec -T postgres psql -U ventstream -d shop -v ON_ERROR_STOP=1 \
    -c "UPDATE shop.orders SET status = '$marker' WHERE order_id = 'ord-0001';" >/dev/null
  for _ in {1..60}; do
    if curl -fsS "http://localhost:$OS_PORT/fleet-demo-orders/_search?q=order_id:ord-0001" \
      | jq -e --arg marker "$marker" '.hits.hits[0]._source.status == $marker' >/dev/null; then
      echo "CDC verified: ord-0001 status is now $marker"
      return 0
    fi
    sleep 1
  done
  echo "The update did not reach OpenSearch within 60 seconds." >&2
  return 1
}

pause() { ensure_agent; ctl pipelines pause "$(cat "$STATE_DIR/pipeline-id")" --reason "local demo" --wait; }
resume() { ensure_agent; ctl pipelines resume "$(cat "$STATE_DIR/pipeline-id")" --wait; }
logs() { require_demo; tail -n 100 -f "$STATE_DIR/agent.log"; }

stop() {
  stop_agent
  engine_compose down --remove-orphans
  control_compose down --remove-orphans
  echo "Demo stopped. Run '$0 reset' to also remove its state and volumes."
}

reset() {
  stop_agent
  engine_compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  control_compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$STATE_DIR"
  echo "Local Fleet demo state removed."
}

case "${1:-}" in
  start) start ;; status) status ;; change) change ;; pause) pause ;;
  resume) resume ;; logs) logs ;;
  ctl) shift; require_demo; ctl "$@" ;;
  stop) stop ;; reset) reset ;;
  *) echo "Usage: $0 {start|status|change|pause|resume|logs|ctl|stop|reset}" >&2; exit 2 ;;
esac
