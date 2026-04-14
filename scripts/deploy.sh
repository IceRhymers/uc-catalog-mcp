#!/usr/bin/env bash
# deploy.sh — end-to-end deployment for uc-catalog-mcp
# Mirrors databricks-claw deploy pattern: subcommands with --target support.
set -euo pipefail

TARGET="${DATABRICKS_BUNDLE_TARGET:-dev}"

COMMAND="${1:-help}"
shift || true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

usage() {
  cat <<EOF
Usage: $(basename "$0") <command> [--target <target>]

Commands:
  build     Install dependencies
  validate  Run databricks bundle validate
  deploy    Run databricks bundle deploy
  migrate   Run Alembic migrations
  start     Start the deployed app
  full      build + validate + deploy + migrate + start
  stop      Stop the running app
  help      Show this help message
EOF
}

cmd_build() {
  echo "==> Installing dependencies..."
  uv sync
}

cmd_validate() {
  echo "==> Validating bundle (target=$TARGET)..."
  databricks bundle validate --target "$TARGET"
}

cmd_deploy() {
  echo "==> Deploying bundle (target=$TARGET)..."
  databricks bundle deploy --target "$TARGET"
}

cmd_migrate() {
  local app_name instance_name
  app_name="$(resolve_app_name)"
  instance_name="$(resolve_instance_name)"
  echo "==> Running database migrations (app=$app_name, instance=$instance_name)..."
  uv run python scripts/migrate.py --app-name "$app_name" --instance "$instance_name"
}

cmd_start() {
  echo "==> Starting app (target=$TARGET)..."
  databricks bundle run uc_catalog_mcp --target "$TARGET"
}

_bundle_json=""
_resolve_bundle_json() {
  if [[ -z "$_bundle_json" ]]; then
    _bundle_json="$(databricks bundle validate --target "$TARGET" -o json 2>/dev/null)"
  fi
}

resolve_app_name() {
  _resolve_bundle_json
  echo "$_bundle_json" \
    | python3 -c "import sys,json; b=json.load(sys.stdin); print(list(b['resources']['apps'].values())[0]['name'])"
}

resolve_instance_name() {
  _resolve_bundle_json
  echo "$_bundle_json" \
    | python3 -c "import sys,json; b=json.load(sys.stdin); print(list(b['resources']['database_instances'].values())[0]['name'])"
}

cmd_full() {
  cmd_build
  cmd_validate
  cmd_deploy
  cmd_migrate
  cmd_start
}

cmd_stop() {
  local app_name
  app_name="$(resolve_app_name)"
  echo "==> Stopping app ($app_name)..."
  databricks apps stop "$app_name"
}

case "$COMMAND" in
  build)    cmd_build ;;
  validate) cmd_validate ;;
  deploy)   cmd_deploy ;;
  migrate)  cmd_migrate ;;
  start)    cmd_start ;;
  full)     cmd_full ;;
  stop)     cmd_stop ;;
  help|*)   usage ;;
esac
