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
  echo "==> Running database migrations..."
  uv run python scripts/migrate.py
}

cmd_start() {
  echo "==> Starting app (target=$TARGET)..."
  databricks bundle run uc_catalog_mcp --target "$TARGET"
}

cmd_full() {
  cmd_build
  cmd_validate
  cmd_deploy
  cmd_migrate
  cmd_start
}

cmd_stop() {
  echo "==> Stopping app (target=$TARGET)..."
  databricks apps stop uc-catalog-mcp
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
