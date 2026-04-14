#!/usr/bin/env bash
# deploy.sh — end-to-end deployment for uc-catalog-mcp
# Idempotent: safe to re-run after first deploy.
set -euo pipefail

echo "==> Deploying Databricks Asset Bundle..."
databricks bundle deploy

echo "==> Running database migrations..."
uv run python scripts/migrate.py

echo "==> Triggering initial sync job..."
databricks bundle run uc-catalog-sync || echo "Sync job triggered (or already running)"

echo "==> Deploy complete."
