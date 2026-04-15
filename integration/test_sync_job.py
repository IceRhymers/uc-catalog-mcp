"""Integration test for sync/job.py.

Requires:
- A configured Databricks workspace (databricks-connect or active cluster session)
- Real Unity Catalog tables accessible via system.information_schema
- A real Lakebase instance (LAKEBASE_INSTANCE_NAME env var)
- CATALOG_ALLOWLIST env var set to a JSON list (e.g. '["main"]')

Run with: make test-integration
Never run in CI — this test requires real Spark and real Lakebase.
"""

import logging
import os

import pytest

# Skip entire module in CI (no Databricks credentials)
pytestmark = pytest.mark.skipif(
    os.environ.get("DATABRICKS_HOST") is None,
    reason="Integration test requires DATABRICKS_HOST — skipped in CI",
)


def test_sync_job_runs_and_returns_stats():
    """Full end-to-end: run_sync() returns a stats dict with expected keys."""
    from sync.job import run_sync

    # CATALOG_ALLOWLIST and LAKEBASE_INSTANCE_NAME must be set externally
    stats = run_sync()

    assert isinstance(stats, dict)
    for key in ("scanned", "skipped", "embedded", "deleted"):
        assert key in stats, f"Missing key: {key}"
        assert isinstance(stats[key], int), f"{key} must be int"

    logging.info("Integration sync stats: %s", stats)


def test_sync_job_idempotent():
    """Running sync twice: second run should skip all (hashes unchanged)."""
    from sync.job import run_sync

    stats1 = run_sync()
    stats2 = run_sync()

    # On second run everything should be skipped (no changes)
    assert stats2["embedded"] == 0, (
        f"Second run embedded {stats2['embedded']} tables — expected 0 (idempotent)"
    )
    assert stats2["skipped"] == stats1["scanned"], (
        f"Second run skipped={stats2['skipped']} but first scanned={stats1['scanned']}"
    )
