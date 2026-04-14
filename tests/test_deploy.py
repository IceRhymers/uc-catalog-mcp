"""Tests for deployment automation — RED first before any files exist."""

import stat
from pathlib import Path

import yaml


def test_databricks_yml_valid_yaml():
    """databricks.yml parses as valid YAML."""
    content = Path("databricks.yml").read_text()
    parsed = yaml.safe_load(content)
    assert parsed is not None


def test_databricks_yml_has_app_resource():
    """databricks.yml defines an app resource under resources.apps."""
    parsed = yaml.safe_load(Path("databricks.yml").read_text())
    resources = parsed.get("resources", {})
    assert "apps" in resources, "resources.apps not found in databricks.yml"


def test_databricks_yml_has_job_resource():
    """databricks.yml defines a job resource under resources.jobs."""
    parsed = yaml.safe_load(Path("databricks.yml").read_text())
    resources = parsed.get("resources", {})
    assert "jobs" in resources, "resources.jobs not found in databricks.yml"


def test_databricks_yml_has_allowlist_variable():
    """databricks.yml has a variables section with catalog_allowlist."""
    parsed = yaml.safe_load(Path("databricks.yml").read_text())
    variables = parsed.get("variables", {})
    assert "catalog_allowlist" in variables


def test_databricks_yml_has_sync_schedule():
    """Sync job in databricks.yml has a schedule entry."""
    parsed = yaml.safe_load(Path("databricks.yml").read_text())
    jobs = parsed.get("resources", {}).get("jobs", {})
    assert jobs, "No jobs defined"
    for job_name, job_def in jobs.items():
        if "sync" in job_name.lower():
            assert "schedule" in job_def, f"Job {job_name} missing schedule"
            return
    first_job = next(iter(jobs.values()))
    assert "schedule" in first_job


def test_deploy_script_exists_and_is_executable():
    """scripts/deploy.sh exists and has execute permission."""
    path = Path("scripts/deploy.sh")
    assert path.exists(), "scripts/deploy.sh not found"
    mode = path.stat().st_mode
    assert mode & stat.S_IXUSR, "scripts/deploy.sh is not executable"


def test_makefile_has_deploy_target():
    """Makefile contains a deploy: target."""
    content = Path("Makefile").read_text()
    assert "deploy:" in content


def test_makefile_has_migrate_target():
    """Makefile contains a migrate: target."""
    content = Path("Makefile").read_text()
    assert "migrate:" in content


def test_makefile_has_sync_target():
    """Makefile contains a sync: target."""
    content = Path("Makefile").read_text()
    assert "sync:" in content
