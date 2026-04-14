"""Tests for deployment automation."""

import stat
from pathlib import Path

import yaml


def test_databricks_yml_valid_yaml():
    """databricks.yml parses as valid YAML."""
    parsed = yaml.safe_load(Path("databricks.yml").read_text())
    assert parsed is not None


def test_databricks_yml_uses_include():
    """databricks.yml uses include: - resources/*.yml pattern."""
    parsed = yaml.safe_load(Path("databricks.yml").read_text())
    assert "include" in parsed
    assert any("resources" in inc for inc in parsed["include"])


def test_databricks_yml_has_allowlist_variable():
    """databricks.yml has a variables section with catalog_allowlist."""
    parsed = yaml.safe_load(Path("databricks.yml").read_text())
    assert "catalog_allowlist" in parsed.get("variables", {})


def test_resources_app_yml_has_app():
    """resources/app.yml defines the app resource."""
    parsed = yaml.safe_load(Path("resources/app.yml").read_text())
    assert "apps" in parsed.get("resources", {})


def test_resources_lakebase_yml_has_database():
    """resources/lakebase.yml defines the database_instances resource."""
    parsed = yaml.safe_load(Path("resources/lakebase.yml").read_text())
    assert "database_instances" in parsed.get("resources", {})


def test_deploy_script_exists_and_is_executable():
    """scripts/deploy.sh exists and has execute permission."""
    path = Path("scripts/deploy.sh")
    assert path.exists(), "scripts/deploy.sh not found"
    assert path.stat().st_mode & stat.S_IXUSR, "scripts/deploy.sh is not executable"


def test_makefile_has_deploy_target():
    """Makefile contains a deploy: target."""
    assert "deploy:" in Path("Makefile").read_text()


def test_makefile_has_migrate_target():
    """Makefile contains a migrate: target."""
    assert "migrate:" in Path("Makefile").read_text()


def test_makefile_has_sync_target():
    """Makefile contains a sync: target."""
    assert "sync:" in Path("Makefile").read_text()