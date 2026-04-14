---
name: create-issue
description: Create a TDD-structured GitHub issue for uc-catalog-mcp using RED→GREEN→REFACTOR flow
user-invocable: true
allowed-tools: Bash, Read, Glob, Grep
---

# Create GitHub Issue (TDD)

Create a well-structured GitHub issue following the project's TDD RED→GREEN→REFACTOR workflow.

## Usage

```
/create-issue <title> <scope description>
```

## Issue Template

Every issue MUST follow this structure:

```markdown
## Context

<1-2 sentences: why this component is needed, what problem it solves>

## Scope

Files to create or modify:
- `path/to/file.py` — what it does
- `tests/test_file.py` — what it tests

## RED — Write failing tests first

Before writing any implementation, write these tests (they must fail):

- `tests/test_X.py::test_name` — what the test asserts
- `tests/test_X.py::test_name2` — what the test asserts

Run `make test` — all new tests must be RED (failing) before implementation begins.

## GREEN — Minimal implementation

Write the minimum code to make the RED tests pass:

- `path/to/file.py`: implement `function_name()` — description
- Wire into `app/main.py` if applicable

Run `make test` — all tests must be GREEN before moving to REFACTOR.

## REFACTOR

With tests green, clean up:
- Remove duplication
- Improve naming
- Add agent-friendly docstrings to all public functions
- Update README tool table if a new MCP tool was added
- Add CHANGELOG.md entry

Run `make test` again — must stay GREEN.

## Acceptance Criteria

- [ ] All RED tests pass (GREEN)
- [ ] `make test` passes with no failures
- [ ] `make lint` passes (ruff)
- [ ] Every public function has an agent-friendly docstring
- [ ] README updated if MCP tools added/changed
- [ ] CHANGELOG.md entry added

## Dependencies

Issues that must be merged before this one:
- #<issue_number> — <title>
```

## How to use this skill

1. Gather context from `/workspace/group/.omc/specs/deep-interview-uc-catalog-mcp.md`
2. Identify the component's files, tests, and dependencies
3. Fill in the template above
4. Run: `gh issue create --repo IceRhymers/uc-catalog-mcp --title "feat: <title>" --body "..."`
5. Apply labels: `tdd` + component label

## Labels

- `tdd` — all issues created with this skill
- `scaffold` — project setup, pyproject.toml, Makefile
- `schema` — Lakebase schema, migrations, models
- `db` — database client, connection pooling
- `sync` — ETL sync job, hash, embeddings
- `mcp-tool` — MCP tool implementations
- `deployment` — databricks.yml, deploy.sh, Makefile targets
