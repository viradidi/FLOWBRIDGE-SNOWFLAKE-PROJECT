# FlowBridge validation guide

## Current execution status

**Author-confirmed — 8 November 2026:** The project owner reports rerunning the latest repository version in their Snowflake environment and confirming that the end-to-end project works correctly.

This is a project-owner confirmation. The repository does not currently contain sanitized run logs, query IDs, row-count snapshots, object-status outputs, or dashboard screenshots that would allow an independent reviewer to reproduce or inspect that specific execution. Do not invent or infer those artifacts.

## Repository-level checks

Run these commands from the repository root:

```bash
python -m compileall -q flowbridge_dashboard
python -m pip install pytest
python -m pytest -q
```

The GitHub Actions workflow performs the same Python compilation and repository contract test steps on pushes to `main`, pull requests, and manual dispatches.

These checks validate Python syntax and selected repository contracts. They do **not** execute Snowflake SQL, Azure event notifications, Snowpipe ingestion, stored procedures, Tasks, dynamic-table refreshes, role grants, or live dashboard queries.

## Environment verification scope

For a repeatable environment review, confirm the following in the target Snowflake/Azure environment and retain sanitized evidence where practical:

- Azure storage integration and event-notification configuration.
- Snowpipe configuration and receipt of file notifications.
- Expected source files and records arriving in Bronze.
- Valid records flowing through Silver stage 1 and stage 2.
- Invalid payloads being captured in the configured dead-letter destination.
- Fact-table grain, dimension relationships, and duplicate-handling behavior.
- Gold dynamic tables refreshing and exposing expected aggregates.
- The intended least-privilege role querying the secure serving views.
- The Streamlit dashboard loading and filtering the serving data.
- DEV/PROD scripts being applied only in the environment and deployment mode for which they were designed.

## Evidence policy

Prefer sanitized, authentic artifacts from the actual environment: execution timestamps, query IDs, row counts, pipe/task/dynamic-table status, and dashboard screenshots. Redact account identifiers, personal data, and secrets. Clearly distinguish measured values from examples. A successful owner-reported rerun is useful project evidence, but it is not a substitute for retaining reviewable deployment artifacts.

## What the automated tests do not prove

The repository contract tests check that expected scripts and the dashboard entry point exist, that the Python source parses, and that key project caveats remain documented. They are not a SQL parser, integration test suite, security assessment, performance benchmark, or proof of production readiness.