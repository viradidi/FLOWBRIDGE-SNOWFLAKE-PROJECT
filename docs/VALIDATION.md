# FlowBridge validation guide

## Validation layers

### Local repository checks
Run from the repository root:

```bash
python -m compileall -q Untitled
python -m pip install pytest
python -m pytest -q
```

These checks verify Python syntax and a small repository contract. They do **not** execute Snowflake SQL, Azure event ingestion, Snowpipe, dynamic-table refreshes, role grants, or live dashboard queries.

### Environment validation checklist
Use a non-production environment first and record real outcomes.

- [ ] Azure storage integration and event notification configuration are valid.
- [ ] Snowpipe is configured and receives file notifications.
- [ ] New source files are visible in Bronze.
- [ ] Valid records flow through Silver stage 1 and stage 2.
- [ ] Invalid payloads are recorded in the configured dead-letter destination.
- [ ] Fact table grain and dimension keys are correct.
- [ ] Gold dynamic tables refresh and expose expected aggregates.
- [ ] Least-privilege role can query serving views.
- [ ] Streamlit dashboard renders using the configured Snowflake connection.
- [ ] DEV/PROD scripts have been reviewed and deployed according to environment policy.

## Evidence policy

Retain sanitized, authentic evidence from your own environment, such as execution timestamps, query IDs, row counts, task/pipe status, and dashboard screenshots. Redact account identifiers, personal data, and secrets. Do not use illustrative values as if they were measured deployment results.
