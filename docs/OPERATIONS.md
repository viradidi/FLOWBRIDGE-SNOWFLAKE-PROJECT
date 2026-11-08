# FlowBridge operations guide

This guide summarizes the repository deployment sequence and the operational boundaries that matter when promoting the platform between environments. The numbered SQL scripts remain authoritative for exact SQL statements, object names, permissions, and configuration requirements.

## Deployment sequence

| Order | Script | Scope |
|---:|---|---|
| 1 | `01_account_setup.sql` | Creates the DEV/PROD databases, schemas, warehouses, and account resources |
| 2 | `02_bronze.sql` | Configures Azure integration, external stage, Bronze landing table, Snowpipe, and related ingestion objects |
| 3 | `03_silver_stage1.sql` | Creates the raw stream, staging/dead-letter structures, validation procedure, and processing task |
| 4 | `04_silver_stage2.sql` | Creates dimensions and the fact table, stream, transformation procedure, and task |
| 5 | `05_gold.sql` | Builds Gold dynamic tables for analytical consumption |
| 6 | `06_serving.sql` | Creates secure serving views over the curated layer |
| 7 | `07_governance.sql` | Creates and grants the analytics consumer role and related governance objects |
| 8 | `08_dev_prod.sql` | Bootstraps the production environment from DEV using the script's documented clone-and-rebind approach |

Review each script's role, database, schema, warehouse, and placeholder requirements before execution. Apply account-level and integration commands only with the required privileges.

## Environment boundaries

- Develop and validate changes in DEV before promoting them.
- Review Azure tenant, storage, notification, Snowflake integration, and email-recipient placeholders before deployment.
- Do not commit credentials, account identifiers, tenant values, private endpoints, or personal email addresses.
- Verify the grants in `07_governance.sql` against the intended consumer role and data-access policy.
- Configure the Streamlit Snowflake connection through the supported application connection mechanism, not source-code credentials.

## Production bootstrap warning

`08_dev_prod.sql` documents a **fresh-PROD bootstrap**, not a general-purpose migration or repeatable in-place upgrade. Its schema clone statements use `IF NOT EXISTS`; an existing production schema is therefore not refreshed simply by rerunning the bootstrap script. Review the script before use, and use an explicit, reviewed migration plan for subsequent releases rather than assuming a rerun will synchronize PROD with DEV.

## Operational verification

After a deployment or release, verify the flow at each boundary:

1. Confirm the Azure storage and event-notification configuration.
2. Confirm Snowpipe receives notifications and Bronze receives the expected files/records.
3. Check valid and rejected payload handling in Silver stage 1, including the dead-letter destination.
4. Verify the order-level fact grain, dimension keys, and duplicate/replay behavior in Silver stage 2.
5. Confirm Gold dynamic tables expose the expected aggregates.
6. Confirm the analytics consumer role can query the secure serving views while remaining limited to its intended scope.
7. Open the Streamlit dashboard with its configured Snowflake connection and verify its filters, KPIs, and analytical sections.

Keep the verification in the target account. A successful SQL script submission alone is not sufficient evidence that downstream data, access, and dashboard behavior are correct.

## Reruns and recovery

- Treat object replacement, stream consumption, task state, and data replay as stateful operations. Review the SQL procedure/task logic before rerunning a partially completed deployment.
- Do not assume `CREATE OR REPLACE` or `CREATE ... IF NOT EXISTS` has the same operational effect; inspect the target objects and script behavior first.
- Do not run the DEV-to-PROD bootstrap as a repair mechanism for an existing production environment.
- Record the failing stage, relevant query IDs, sanitized error text, and the recovery action. Avoid committing credentials or sensitive account metadata.
- For schema or grain changes, update Silver transformations, Gold aggregates, secure serving views, dashboard queries, tests, and documentation together.

## Release evidence

Retain sanitized run timestamps, query IDs, pipe/task/dynamic-table status, row-count checks, and a dashboard screenshot where appropriate. Keep owner confirmation and measured account evidence clearly distinguished. Never manufacture logs, metrics, or deployment artifacts.