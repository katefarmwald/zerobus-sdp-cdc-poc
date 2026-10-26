# zerobus-sdp-cdc-poc

Personal practice POC: a near-real-time streaming pipeline on **Databricks Free Edition**.
Events are pushed into a Delta table with **Zerobus Ingest**, processed by a **Lakeflow Spark
Declarative Pipeline (SDP)** that uses **AUTO CDC** for SCD Type 1 and Type 2, and deployed
with a **Databricks Asset Bundle (DAB)**.

The goal is to learn and practice these tools, not to build production software. Prefer small,
clear, well-commented code over abstraction. Explain non-obvious Databricks behavior in comments.

## Environment constraints (Free Edition)

These shape almost every design decision. Respect them.

- **Serverless compute only.** No classic clusters, no `new_cluster` / `job_cluster_key`
  definitions. The pipeline must set `serverless: true`.
- **One active pipeline per pipeline type.** A continuously running pipeline blocks other
  pipeline runs. Never assume dev and prod pipelines can run side by side.
- **Daily fair-usage quota.** Exceeding it shuts compute off for the rest of the day (or
  longer). Keep data volumes tiny, default to **triggered** runs, and run **continuous**
  mode only in short, deliberate sessions (15 to 30 minutes), then stop it.
- **Non-commercial use only.**
- Default catalog in Free Edition is `workspace`. Tables live on default storage.
- Structured Streaming in serverless notebooks/jobs only supports `availableNow` / `once`
  style triggers, not long-running processing-time triggers.

## Architecture

```
laptop: producer/producer.py  (~3 events/sec, free, runs outside Databricks)
        |
        |  Zerobus Ingest (gRPC, service principal OAuth)
        v
workspace.zb_poc.orders_raw         append-only Delta table (bronze / landing)
        |
        |  SDP pipeline (triggered by default)
        v
orders_current   AUTO CDC, SCD Type 1  (current state, deletes remove the row)
orders_history   AUTO CDC, SCD Type 2  (full history, __START_AT / __END_AT)
        |
        v
gold materialized view(s)   aggregates, joined to samples.tpch.customer
```

Ingestion (Zerobus) costs no compute of ours while it runs. Compute is used only when the
pipeline updates or when we query.

## Key decisions (and why)

| Decision | Reason |
|---|---|
| Zerobus Ingest as the primary ingestion path | Practice push-based streaming ingest; producer runs on the laptop for free. |
| Fallback: producer writes small JSON files to a UC volume, bronze = Auto Loader streaming table | Zerobus may fail on Free Edition (service principal OAuth relies on account-level infrastructure Free Edition may lack; default-storage tables are a Public Preview target). Everything downstream of bronze stays identical. |
| Low event rate (a few events/sec) | Enough to learn all streaming/CDC semantics while protecting the quota. |
| Triggered pipeline by default, continuous via a bundle variable/target override | Write streaming code but pay compute per run; flip to continuous only to observe latency. |
| AUTO CDC (not hand-written MERGE) for SCD1 and SCD2 | Handles ordering via `SEQUENCE BY`, deletes, and SCD2 history declaratively. |
| `seq` column is clock-based and strictly increasing (epoch micros, +1 on collision) | Survives producer restarts; required for correct out-of-order handling. |
| Delta Change Data Feed (CDF) is **not** used on `orders_raw` | The raw table is append-only, so CDF would only ever show inserts. The change semantics live in the `operation` column and are applied by AUTO CDC. CDF is only worth exploring downstream (for example reading changes out of `orders_current`) as a later experiment. |
| Gold as materialized views | On serverless, MVs refresh incrementally when possible, so gold stays incremental without custom logic. |
| `customer_key` drawn from 1..750,000 | Matches `samples.tpch.customer` keys so gold can join to free sample data. |
| Pipeline code in **SQL** (current default) | Snippets so far are SQL. Python (`pyspark.pipelines`, `dp.create_auto_cdc_flow`) is acceptable if the user asks. |

## Data contract: `workspace.zb_poc.orders_raw`

Zerobus never evolves the schema and rejects records with unknown fields. **Change the
table first, then the producer.** After `CREATE OR REPLACE TABLE`, re-run the grants.

```sql
CREATE SCHEMA IF NOT EXISTS workspace.zb_poc;

CREATE OR REPLACE TABLE workspace.zb_poc.orders_raw (
  event_id     STRING,     -- unique per event (uuid4)
  order_id     STRING,     -- business key for AUTO CDC
  customer_key BIGINT,     -- joins to samples.tpch.customer.c_custkey
  amount       DOUBLE,
  status       STRING,     -- CREATED -> PAID -> SHIPPED, or CANCELLED
  operation    STRING,     -- INSERT | UPDATE | DELETE
  seq          BIGINT,     -- strictly increasing, used by SEQUENCE BY
  event_time   TIMESTAMP   -- producer sends epoch microseconds
);

GRANT USE CATALOG ON CATALOG workspace TO `<sp-application-id>`;
GRANT USE SCHEMA ON SCHEMA workspace.zb_poc TO `<sp-application-id>`;
GRANT MODIFY, SELECT ON TABLE workspace.zb_poc.orders_raw TO `<sp-application-id>`;
```

## Producer (`producer/producer.py`)

- Python 3.9+, `pip install databricks-zerobus-ingest-sdk python-dotenv`.
- Reads config from `.env` (never commit it): `ZEROBUS_SERVER_ENDPOINT`,
  `DATABRICKS_WORKSPACE_URL`, `ZEROBUS_TABLE_NAME`, `DATABRICKS_CLIENT_ID`,
  `DATABRICKS_CLIENT_SECRET`.
- Zerobus endpoint format: `https://<workspace-id>.zerobus.<region>.cloud.databricks.com`.
- Emits a change feed: ~40% INSERT (new order), the rest touch an existing order:
  UPDATE (status advance, ~5% CANCELLED, ~10% amount corrections) or DELETE (~4%).
- Keeps at most 200 open orders in memory.
- Usage:
  - `python producer/producer.py --count 5` (smoke test)
  - `python producer/producer.py --rate 3 --minutes 15`
- Uses the sync SDK: `ZerobusSdk(endpoint, workspace_url)`,
  `sdk.create_stream(client_id, client_secret, TableProperties(table))`,
  `stream.ingest_record_offset(dict)`, `stream.close()` in `finally`.

## Pipeline design

Source files go in `src/pipeline/`. Target catalog/schema come from bundle variables.

```sql
-- SCD Type 1
CREATE OR REFRESH STREAMING TABLE orders_current;
CREATE FLOW orders_current_cdc AS AUTO CDC INTO orders_current
FROM STREAM(workspace.zb_poc.orders_raw)
KEYS (order_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY seq
COLUMNS * EXCEPT (operation, seq, event_id)
STORED AS SCD TYPE 1;

-- SCD Type 2
CREATE OR REFRESH STREAMING TABLE orders_history;
CREATE FLOW orders_history_cdc AS AUTO CDC INTO orders_history
FROM STREAM(workspace.zb_poc.orders_raw)
KEYS (order_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY seq
COLUMNS * EXCEPT (operation, seq, event_id)
STORED AS SCD TYPE 2
TRACK HISTORY ON status, amount;
```

Still to build:
- Data-quality expectations on raw input (for example valid `operation` values, non-null
  `order_id` and `seq`, `amount > 0`). Decide whether these live in a silver view between
  raw and the AUTO CDC flows.
- Gold materialized view(s): for example revenue and order counts by status and by
  customer nation (join `samples.tpch.customer` and `samples.tpch.nation`).

## Bundle design

- `databricks.yml` at repo root, pipeline resource in `resources/orders_pipeline.yml`.
- Variables: `catalog` (default `workspace`), `schema` (default `zb_poc`),
  `continuous` (default `false`).
- Pipeline resource: `serverless: true`, `catalog: ${var.catalog}`,
  `schema: ${var.schema}`, `continuous: ${var.continuous}`, source code from
  `src/pipeline/`.
- Targets: `dev` (default, `mode: development`). Optionally a `stream` target or variable
  override that sets `continuous: true`. Do not plan on running two pipelines at once.
- Auth: `databricks auth login` (OAuth U2M) with a CLI profile for the Free Edition workspace.
- Always verify YAML keys against current Databricks docs and run `databricks bundle validate`
  before deploying; DAB and SDP field names have changed across releases.

Common commands:
```
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run orders_pipeline -t dev
databricks bundle deploy -t dev --var="continuous=true"   # short real-time sessions only
databricks bundle destroy -t dev
```

## Repo layout

```
zerobus-sdp-cdc-poc/
├── CLAUDE.md
├── databricks.yml
├── resources/orders_pipeline.yml
├── src/pipeline/            # SDP source files (SQL)
├── producer/producer.py     # runs locally, not deployed by the bundle
├── sql/setup.sql            # schema, table, grants (run manually in SQL editor)
├── .env                     # local secrets, gitignored
└── .gitignore               # include .env, .venv/, .databricks/, __pycache__/
```

## Current status

- [x] Repo created and cloned.
- [x] Producer written (change feed with INSERT/UPDATE/DELETE and `seq`).
- [ ] Confirm Zerobus region availability for the workspace.
- [ ] Create schema, table, service principal (with Workspace access entitlement), grants.
- [ ] Smoke test: `python producer/producer.py --count 5`, then
      `SELECT * FROM workspace.zb_poc.orders_raw ORDER BY seq DESC`.
  - Auth/token error: switch to the volume + Auto Loader fallback.
  - Type error on `event_time`: adjust the timestamp encoding in the producer.
- [ ] Scaffold the bundle (`databricks.yml`, pipeline resource, `src/pipeline/`).
- [ ] Build expectations, AUTO CDC flows, gold MV.
- [ ] Triggered runs, then a short continuous session to observe latency.

## Experiments to try once data flows

- Remove `TRACK HISTORY ON` and compare SCD2 versions.
- Compare a deleted order in `orders_current` (gone) vs `orders_history` (closed via `__END_AT`).
- Send an event with an older `seq` and confirm AUTO CDC applies by sequence, not arrival.
- Later: read Delta CDF from `orders_current` downstream and compare to the MV approach.

## Working agreements

- Keep compute usage minimal; ask before suggesting anything that runs continuously or
  scans large data.
- Never commit secrets. Credentials only in `.env`.
- When unsure whether a feature works on Free Edition, say so and propose a quick test
  rather than assuming.