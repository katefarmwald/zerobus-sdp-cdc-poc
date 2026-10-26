-- One-time setup for the Zerobus landing table. Run manually in the Databricks SQL editor.
--
-- Zerobus never evolves the schema and rejects records with unknown fields:
-- change this table first, then the producer. CREATE OR REPLACE drops existing
-- grants, so re-run the GRANT statements after any change.

CREATE SCHEMA IF NOT EXISTS workspace.zb_poc;

-- Append-only bronze table. Change Data Feed is deliberately NOT enabled: it would only
-- ever show inserts. Update/delete semantics live in `operation` and are applied by AUTO CDC.
CREATE OR REPLACE TABLE workspace.zb_poc.orders_raw (
  event_id     STRING    COMMENT 'Unique per event (uuid4)',
  order_id     STRING    COMMENT 'Business key for AUTO CDC',
  customer_key BIGINT    COMMENT 'Joins to samples.tpch.customer.c_custkey',
  amount       DOUBLE,
  status       STRING    COMMENT 'CREATED -> PAID -> SHIPPED, or CANCELLED',
  operation    STRING    COMMENT 'INSERT | UPDATE | DELETE',
  seq          BIGINT    COMMENT 'Strictly increasing, used by SEQUENCE BY',
  event_time   TIMESTAMP COMMENT 'Producer sends epoch microseconds'
);

-- Grants for the Zerobus service principal. Replace <sp-application-id> with the
-- service principal's application (client) ID, the same value as DATABRICKS_CLIENT_ID.
-- The service principal also needs the "Workspace access" entitlement.
GRANT USE CATALOG ON CATALOG workspace TO `<sp-application-id>`;
GRANT USE SCHEMA ON SCHEMA workspace.zb_poc TO `<sp-application-id>`;
GRANT MODIFY, SELECT ON TABLE workspace.zb_poc.orders_raw TO `<sp-application-id>`;

-- Smoke-test check after `python producer/producer.py --count 5`:
-- SELECT * FROM workspace.zb_poc.orders_raw ORDER BY seq DESC;
