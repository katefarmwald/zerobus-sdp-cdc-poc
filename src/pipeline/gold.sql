-- Gold: aggregates over the current state of orders (SCD1).
--
-- Materialized views, not streaming tables: orders_current changes in place (updates and
-- deletes), and an MV always reflects the latest state of its inputs. On serverless the
-- pipeline refreshes MVs incrementally when it can, and recomputes them when it can't.
--
-- Columns used by all three views:
--   order_count  - orders currently in that group (deleted orders are already gone)
--   total_amount - sum of amount over all of them, including CANCELLED
--   revenue      - sum of amount excluding CANCELLED orders

CREATE OR REFRESH MATERIALIZED VIEW revenue_by_status
COMMENT 'Current orders and amounts per status'
AS SELECT
  status,
  count(*)                                       AS order_count,
  round(sum(amount), 2)                          AS total_amount,
  round(sum(amount) FILTER (WHERE status <> 'CANCELLED'), 2) AS revenue
FROM orders_current
GROUP BY status;

-- customer_key was drawn from 1..750,000 by the producer to match the TPC-H sample keys.
-- samples.tpch.customer is ~750K rows: small, but it is scanned on every full recompute.
CREATE OR REFRESH MATERIALIZED VIEW revenue_by_nation
COMMENT 'Current orders and revenue per customer nation (samples.tpch)'
AS SELECT
  n.n_name                                       AS nation,
  count(*)                                       AS order_count,
  round(sum(o.amount), 2)                        AS total_amount,
  round(sum(o.amount) FILTER (WHERE o.status <> 'CANCELLED'), 2) AS revenue
FROM orders_current o
JOIN samples.tpch.customer c ON o.customer_key = c.c_custkey
JOIN samples.tpch.nation   n ON c.c_nationkey  = n.n_nationkey
GROUP BY n.n_name;

CREATE OR REFRESH MATERIALIZED VIEW status_by_nation
COMMENT 'Current orders and amounts per customer nation and status'
AS SELECT
  n.n_name                                       AS nation,
  o.status,
  count(*)                                       AS order_count,
  round(sum(o.amount), 2)                        AS total_amount
FROM orders_current o
JOIN samples.tpch.customer c ON o.customer_key = c.c_custkey
JOIN samples.tpch.nation   n ON c.c_nationkey  = n.n_nationkey
GROUP BY n.n_name, o.status;
