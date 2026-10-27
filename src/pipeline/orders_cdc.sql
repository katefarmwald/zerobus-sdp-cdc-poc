-- AUTO CDC flows from the validated change feed (orders_clean.sql).
--
-- orders_raw is append-only: every event is a new row, and the change semantics
-- live in the `operation` column. AUTO CDC reads the stream, orders events per
-- order_id by `seq` (not by arrival), and applies them to the targets.

-- SCD Type 1: one row per order with its latest state. A DELETE removes the row.
CREATE OR REFRESH STREAMING TABLE orders_current;

CREATE FLOW orders_current_cdc AS AUTO CDC INTO orders_current
FROM STREAM(orders_clean)
KEYS (order_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY seq
COLUMNS * EXCEPT (operation, seq, event_id)
STORED AS SCD TYPE 1;

-- SCD Type 2: one row per version of an order, with __START_AT / __END_AT.
-- A new version opens only when status or amount changes (TRACK HISTORY ON);
-- a DELETE closes the current version instead of removing it.
CREATE OR REFRESH STREAMING TABLE orders_history;

CREATE FLOW orders_history_cdc AS AUTO CDC INTO orders_history
FROM STREAM(orders_clean)
KEYS (order_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY seq
COLUMNS * EXCEPT (operation, seq, event_id)
STORED AS SCD TYPE 2
TRACK HISTORY ON status, amount;
