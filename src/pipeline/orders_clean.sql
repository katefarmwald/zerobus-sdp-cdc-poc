-- Silver: validated view of the raw Zerobus change feed.
--
-- Both AUTO CDC flows read from this view instead of orders_raw, so an event that
-- breaks a rule never reaches orders_current / orders_history. orders_raw still keeps
-- every event, so dropped rows can be inspected there.
--
-- It is a TEMPORARY view: it exists only inside the pipeline (not published to Unity
-- Catalog) and stores nothing. Because it reads STREAM(...), it is a streaming view and
-- each run only sees events that are new since the last run.
--
-- ON VIOLATION DROP ROW removes the event; pass/fail counts per expectation show up in
-- the pipeline UI (Data quality tab) and in the event log.

CREATE TEMPORARY VIEW orders_clean (
  CONSTRAINT valid_operation   EXPECT (operation IN ('INSERT', 'UPDATE', 'DELETE')) ON VIOLATION DROP ROW,
  -- AUTO CDC cannot key or order an event without these
  CONSTRAINT order_id_not_null EXPECT (order_id IS NOT NULL)                         ON VIOLATION DROP ROW,
  CONSTRAINT seq_not_null      EXPECT (seq IS NOT NULL)                              ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount   EXPECT (amount > 0)                                   ON VIOLATION DROP ROW
)
AS SELECT * FROM STREAM(${raw_table});
