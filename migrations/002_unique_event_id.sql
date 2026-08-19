-- The original index on events.event_id was non-unique, so nothing at the
-- database layer prevented two rows for the same event_id. Combined with the
-- check-then-insert pattern in internal/ingest/service.go, concurrent
-- redelivery of the same event_id could (and did) produce duplicate rows and
-- double-counted account_stats.
--
-- Replace the plain index with a uniqueness constraint so a duplicate
-- event_id is rejected atomically by Postgres itself, no matter how many
-- concurrent requests race to insert it. Postgres backs this constraint with
-- its own unique index, so EventExists lookups stay just as fast.
DROP INDEX IF EXISTS idx_events_event_id;

ALTER TABLE events
    ADD CONSTRAINT events_event_id_key UNIQUE (event_id);