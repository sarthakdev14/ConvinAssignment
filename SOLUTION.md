# SOLUTION.md

## What was broken, and why

Three separate bugs were producing the reported symptoms.

Duplicate call records and inflated counts came from a check-then-insert race in
`Ingest`: it queried `EventExists`, then inserted, as two separate round trips with
nothing preventing two concurrent deliveries of the same `event_id` from both
passing the check before either had inserted. The schema didn't help — `event_id`
only had a plain index, not a unique constraint — so nothing at any layer actually
stopped the duplicate. Account stats were also incremented as a fourth, independent
write, so a duplicate delivery didn't just insert a duplicate row, it counted the
call twice. Separately, `stats.Cache.Record` mutated its map and counters without
taking the mutex it already had (`Get` took it, `Record` didn't), so concurrent
requests could lose updates or, less often, panic on a concurrent map write.

Recordings never getting marked processed came from `processRecording` running on
`r.Context()` inside a detached goroutine. `Ingest` returns as soon as the goroutine
is launched, the handler returns right behind it, and `net/http` cancels the
request's context the moment the handler returns — before the simulated 50ms of
work finished. `MarkRecordingProcessed` then failed with `context canceled` on
essentially every delivery, and the error was thrown away (`// TODO: handle`),
which is why nothing showed up in the logs. The same detached-goroutine design
meant any recording still in flight (or never even started) at shutdown was just
lost, since `srv.Shutdown` only drains HTTP connections, not background goroutines.

## Why this dedup strategy over the alternatives

I added a unique constraint on `events.event_id` and wrapped the insert, call
upsert, and stats increment in one transaction, catching a `23505` unique-violation
as "already handled" rather than a real error. I considered a Redis `SETNX` as a
pre-check lock instead, but that adds a second source of truth that can disagree
with Postgres — the lock can succeed while the DB write fails, or Redis can evict
or be unavailable independent of what's durably stored. Since Postgres is already
doing the write, letting it enforce uniqueness atomically is the smaller, more
honest solution, and it composes correctly with retries: a losing request just gets
told "already done" instead of erroring.

## At 10,000 webhooks/sec

A single Postgres row lock on `account_stats` per account would become the
bottleneck for any account with disproportionate traffic. I'd move stats off the
synchronous path entirely — insert the event durably, then aggregate asynchronously
(a queue consumed in batches, or a periodic rollup job) instead of updating
`account_stats` inline on every request. I'd also reconsider Redis at that point,
not for dedup correctness but as a read-through cache in front of the stats table
to keep `GET /accounts/{id}/stats` fast without hitting Postgres per read.
