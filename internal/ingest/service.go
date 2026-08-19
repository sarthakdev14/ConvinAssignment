// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// recordingProcessingTimeout bounds how long background recording
// processing may run. It is deliberately independent of the inbound
// request's context: net/http cancels a request's context as soon as its
// handler returns, which happens immediately after Ingest launches this
// work - well before recordingWork would elapse - so tying this to the
// request context would fail it on almost every delivery.
const recordingProcessingTimeout = 30 * time.Second

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
//
// Storage is atomic: the event insert, call upsert, and stats increment all
// happen inside one transaction (store.IngestEvent). The provider delivers
// at least once and redelivers even after a 200, so seeing an event_id
// again - including two deliveries racing each other - is expected: it
// comes back as store.ErrDuplicateEvent and is treated as a successful
// no-op, never as a failure. The in-memory cache is only updated once we
// know this delivery was the one that actually got stored, so a losing
// duplicate can never double-count it.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	if err := s.store.IngestEvent(ctx, rec); err != nil {
		if errors.Is(err, store.ErrDuplicateEvent) {
			s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
			return nil
		}
		return err
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	if rec.RecordingURL != "" {
		s.processRecordingAsync(rec)
	}

	return nil
}

// processRecordingAsync runs processRecording in the background on a
// context whose lifetime is independent of any inbound HTTP request, bounded
// by recordingProcessingTimeout so a stuck fetch cannot run forever.
// Failures are logged instead of being silently discarded.
func (s *Service) processRecordingAsync(rec store.Event) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), recordingProcessingTimeout)
		defer cancel()
		if err := s.processRecording(ctx, rec); err != nil {
			s.log.Error("process recording", "call_id", rec.CallID, "account_id", rec.AccountID, "err", err)
		}
	}()
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

// RecoverPendingRecordings re-launches processing for every call whose
// recording was never marked processed - because a previous run was
// interrupted (a restart/deploy), or because an earlier goroutine's context
// was cancelled before it finished. Intended to run once at startup, before
// the server begins accepting traffic, so nothing is silently lost across a
// deploy.
func (s *Service) RecoverPendingRecordings(ctx context.Context) error {
	pending, err := s.store.PendingRecordings(ctx)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	s.log.Info("recovering pending recordings", "count", len(pending))
	for _, p := range pending {
		s.processRecordingAsync(store.Event{
			CallID:       p.CallID,
			AccountID:    p.AccountID,
			RecordingURL: p.RecordingURL,
		})
	}
	return nil
}