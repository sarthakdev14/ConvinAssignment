package store_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestInsertEventThenExists(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	exists, err := s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if exists {
		t.Fatal("expected event to be absent before insert")
	}

	if err := s.InsertEvent(ctx, evt); err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	exists, err = s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected event to exist after insert")
	}
}

func TestIncrementAccountStatsAccumulates(t *testing.T) {
	s := testutil.NewStore(t)
	_, _, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	if err := s.IncrementAccountStats(ctx, accountID, 30); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}
	if err := s.IncrementAccountStats(ctx, accountID, 12); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("got %+v, want CallCount=2 TotalDurationSec=42", got)
	}
}

func TestUpsertCallThenMarkRecordingProcessed(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/a.wav", Payload: []byte(`{}`),
	}
	if err := s.UpsertCall(ctx, evt); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}
	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	var processed bool
	row := s.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true")
	}
}

// TestIngestEventStoresEventCallAndStatsAtomically covers the happy path of
// the atomic write method that replaces the separate InsertEvent /
// UpsertCall / IncrementAccountStats calls in the ingest path: a single
// IngestEvent call should leave events, calls, and account_stats all
// correctly populated.
func TestIngestEventStoresEventCallAndStatsAtomically(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 42,
		RecordingURL: "https://example.com/a.wav", Payload: []byte(`{}`),
	}

	if err := s.IngestEvent(ctx, evt); err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}

	exists, err := s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount, gotStatus string
	row := s.Pool().QueryRow(ctx, `SELECT account_id, status FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount, &gotStatus); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID || gotStatus != "completed" {
		t.Fatalf("call got account=%q status=%q, want account=%q status=completed", gotAccount, gotStatus, accountID)
	}

	stats, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if stats.CallCount != 1 || stats.TotalDurationSec != 42 {
		t.Fatalf("account_stats got %+v, want CallCount=1 TotalDurationSec=42", stats)
	}
}

// TestIngestEventDuplicateIsRejectedWithoutDoubleCounting simulates a
// straightforward redelivery of the same event_id (the provider explicitly
// redelivers even after a 200). The second call must be recognized as a
// duplicate and must not double-count account stats.
func TestIngestEventDuplicateIsRejectedWithoutDoubleCounting(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 15, Payload: []byte(`{}`),
	}

	if err := s.IngestEvent(ctx, evt); err != nil {
		t.Fatalf("first IngestEvent: %v", err)
	}

	err := s.IngestEvent(ctx, evt)
	if !errors.Is(err, store.ErrDuplicateEvent) {
		t.Fatalf("second IngestEvent: got err=%v, want ErrDuplicateEvent", err)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 15 {
		t.Fatalf("got %+v after redelivery, want CallCount=1 TotalDurationSec=15 (double-counted)", got)
	}
}

// TestIngestEventConcurrentDuplicatesOnlyCountOnce is the regression test
// for the incident: concurrent redelivery of an identical event_id must
// never produce more than one events row or double-count account_stats,
// no matter how many requests race each other.
func TestIngestEventConcurrentDuplicatesOnlyCountOnce(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 20, Payload: []byte(`{}`),
	}

	const attempts = 8
	var wg sync.WaitGroup
	var successes, duplicates int32
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := s.IngestEvent(ctx, evt); {
			case err == nil:
				atomic.AddInt32(&successes, 1)
			case errors.Is(err, store.ErrDuplicateEvent):
				atomic.AddInt32(&duplicates, 1)
			default:
				t.Errorf("unexpected error from IngestEvent: %v", err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("got %d successful inserts under concurrent identical event_id, want exactly 1", successes)
	}
	if duplicates != attempts-1 {
		t.Fatalf("got %d duplicate results, want %d", duplicates, attempts-1)
	}

	var n int
	row := s.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan events: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s under concurrent delivery, want 1", n, eventID)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 20 {
		t.Fatalf("account_stats got %+v under concurrent delivery, want CallCount=1 TotalDurationSec=20", got)
	}
}

// TestPendingRecordingsListsUnprocessedCalls covers the query used to
// recover recordings whose processing was interrupted (by a restart, or by
// having their goroutine's context cancelled early): calls with a recording
// URL that have not been marked processed should be listed, and should drop
// off the list once MarkRecordingProcessed runs.
func TestPendingRecordingsListsUnprocessedCalls(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 5,
		RecordingURL: "https://example.com/pending.wav", Payload: []byte(`{}`),
	}
	if err := s.IngestEvent(ctx, evt); err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}

	pending, err := s.PendingRecordings(ctx)
	if err != nil {
		t.Fatalf("PendingRecordings: %v", err)
	}
	if !containsCallID(pending, callID) {
		t.Fatalf("expected %s to be listed as pending, got %+v", callID, pending)
	}

	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	pending, err = s.PendingRecordings(ctx)
	if err != nil {
		t.Fatalf("PendingRecordings after processing: %v", err)
	}
	if containsCallID(pending, callID) {
		t.Fatalf("expected %s to no longer be pending after MarkRecordingProcessed, got %+v", callID, pending)
	}
}

func containsCallID(pending []store.PendingRecording, callID string) bool {
	for _, p := range pending {
		if p.CallID == callID {
			return true
		}
	}
	return false
}