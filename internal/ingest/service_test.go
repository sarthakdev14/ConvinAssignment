package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

// TestConcurrentDuplicateDeliveryReturns200 is the regression test for the
// incident's "duplicate call records / counts drifting" symptom under real
// concurrency (not the sequential redelivery TestDuplicateDeliveryIsIgnored
// covers). The provider retries any non-2xx response forever, so a genuine
// redelivery racing against itself must still be accepted with 200 - it
// must not surface as a 500 just because it happened to lose a database
// race.
func TestConcurrentDuplicateDeliveryReturns200(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const attempts = 8
	type deliveryResult struct {
		status int
		err    error
	}
	results := make([]deliveryResult, attempts)

	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				results[i] = deliveryResult{err: err}
				return
			}
			defer resp.Body.Close()
			results[i] = deliveryResult{status: resp.StatusCode}
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		if r.err != nil {
			t.Fatalf("delivery %d: post: %v", i, r.err)
		}
		if r.status != http.StatusOK {
			t.Errorf("delivery %d: got status %d, want 200 (a losing race on a "+
				"genuine redelivery must still be accepted, not rejected - the "+
				"provider retries any non-2xx forever)", i, r.status)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan events: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s under concurrent delivery, want 1", n, eventID)
	}

	var callCount, totalDuration int64
	row = st.Pool().QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`, accountID)
	if err := row.Scan(&callCount, &totalDuration); err != nil {
		t.Fatalf("scan account_stats: %v", err)
	}
	if callCount != 1 || totalDuration != 143 {
		t.Fatalf("account_stats got call_count=%d total_duration_sec=%d under concurrent "+
			"delivery, want call_count=1 total_duration_sec=143 (double-counted)",
			callCount, totalDuration)
	}
}

// TestRecordingProcessingSurvivesResponseBeingSent is the regression test
// for "recordings never get marked processed - and there's nothing in the
// logs about it." processRecording currently runs on the inbound request's
// context, which net/http cancels as soon as the handler returns - almost
// always before the simulated 50ms of processing work finishes - so the
// resulting update fails silently.
func TestRecordingProcessingSurvivesResponseBeingSent(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// Poll well past the simulated 50ms of recording work, so this isn't a
	// timing race either way: if it's still false after this long, that's
	// the context being cancelled out from under the goroutine, not a slow
	// scheduler.
	deadline := time.Now().Add(2 * time.Second)
	var processed bool
	for time.Now().Before(deadline) {
		row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		if err := row.Scan(&processed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if processed {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !processed {
		t.Fatalf("recording for %s was never marked processed within 2s", callID)
	}
}