package stats_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

// TestCacheRecordIsSafeForConcurrentUse demonstrates that Record is not safe
// for concurrent use: it mutates c.m and the per-account counters without
// holding c.mu. Run with `go test -race` to see the race detector catch it
// directly; even without -race, concurrent increments to the same account
// can be lost, which the final assertions below also catch.
func TestCacheRecordIsSafeForConcurrentUse(t *testing.T) {
	c := stats.NewCache()

	const accounts = 20
	const callsPerAccount = 50

	var wg sync.WaitGroup
	for a := 0; a < accounts; a++ {
		accountID := fmt.Sprintf("acc_concurrent_%d", a)
		for i := 0; i < callsPerAccount; i++ {
			wg.Add(1)
			go func(accountID string) {
				defer wg.Done()
				c.Record(accountID, 1)
			}(accountID)
		}
	}
	wg.Wait()

	for a := 0; a < accounts; a++ {
		accountID := fmt.Sprintf("acc_concurrent_%d", a)
		got := c.Get(accountID)
		if got.CallCount != int64(callsPerAccount) {
			t.Fatalf("%s: got CallCount=%d, want %d (lost update under concurrent Record)",
				accountID, got.CallCount, callsPerAccount)
		}
		if got.TotalDurationSec != int64(callsPerAccount) {
			t.Fatalf("%s: got TotalDurationSec=%d, want %d",
				accountID, got.TotalDurationSec, callsPerAccount)
		}
	}
}