package gc

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/draganm/rebma/key"
)

// TestOracle drives random reference churn and cycles against an
// in-memory model of what must stay readable.
func TestOracle(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	c := ts.openCollector(t, Options{Grace: time.Hour})
	rng := rand.New(rand.NewPCG(7, 11))

	type tree struct {
		root key.Key
		keys []key.Key
	}
	live := map[string]tree{} // ref name -> tree
	var graveyard []tree      // unreferenced trees; their unique data may vanish

	refNames := []string{"a", "b", "c", "d"}
	for round := 0; round < 30; round++ {
		name := refNames[rng.IntN(len(refNames))]
		switch rng.IntN(3) {
		case 0: // set to a fresh tree
			root, keys := storeTree(t, ts.objects, fmt.Sprintf("t%d-", round), rng.IntN(20)+5)
			putTestRef(t, c, ts.refs, name, root)
			if old, ok := live[name]; ok {
				graveyard = append(graveyard, old)
			}
			live[name] = tree{root, keys}
		case 1: // delete
			if old, ok := live[name]; ok {
				if err := ts.refs.Delete(name); err != nil {
					t.Fatal(err)
				}
				if err := c.ReleaseRef(old.root); err != nil {
					t.Fatal(err)
				}
				graveyard = append(graveyard, old)
				delete(live, name)
			}
		case 2: // cycle at a random line
			backdatePacks(t, ts)
			line := []float64{-1, 0, 0.3}[rng.IntN(3)]
			if _, err := c.Run(context.Background(), line); err != nil {
				t.Fatalf("round %d: Run: %v", round, err)
			}
		}
		// Invariant: everything referenced reads back, always.
		for name, tr := range live {
			for _, k := range tr.keys {
				if _, err := ts.objects.Get(k); err != nil {
					t.Fatalf("round %d: ref %q key %s: %v", round, name, k, err)
				}
			}
		}
	}
	// Force a full sweep and re-verify, then reopen the collector and
	// verify a fresh mark reaches the same conclusion.
	backdatePacks(t, ts)
	if _, err := c.Run(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	for _, tr := range live {
		for _, k := range tr.keys {
			if _, err := ts.objects.Get(k); err != nil {
				t.Fatalf("after sweep: %v", err)
			}
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := ts.openCollector(t, Options{Grace: time.Hour})
	backdatePacks(t, ts)
	if _, err := c2.Run(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	for _, tr := range live {
		for _, k := range tr.keys {
			if _, err := ts.objects.Get(k); err != nil {
				t.Fatalf("after reopen sweep: %v", err)
			}
		}
	}
	_ = graveyard // dead trees: no assertion — they may or may not be gone yet
}
