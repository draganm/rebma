package packstore

import (
	"bytes"
	"context"
	"math/rand/v2"
	"testing"

	"github.com/draganm/rebma/key"
)

// TestOracle drives a store through random Put/WriteBatch/reopen cycles with
// a small rotation threshold and cross-checks every observable (Get, Has,
// Missing, Verify) against an in-memory map.
func TestOracle(t *testing.T) {
	dir := t.TempDir()
	r := rand.New(rand.NewPCG(1, 2))
	oracle := make(map[key.Key][]byte)

	newObj := func() Object {
		n := 1 + r.IntN(4000)
		data := make([]byte, n)
		if r.IntN(2) == 0 {
			for i := range data {
				data[i] = byte(r.Uint64()) // incompressible
			}
		} else {
			for i := range data {
				data[i] = byte(i % 7) // compressible
			}
		}
		return blobObj(t, data)
	}

	s := openStore(t, dir, WithSegmentSize(16<<10), WithSync(false))
	for round := 0; round < 20; round++ {
		switch r.IntN(3) {
		case 0: // single puts
			for i := 0; i < 20; i++ {
				o := newObj()
				if err := s.Put(o.Key, o.Data); err != nil {
					t.Fatal(err)
				}
				oracle[o.Key] = o.Data
			}
		case 1: // a batch with duplicates
			var objs []Object
			for i := 0; i < 30; i++ {
				o := newObj()
				objs = append(objs, o, o)
				oracle[o.Key] = o.Data
			}
			if err := s.WriteBatch(objSeq(objs, -1)); err != nil {
				t.Fatal(err)
			}
		case 2: // reopen (exercises seal-survival + tail-scan resume)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openStore(t, dir, WithSegmentSize(16<<10), WithSync(false))
		}
	}

	// Full readback.
	var present []key.Key
	for k, want := range oracle {
		got, err := s.Get(k)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("Get(%s): %v", k, err)
		}
		present = append(present, k)
	}

	// Absent probes: compressible test data is a pure function of its length,
	// so fresh keys CAN collide with stored ones (and with each other) — skip
	// stored keys and dedupe, or Missing's exact answers look like failures.
	absentSet := make(map[key.Key]struct{})
	var absent []key.Key
	for len(absent) < 100 {
		k := newObj().Key
		if _, stored := oracle[k]; stored {
			continue
		}
		if _, dup := absentSet[k]; dup {
			continue
		}
		absentSet[k] = struct{}{}
		absent = append(absent, k)
	}

	// Missing cross-check.
	query := append(append([]key.Key{}, present...), absent...)
	miss, err := s.Missing(query)
	if err != nil {
		t.Fatal(err)
	}
	missSet := make(map[key.Key]int)
	for _, k := range miss {
		missSet[k]++
	}
	for _, k := range present {
		if missSet[k] != 0 {
			t.Fatalf("present key %s reported missing", k)
		}
	}
	for _, k := range absent {
		if missSet[k] != 1 {
			t.Fatalf("absent key %s not reported missing exactly once", k)
		}
	}

	// Structural scrub.
	if err := s.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
}
