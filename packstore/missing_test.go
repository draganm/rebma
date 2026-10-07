package packstore

import (
	"runtime"
	"slices"
	"testing"

	"github.com/draganm/rebma/key"
)

func TestMissing(t *testing.T) {
	s := openStore(t, t.TempDir(), WithSegmentSize(8<<10)) // force sealed + active mix
	objs := testObjects(t, 200)
	stored, absent := objs[:120], objs[120:]
	for _, o := range stored {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	// Interleave present and absent keys, with a duplicate absent key.
	var query []key.Key
	for i := range absent {
		query = append(query, stored[i%len(stored)].Key, absent[i].Key)
	}
	query = append(query, absent[0].Key) // duplicate, must be reported twice

	got, err := s.Missing(query)
	if err != nil {
		t.Fatal(err)
	}
	var want []key.Key
	for _, o := range absent {
		want = append(want, o.Key)
	}
	want = append(want, absent[0].Key)
	if !slices.Equal(got, want) {
		t.Fatalf("Missing: got %d keys, want %d (order and multiplicity preserved)", len(got), len(want))
	}
}

func TestMissingEmptyInput(t *testing.T) {
	s := openStore(t, t.TempDir())
	got, err := s.Missing(nil)
	if err != nil || got != nil {
		t.Fatalf("Missing(nil) = %v, %v", got, err)
	}
}

func TestMissingManyWorkersChunkClamp(t *testing.T) {
	// GOMAXPROCS=67 with 4289 keys made worker 66 compute keys[4290:4289]
	// and panic before the low bound was clamped.
	old := runtime.GOMAXPROCS(67)
	defer runtime.GOMAXPROCS(old)

	s := openStore(t, t.TempDir())
	keys := make([]key.Key, 0, 4289)
	for i := 0; i < 4289; i++ {
		data := []byte{byte(i), byte(i >> 8), byte(i >> 16), 0xA5}
		k, err := key.New(key.Blob, uint64(len(data)), data)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	got, err := s.Missing(keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(keys) {
		t.Fatalf("Missing returned %d keys, want %d", len(got), len(keys))
	}
}
