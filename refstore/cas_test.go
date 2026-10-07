package refstore_test

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/reference"
	"github.com/draganm/rebma/refstore"
)

// blobKey is the key of a Blob holding s.
func blobKey(t *testing.T, s string) key.Key {
	t.Helper()
	k, err := key.New(key.Blob, uint64(len(s)), []byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// record is an encoded reference called name pointing at k.
func record(t *testing.T, name string, k key.Key, createdAt int64) []byte {
	t.Helper()
	raw, err := reference.Reference{Name: name, Key: k[:], CreatedAt: createdAt}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCompareAndSwap(t *testing.T) {
	s := open(t, t.TempDir())
	k1, k2, k3 := blobKey(t, "one"), blobKey(t, "two"), blobKey(t, "three")

	if err := s.CompareAndSwap("r", k1, record(t, "r", k2, 1)); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("swap of an absent reference = %v, want ErrNotFound", err)
	}
	at1 := record(t, "r", k1, 1)
	if err := s.Put("r", at1); err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSwap("r", k3, record(t, "r", k2, 2)); !errors.Is(err, refstore.ErrConflict) {
		t.Fatalf("swap from the wrong key = %v, want ErrConflict", err)
	}
	if got, _ := s.Get("r"); !bytes.Equal(got, at1) {
		t.Fatal("a refused swap changed the record")
	}
	at2 := record(t, "r", k2, 2)
	if err := s.CompareAndSwap("r", k1, at2); err != nil {
		t.Fatalf("swap from the current key: %v", err)
	}
	if got, _ := s.Get("r"); !bytes.Equal(got, at2) {
		t.Fatal("the swap did not store the new record")
	}
	// The expectation is the key, not the record: a re-put of the same key
	// with a newer timestamp does not invalidate it.
	if err := s.Put("r", record(t, "r", k2, 3)); err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSwap("r", k2, record(t, "r", k3, 4)); err != nil {
		t.Fatalf("swap after a same-key re-put: %v", err)
	}
}

func TestCreate(t *testing.T) {
	s := open(t, t.TempDir())
	k1, k2 := blobKey(t, "one"), blobKey(t, "two")
	first := record(t, "r", k1, 1)
	if err := s.Create("r", first); err != nil {
		t.Fatal(err)
	}
	if err := s.Create("r", record(t, "r", k2, 2)); !errors.Is(err, refstore.ErrConflict) {
		t.Fatalf("second Create = %v, want ErrConflict", err)
	}
	if got, _ := s.Get("r"); !bytes.Equal(got, first) {
		t.Fatal("a refused Create changed the record")
	}
}

func TestCompareAndDelete(t *testing.T) {
	s := open(t, t.TempDir())
	k1, k2 := blobKey(t, "one"), blobKey(t, "two")
	if err := s.CompareAndDelete("r", k1); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("delete of an absent reference = %v, want ErrNotFound", err)
	}
	if err := s.Put("r", record(t, "r", k1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndDelete("r", k2); !errors.Is(err, refstore.ErrConflict) {
		t.Fatalf("delete from the wrong key = %v, want ErrConflict", err)
	}
	if _, err := s.Get("r"); err != nil {
		t.Fatalf("a refused delete removed the reference: %v", err)
	}
	if err := s.CompareAndDelete("r", k1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("r"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("Get after the delete = %v, want ErrNotFound", err)
	}
}

func TestCompareFormsRejectAnUndecodableCurrentRecord(t *testing.T) {
	s := open(t, t.TempDir())
	k1 := blobKey(t, "one")
	if err := s.Put("r", []byte("not cbor")); err != nil {
		t.Fatal(err)
	}
	err := s.CompareAndSwap("r", k1, record(t, "r", k1, 1))
	if err == nil || errors.Is(err, refstore.ErrConflict) || errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("swap over an undecodable record = %v, want a decode error", err)
	}
	if err := s.CompareAndDelete("r", k1); err == nil || errors.Is(err, refstore.ErrConflict) {
		t.Fatalf("delete of an undecodable record = %v, want a decode error", err)
	}
	if got, _ := s.Get("r"); string(got) != "not cbor" {
		t.Fatal("the undecodable record was changed")
	}
}

// Of many writers moving a reference away from the same expected key, across
// two handles, exactly one wins.
func TestConcurrentSwapsHaveOneWinner(t *testing.T) {
	dir := t.TempDir()
	handles := []*refstore.Store{open(t, dir), open(t, dir)}
	start := blobKey(t, "start")
	if err := handles[0].Put("r", record(t, "r", start, 0)); err != nil {
		t.Fatal(err)
	}
	const n = 8
	errs := make([]error, n)
	targets := make([]key.Key, n)
	records := make([][]byte, n)
	for i := range n {
		targets[i] = blobKey(t, string(rune('a'+i)))
		records[i] = record(t, "r", targets[i], int64(i+1))
	}
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			errs[i] = handles[i%2].CompareAndSwap("r", start, records[i])
		})
	}
	wg.Wait()
	winner := -1
	for i, err := range errs {
		switch {
		case err == nil && winner == -1:
			winner = i
		case err == nil:
			t.Fatalf("swaps %d and %d both succeeded", winner, i)
		case !errors.Is(err, refstore.ErrConflict):
			t.Errorf("swap %d: %v, want nil or ErrConflict", i, err)
		}
	}
	if winner == -1 {
		t.Fatal("no swap succeeded")
	}
	got, err := handles[0].Get("r")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := reference.Decode(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ref.Key, targets[winner][:]) {
		t.Fatal("the stored reference is not the winner's")
	}
}
