package refstore_test

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/draganm/rebma/refstore"
)

func open(t *testing.T, dir string) *refstore.Store {
	t.Helper()
	s, err := refstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutGetDelete(t *testing.T) {
	s := open(t, t.TempDir())

	if _, err := s.Get("missing"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
	}
	if err := s.Put("a/b", []byte("rec1")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("a/b")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("rec1")) {
		t.Fatalf("Get = %q, want rec1", got)
	}
	// Overwrite is unconditional.
	if err := s.Put("a/b", []byte("rec2")); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get("a/b")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("rec2")) {
		t.Fatalf("Get after overwrite = %q, want rec2", got)
	}
	if err := s.Delete("a/b"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("a/b"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("Get after delete = %v, want ErrNotFound", err)
	}
	if err := s.Delete("a/b"); !errors.Is(err, refstore.ErrNotFound) {
		t.Fatalf("Delete(absent) = %v, want ErrNotFound", err)
	}
}

func TestAllSortedByName(t *testing.T) {
	s := open(t, t.TempDir())
	for _, n := range []string{"zeta", "alpha", "mid/dle"} {
		if err := s.Put(n, []byte("v-"+n)); err != nil {
			t.Fatal(err)
		}
	}
	recs, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{"alpha", "mid/dle", "zeta"}
	if len(recs) != len(wantNames) {
		t.Fatalf("All returned %d records, want %d", len(recs), len(wantNames))
	}
	for i, want := range wantNames {
		if recs[i].Name != want {
			t.Fatalf("recs[%d].Name = %q, want %q", i, recs[i].Name, want)
		}
		if !bytes.Equal(recs[i].Data, []byte("v-"+want)) {
			t.Fatalf("recs[%d].Data = %q", i, recs[i].Data)
		}
	}
}

func TestSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := refstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("keep", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := open(t, dir)
	got, err := s2.Get("keep")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get after reopen = %q, want v", got)
	}
}

func TestAllEmpty(t *testing.T) {
	s := open(t, t.TempDir())
	recs, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("All on empty store = %d records, want 0", len(recs))
	}
}

func TestConcurrentDeleteReportsOnce(t *testing.T) {
	s := open(t, t.TempDir())
	if err := s.Put("target", []byte("data")); err != nil {
		t.Fatal(err)
	}

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	// Use a gate so all goroutines start as close together as possible.
	var gate sync.WaitGroup
	gate.Add(1)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			gate.Wait()
			errs[i] = s.Delete("target")
		}(i)
	}
	gate.Done()
	wg.Wait()

	var nilCount int
	for _, err := range errs {
		switch {
		case err == nil:
			nilCount++
		case errors.Is(err, refstore.ErrNotFound):
			// expected for all-but-one
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if nilCount != 1 {
		t.Errorf("exactly 1 Delete should succeed, got %d", nilCount)
	}
}

func TestWipe(t *testing.T) {
	s := open(t, t.TempDir())
	for _, n := range []string{"a", "b", "c"} {
		if err := s.Put(n, []byte("v-"+n)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Wipe(); err != nil {
		t.Fatal(err)
	}
	recs, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("All after Wipe: %d records, want 0", len(recs))
	}
	if _, err := s.Get("a"); err != refstore.ErrNotFound {
		t.Fatalf("Get after Wipe: %v, want ErrNotFound", err)
	}
	// The store stays usable.
	if err := s.Put("d", []byte("v-d")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("d")
	if err != nil || string(got) != "v-d" {
		t.Fatalf("Put/Get after Wipe: %q %v", got, err)
	}
}
