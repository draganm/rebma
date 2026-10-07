package refstore_test

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/refstore"
)

// recordAt is the stored form of a reference called name pointing at k.
func recordAt(t *testing.T, name string, k key.Key) refstore.Record {
	t.Helper()
	return refstore.Record{Name: name, Data: record(t, name, k, 1)}
}

// storedRecords is every record of s, in name order.
func storedRecords(t *testing.T, s *refstore.Store) []refstore.Record {
	t.Helper()
	all, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	return all
}

// storedNames is the name of every record of s, in order.
func storedNames(t *testing.T, s *refstore.Store) []string {
	t.Helper()
	var names []string
	for _, r := range storedRecords(t, s) {
		names = append(names, r.Name)
	}
	return names
}

func sameRecords(a, b []refstore.Record) bool {
	return slices.EqualFunc(a, b, func(x, y refstore.Record) bool {
		return x.Name == y.Name && bytes.Equal(x.Data, y.Data)
	})
}

func TestUpdateBatchWithdrawsAndPublishesInOneCommit(t *testing.T) {
	s := open(t, t.TempDir())
	k1, k2 := blobKey(t, "one"), blobKey(t, "two")
	if err := s.PutBatch([]refstore.Record{recordAt(t, "old1", k1), recordAt(t, "old2", k1), recordAt(t, "moved", k1)}); err != nil {
		t.Fatal(err)
	}

	removed, err := s.UpdateBatch(
		[]refstore.Publication{{Record: recordAt(t, "new", k2)}, {Record: recordAt(t, "moved", k2), Old: &k1}},
		[]refstore.Withdrawal{{Name: "old1", Old: k1}, {Name: "old2", Old: k1}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	if got, want := storedNames(t, s), []string{"moved", "new"}; !slices.Equal(got, want) {
		t.Fatalf("names after the batch = %q, want %q", got, want)
	}
	if got, err := s.Get("moved"); err != nil || !bytes.Equal(got, record(t, "moved", k2, 1)) {
		t.Fatalf("the batch did not store the new record of \"moved\": %v", err)
	}
}

// The caller asked for the name to be gone, and it is: not a conflict, and
// not counted.
func TestUpdateBatchWithdrawsAMissingNameQuietly(t *testing.T) {
	s := open(t, t.TempDir())
	k1 := blobKey(t, "one")
	if err := s.Put("a", record(t, "a", k1, 1)); err != nil {
		t.Fatal(err)
	}
	removed, err := s.UpdateBatch(nil, []refstore.Withdrawal{{Name: "a", Old: k1}, {Name: "z", Old: k1}})
	if err != nil {
		t.Fatalf("withdrawal of a name that is already gone: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if got := storedNames(t, s); len(got) != 0 {
		t.Fatalf("names after the batch = %q, want none", got)
	}
}

// The withdrawal runs first, so the caller gets the record rather than a hole.
func TestUpdateBatchPublishesANameItAlsoWithdraws(t *testing.T) {
	s := open(t, t.TempDir())
	k1, k2 := blobKey(t, "one"), blobKey(t, "two")
	if err := s.Put("x", record(t, "x", k1, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateBatch(
		[]refstore.Publication{{Record: recordAt(t, "x", k2), Old: &k1}},
		[]refstore.Withdrawal{{Name: "x", Old: k1}},
	); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get("x"); err != nil || !bytes.Equal(got, record(t, "x", k2, 1)) {
		t.Fatalf("a name both withdrawn and published does not hold the published record: %v", err)
	}
}

func TestEmptyUpdateBatchChangesNothing(t *testing.T) {
	s := open(t, t.TempDir())
	if err := s.Put("x", []byte("v")); err != nil {
		t.Fatal(err)
	}
	removed, err := s.UpdateBatch(nil, nil)
	if err != nil || removed != 0 {
		t.Fatalf("empty UpdateBatch = %d, %v; want 0, nil", removed, err)
	}
	if got, want := storedNames(t, s), []string{"x"}; !slices.Equal(got, want) {
		t.Fatalf("names after an empty batch = %q, want %q", got, want)
	}
	if got, err := s.Get("x"); err != nil || string(got) != "v" {
		t.Fatalf("Get after an empty batch = %q, %v", got, err)
	}
}

// Any expectation that fails, here mostly because another handle moved one
// name after the caller read it, is a conflict, and none of the batch's
// other changes land.
func TestUpdateBatchConflictRollsTheWholeBatchBack(t *testing.T) {
	dir := t.TempDir()
	s, other := open(t, dir), open(t, dir)
	k1, k2, k3 := blobKey(t, "one"), blobKey(t, "two"), blobKey(t, "three")
	before := []refstore.Record{recordAt(t, "a", k1), recordAt(t, "b", k1), recordAt(t, "c", k1)}
	if err := s.PutBatch(before); err != nil {
		t.Fatal(err)
	}
	snapshot := storedRecords(t, s)

	for _, try := range []struct {
		what    string
		records []refstore.Publication
		names   []refstore.Withdrawal
	}{
		{
			"a withdrawal of a moved name",
			[]refstore.Publication{{Record: recordAt(t, "new", k3)}},
			[]refstore.Withdrawal{{Name: "a", Old: k1}, {Name: "b", Old: k1}},
		},
		{
			"a replacement of a moved name",
			[]refstore.Publication{{Record: recordAt(t, "a", k3), Old: &k1}, {Record: recordAt(t, "b", k3), Old: &k1}},
			[]refstore.Withdrawal{{Name: "c", Old: k1}},
		},
		{
			"a creation of a name that exists",
			[]refstore.Publication{{Record: recordAt(t, "c", k3)}},
			[]refstore.Withdrawal{{Name: "a", Old: k1}},
		},
		{
			"a replacement of a missing name",
			[]refstore.Publication{{Record: recordAt(t, "z", k3), Old: &k1}},
			[]refstore.Withdrawal{{Name: "a", Old: k1}},
		},
	} {
		if err := other.Put("b", record(t, "b", k2, 1)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.UpdateBatch(try.records, try.names); !errors.Is(err, refstore.ErrConflict) {
			t.Fatalf("%s = %v, want ErrConflict", try.what, err)
		}
		if err := other.PutBatch(before); err != nil {
			t.Fatal(err)
		}
		if got := storedRecords(t, s); !sameRecords(got, snapshot) {
			t.Fatalf("%s: the refused batch changed the store: names %q", try.what, storedNames(t, s))
		}
	}
}
