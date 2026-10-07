package refstore

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/reference"
)

// A batch that fails after some of its changes ran leaves none of them
// behind, and its transaction ends: the next writer, on another handle, does
// not wait.
func TestUpdateBatchIsAllOrNothing(t *testing.T) {
	shortBusyTimeout(t, 500*time.Millisecond)
	dir := t.TempDir()
	s, other := openT(t, dir, false), openT(t, dir, false)
	blobKey := func(content string) key.Key {
		k, err := key.New(key.Blob, uint64(len(content)), []byte(content))
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	recordAt := func(name string, k key.Key) Record {
		raw, err := reference.Reference{Name: name, Key: k[:], CreatedAt: 1}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		return Record{Name: name, Data: raw}
	}
	k1, k2 := blobKey("one"), blobKey("two")
	if err := s.PutBatch([]Record{recordAt("a", k1), recordAt("b", k1)}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rawDB(t, dir).Exec(`CREATE TRIGGER refuse BEFORE INSERT ON refs
		WHEN NEW.name = CAST('refused' AS BLOB)
		BEGIN SELECT RAISE(ABORT, 'refused'); END`); err != nil {
		t.Fatal(err)
	}

	_, err = s.UpdateBatch(
		[]Publication{{Record: recordAt("a", k2), Old: &k1}, {Record: recordAt("refused", k2)}},
		[]Withdrawal{{Name: "b", Old: k1}},
	)
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("a batch with a refused insert = %v, want the trigger's error", err)
	}
	got, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("the failed batch left changes behind: %d records, want the %d from before", len(got), len(snapshot))
	}
	if err := other.Put("w", nil); err != nil {
		t.Fatalf("a writer on another handle after a failed batch: %v", err)
	}
}
