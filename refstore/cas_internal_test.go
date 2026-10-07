package refstore

import (
	"context"
	"testing"
	"time"

	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/reference"
	"github.com/draganm/rebma/refstore/internal/refsdb"
)

// A panic inside a write transaction must not leave SQLite's write lock held:
// a host that recovers panics would otherwise wedge every writer in every
// process until it exits.
func TestWriteTransactionsSurviveAPanic(t *testing.T) {
	shortBusyTimeout(t, 500*time.Millisecond)
	dir := t.TempDir()
	s, other := openT(t, dir, false), openT(t, dir, false)
	k, err := key.New(key.Blob, 1, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := reference.Reference{Name: "r", Key: k[:], CreatedAt: 1}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("r", raw); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the change did not panic")
			}
		}()
		s.ifAt("r", k, func(context.Context, *refsdb.Queries, []byte) (int64, error) { panic("boom") })
	}()
	if err := s.Put("after", []byte("1")); err != nil {
		t.Fatalf("the handle that panicked cannot write any more: %v", err)
	}
	if err := other.Put("after-2", []byte("2")); err != nil {
		t.Fatalf("another handle cannot write any more: %v", err)
	}
}
