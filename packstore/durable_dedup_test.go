package packstore

import (
	"testing"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

// ownCopy reports whether the store's own active segment holds k.
func ownCopy(s *Store, k key.Key) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.active == nil {
		return false
	}
	_, ok := s.active.index[k]
	return ok
}

// A store that syncs acknowledges a write only when losing power would not
// lose the object. A copy in another store's active segment counts as a
// duplicate only as far as that store has synced it.
func TestDedupDoesNotRelyOnAnotherStoresUnsyncedRecords(t *testing.T) {
	o := testObjects(t, 1)[0]

	dir := t.TempDir()
	putAll(t, openStore(t, dir, WithSync(false)), []Object{o}) // written, never synced, its writer still open
	s := openStore(t, dir)                                     // syncs
	wantObjects(t, s, []Object{o})
	putAll(t, s, []Object{o})
	if !ownCopy(s, o.Key) {
		t.Fatal("a syncing store acknowledged a write whose only copy another store has yet to sync")
	}

	dir = t.TempDir()
	putAll(t, openStore(t, dir), []Object{o}) // written and synced
	s = openStore(t, dir)
	wantObjects(t, s, []Object{o})
	putAll(t, s, []Object{o})
	if ownCopy(s, o.Key) {
		t.Fatal("a second copy was written of a record another store had synced")
	}
}

// Compaction must not delete a synced copy on the strength of one that a live
// writer has yet to sync.
func TestCompactDoesNotLeaveTheOnlyCopyUnsynced(t *testing.T) {
	fill := func(tag byte) Object {
		data := incompressible(4 << 10)
		data[0] = tag
		return blobObj(t, data)
	}
	kept, garbage := fill(1), fill(2)

	dir := t.TempDir()
	putAll(t, openStore(t, dir, WithSync(false)), []Object{kept}) // a live writer that never syncs

	s := openStore(t, dir, WithSegmentSize(8<<10)) // syncs
	rec, err := amberpack.EncodeRecord(kept.Key, kept.Data)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AppendRecord(kept.Key, rec); err != nil { // a copy of its own, whatever the duplicate check thinks
		t.Fatal(err)
	}
	putAll(t, s, []Object{garbage}) // fills the segment: sealed, and synced
	if _, err := s.Compact(func(k key.Key) bool { return k == kept.Key }, CompactOpts{MinDeadRatio: 0.1}); err != nil {
		t.Fatal(err)
	}

	s.mu.RLock()
	sealedCopy := false
	for _, g := range s.sealed {
		sealedCopy = sealedCopy || g.has(kept.Key)
	}
	s.mu.RUnlock()
	if !sealedCopy && !ownCopy(s, kept.Key) {
		t.Fatal("the pass deleted a synced copy and left only the one another store has yet to sync")
	}
}
