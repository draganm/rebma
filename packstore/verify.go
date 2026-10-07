package packstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/commit"
	"github.com/draganm/rebma/key"
	"github.com/zeebo/blake3"
)

// ErrVerify is returned (wrapped) when an object's key does not match its
// payload. Callers distinguish it with errors.Is to map to a client error.
var ErrVerify = errors.New("packstore: object verification failed")

// Verify scrubs every sealed segment: walks the body record by record
// (validating framing, CRCs, and that each payload re-hashes to its key),
// recomputes the index section and compares it bytewise with the footer's,
// and checks the filter contains every body key. The active segment is
// covered by tail-scan on reopen, not by Verify. Segments sealed by
// rotations that happen after Verify snapshots the segment list are not
// covered by that call. Close blocks until in-flight Verify walks finish;
// cancel the context to stop a scrub early.
func (s *Store) Verify(ctx context.Context) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrClosed
	}
	// Registering under the read lock orders beginScrub strictly before any
	// Close (which sets closed under the write lock and then waits): a scrub
	// can never start after Close begins, and Close never unmaps while a
	// scrub is walking the mappings.
	s.beginScrub()
	defer s.endScrub()
	segs := make([]*sealedSegment, len(s.sealed))
	copy(segs, s.sealed)
	s.mu.RUnlock()

	for _, seg := range segs {
		if err := seg.verify(ctx); err != nil {
			return err
		}
	}
	return nil
}

// verify scrubs one sealed segment. The segment is immutable and the caller
// holds no locks: scrubbing runs concurrently with reads and writes.
func (g *sealedSegment) verify(ctx context.Context) error {
	var entries []indexEntry
	off := int64(len(magicHeader))
	for off < g.fv.bodyLen {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, err := amberpack.ParseRecord(g.mm[off:g.fv.bodyLen])
		if err != nil {
			return fmt.Errorf("%s: record at offset %d: %w", g.path, off, err)
		}
		payload, err := amberpack.DecodePayload(rec.Flags, rec.Ulen, g.mm[off+amberpack.RecHeaderSize:off+amberpack.RecHeaderSize+int64(rec.Slen)])
		if err != nil {
			return fmt.Errorf("%s: record at offset %d: %w", g.path, off, err)
		}
		if err := verifyObject(Object{Key: rec.Key, Data: payload}); err != nil {
			// Scrub findings are corruption: both sentinels match via errors.Is.
			return fmt.Errorf("%w: %s: record at offset %d: %w", ErrCorrupt, g.path, off, err)
		}
		if !g.fv.filter.Contains(filterKey(rec.Key)) {
			return fmt.Errorf("%w: %s: filter missing key %s (offset %d)", ErrCorrupt, g.path, rec.Key, off)
		}
		entries = append(entries, indexEntry{k: rec.Key, off: uint64(off), slen: rec.Slen})
		off += amberpack.RecHeaderSize + int64(rec.Slen)
	}
	// Defensive only: parseRecord bounds every record against bodyLen, so the
	// walk can only exit exactly at bodyLen or via an error above.
	if off != g.fv.bodyLen {
		return fmt.Errorf("%w: %s: records end at %d, trailer says %d", ErrCorrupt, g.path, off, g.fv.bodyLen)
	}
	if uint64(len(entries)) != g.fv.keyCount {
		return fmt.Errorf("%w: %s: body has %d records, trailer says %d", ErrCorrupt, g.path, len(entries), g.fv.keyCount)
	}
	rebuilt := buildIndexSection(entries)
	stored := g.mm[g.fv.indexOff : g.fv.indexOff+g.fv.indexLen]
	if !bytes.Equal(rebuilt, stored) {
		return fmt.Errorf("%w: %s: index section does not match body", ErrCorrupt, g.path)
	}
	return nil
}

// verifyObject recomputes o.Key from o.Data and reports ErrVerify on mismatch.
// For Blob and XattrSet, whose key length is the serialized byte length, it
// also checks the length field; and for Commit, whose length is a footprint —
// own bytes plus the trees it records — which the commit's own bytes suffice
// to recompute. A commit that does not decode fails verification. The other
// aggregate types (FileNode/DirLeaf/DirNode) carry a logical length the store
// does not recompute, so only their hash is checked.
func verifyObject(o Object) error {
	sum := blake3.Sum256(o.Data)
	want, err := key.NewFromHash(o.Key.Type(), o.Key.Length(), sum)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrVerify, o.Key, err)
	}
	if want != o.Key {
		return fmt.Errorf("%w: payload hashes to %s, not %s", ErrVerify, want, o.Key)
	}
	switch o.Key.Type() {
	case key.Blob, key.XattrSet:
		if o.Key.Length() != uint64(len(o.Data)) {
			return fmt.Errorf("%w: %s length field %d != payload %d", ErrVerify, o.Key, o.Key.Length(), len(o.Data))
		}
	case key.Commit:
		c, err := commit.Decode(o.Data)
		if err != nil {
			return fmt.Errorf("%w: %s: %v", ErrVerify, o.Key, err)
		}
		want, err := commit.Footprint(uint64(len(o.Data)), c.Trees())
		if err != nil {
			return fmt.Errorf("%w: %s: %v", ErrVerify, o.Key, err)
		}
		if o.Key.Length() != want {
			return fmt.Errorf("%w: %s length field %d != footprint %d (own %d bytes plus its trees)", ErrVerify, o.Key, o.Key.Length(), want, len(o.Data))
		}
	}
	return nil
}
