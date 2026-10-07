// Format-neutral views of sealed segments: segment listing, index scans,
// raw record reads and re-appends, removal. The mark-and-sweep collector
// (package gc, architecture/mark-sweep-gc.md) uses Segments for its
// status report; the rest stays as the store's generic GC surface.

package packstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

// ErrUnknownSegment reports an id that names no sealed segment (never
// sealed, or already removed).
var ErrUnknownSegment = errors.New("packstore: no such segment")

// writeToken marks one in-flight exported write call for the GC horizon.
type writeToken struct{ _ byte }

// beginWrite opens an exported write: a span of the gate, which keeps other
// stores' sweeps out, and a token for this store's GC horizon.
func (s *Store) beginWrite() (*writeToken, error) {
	if err := s.gate.beginShared(); err != nil {
		return nil, err
	}
	t := new(writeToken)
	s.writesMu.Lock()
	s.writes[t] = time.Now()
	s.writesMu.Unlock()
	return t, nil
}

func (s *Store) endWrite(t *writeToken) {
	s.writesMu.Lock()
	delete(s.writes, t)
	s.writesMu.Unlock()
	s.gate.endShared()
}

// OldestInflightWrite returns the start time of the oldest Put, WriteBatch
// or WriteParallel still in progress. The GC horizon never passes it.
func (s *Store) OldestInflightWrite() (time.Time, bool) {
	s.writesMu.Lock()
	defer s.writesMu.Unlock()
	var min time.Time
	for _, t := range s.writes {
		if min.IsZero() || t.Before(min) {
			min = t
		}
	}
	return min, !min.IsZero()
}

// SegmentInfo describes one sealed segment.
type SegmentInfo struct {
	ID     uint64
	Sealed time.Time // .seg mtime: the footer write is the file's last write
	Body   int64     // record bytes: body length minus the header magic
	Keys   uint64
}

// Segments lists the sealed segments, ascending id. The active segment is
// not listed; it is never a reaping victim.
func (s *Store) Segments() ([]SegmentInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	out := make([]SegmentInfo, 0, len(s.sealed))
	for _, g := range s.sealed {
		st, err := os.Stat(g.path)
		if err != nil {
			return nil, err
		}
		out = append(out, SegmentInfo{
			ID:     g.id,
			Sealed: st.ModTime(),
			Body:   g.fv.bodyLen - int64(len(magicHeader)),
			Keys:   g.fv.keyCount,
		})
	}
	return out, nil
}

// pinSegment finds sealed segment id and registers a scrub so the mmap
// outlives the caller's walk (Remove waits for scrubs before munmap). The
// caller must s.endScrub() when finished with the segment.
func (s *Store) pinSegment(id uint64) (*sealedSegment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	for _, g := range s.sealed {
		if g.id == id {
			s.beginScrub()
			return g, nil
		}
	}
	return nil, ErrUnknownSegment
}

// ScanIndex walks segment id's footer index in index order (ascending key,
// which the first-byte fanout buckets), calling fn with each entry's key, record
// offset and stored payload length. No pack body is read.
func (s *Store) ScanIndex(id uint64, fn func(k key.Key, off uint64, slen uint32)) error {
	seg, err := s.pinSegment(id)
	if err != nil {
		return err
	}
	defer s.endScrub()
	fv := seg.fv
	for i := uint64(0); i < fv.keyCount; i++ {
		e := fv.entries[i*indexEntrySize : (i+1)*indexEntrySize]
		var k key.Key
		copy(k[:], e[:key.Size])
		fn(k, binary.BigEndian.Uint64(e[32:40]), binary.BigEndian.Uint32(e[40:44]))
	}
	return nil
}

// Record returns the raw record at off in segment id, CRC-checked. The
// returned bytes are caller-owned and re-appendable verbatim.
func (s *Store) Record(id, off uint64) ([]byte, error) {
	seg, err := s.pinSegment(id)
	if err != nil {
		return nil, err
	}
	defer s.endScrub()
	body := uint64(seg.fv.bodyLen)
	// Overflow-safe: off may come from a crafted index; off+RecHeaderSize
	// could wrap. Same shape as the sealed-segment readers in footer.go.
	if off < uint64(len(magicHeader)) || off > body || uint64(amberpack.RecHeaderSize) > body-off {
		return nil, fmt.Errorf("%w: %s: record offset %d out of range", ErrCorrupt, seg.path, off)
	}
	rec, err := amberpack.ParseRecord(seg.mm[off:body])
	if err != nil {
		return nil, fmt.Errorf("%s: offset %d: %w", seg.path, off, err)
	}
	end := off + amberpack.RecHeaderSize + uint64(rec.Slen)
	out := make([]byte, end-off)
	copy(out, seg.mm[off:end])
	return out, nil
}

// HasOutside reports whether k is stored anywhere but segment id: the
// active segment or any other sealed segment. Reaping uses it to skip
// records that already survived.
func (s *Store) HasOutside(id uint64, k key.Key) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, ErrClosed
	}
	if s.reliableActiveLocked(k) {
		return true, nil
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		if s.sealed[i].id == id {
			continue
		}
		if s.sealed[i].has(k) {
			return true, nil
		}
	}
	return false, nil
}

// AppendRecord re-appends an already-encoded record through the normal
// append path: no decode, no re-encode, no fsync — callers batch appends
// and call Sync. raw must be exactly one record, CRC-valid, keyed k. A nil
// raw is rejected outright: prepare would read it as an Object carrying
// Data instead of a Record and happily append an empty payload under k.
func (s *Store) AppendRecord(k key.Key, raw []byte) error {
	if raw == nil {
		return fmt.Errorf("%w: nil record", ErrCorrupt)
	}
	w, gerr := s.beginWrite()
	if gerr != nil {
		return gerr
	}
	defer s.endWrite(w)
	rec, _, err := prepare(Object{Key: k, Record: raw}, false)
	if err != nil {
		return err
	}
	return s.append(k, rec, false)
}

// Sync fsyncs the active segment, making every append so far durable —
// AppendRecord batches end with one Sync.
func (s *Store) Sync() error {
	return s.syncActive()
}

// Remove drops sealed segment id: out of the probe list under the write
// lock (draining in-flight reads, like a seal), munmap after in-flight
// scrubs (as Close does), unlink, fsync the directory. The caller ensures
// every live record was copied out first.
//
// waitScrubs is a global gate: it waits out every in-flight scrub, not just
// this segment's pins; under sustained ScanIndex/Verify traffic that can
// stall Remove (and a gc deletePack holding its removal lock). Per-segment
// pinning is the known follow-up before a long-running embedder leans on
// this.
func (s *Store) Remove(id uint64) error {
	end, err := s.gate.beginExclusive(context.Background()) // other stores' writers wait, and look again afterwards
	if err != nil {
		return err
	}
	defer end()
	s.appendMu.Lock()
	// Held until the file is gone: a lookup's listing between the segment
	// leaving the view and leaving the directory would map it again
	// (removeVictims has the whole story).
	s.refreshMu.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.refreshMu.Unlock()
		s.appendMu.Unlock()
		return ErrClosed
	}
	idx := -1
	for i, g := range s.sealed {
		if g.id == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		s.mu.Unlock()
		s.refreshMu.Unlock()
		s.appendMu.Unlock()
		return ErrUnknownSegment
	}
	seg := s.sealed[idx]
	// Fresh slice: never mutate an array a concurrent reader may still hold;
	// the old array stays valid for anyone who grabbed it.
	ns := make([]*sealedSegment, 0, len(s.sealed)-1)
	ns = append(ns, s.sealed[:idx]...)
	ns = append(ns, s.sealed[idx+1:]...)
	s.sealed = ns
	s.mu.Unlock()
	if s.afterDetach != nil {
		s.afterDetach()
	}
	firstErr := os.Remove(seg.path)
	if s.cfg.sync {
		if err := s.dirF.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.refreshMu.Unlock()
	s.appendMu.Unlock()

	// Pre-existing scrubs may still be walking seg's mmap; munmap under one
	// is an uncatchable SIGSEGV. waitScrubs tolerates a scrub registering
	// after the unlock above: it can only reach segments still in s.sealed,
	// never seg (already detached), so the wait still terminates correctly.
	s.waitScrubs()
	if err := seg.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
