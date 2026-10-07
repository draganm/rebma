// The mark-and-sweep GC surface: Liveness (the dry run) and Compact (the
// sweep), driven by the collector in package gc against a MarkSet built
// from the references' closures. Ported from Mic92's bitmap GC
// (draganm/amber-store#9); see architecture/mark-sweep-gc.md and
// specs/gc.qnt.

package packstore

import (
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"os"
	"runtime"
	"time"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
	"golang.org/x/sync/errgroup"
)

type copyRec struct {
	g   *sealedSegment
	e   indexEntry
	rec []byte
}

// SegmentLiveness is one segment's liveness breakdown. Byte counts cover
// record bytes only, not footer overhead.
type SegmentLiveness struct {
	ID        uint64
	Sealed    bool
	LiveKeys  int
	DeadKeys  int
	LiveBytes uint64
	DeadBytes uint64
}

// CompactOpts tune one Compact pass.
type CompactOpts struct {
	// MinDeadRatio is the selection line: a sealed segment whose dead
	// bytes reach this fraction of its record bytes is rewritten.
	MinDeadRatio float64
	// Horizon, when nonzero, excludes segments sealed at or after it —
	// the gc grace period. The zero value means every sealed segment is
	// eligible.
	Horizon time.Time
	// Pace, when non-nil, is called with each copied record's size; the
	// collector uses it to cap copy bandwidth.
	Pace func(n int)
	// MaxCopyBytes, when non-nil, caps the live record bytes this pass
	// copies. Victim selection skips a segment whose live bytes exceed what
	// is left of the cap and goes on to smaller ones, so the pass never
	// copies more. Record bytes only: footers and allocation overhead sit
	// outside it. At a cap of 0 fully dead segments are still reclaimed,
	// since those need no copy, and nothing is sealed, since a footer would
	// grow the store. Nil, the zero value, means no cap.
	MaxCopyBytes *uint64
}

// copyBudget is the live record bytes the pass may copy: more than any
// segment holds when there is no cap.
func (o CompactOpts) copyBudget() uint64 {
	if o.MaxCopyBytes == nil {
		return math.MaxUint64
	}
	return *o.MaxCopyBytes
}

type CompactStats struct {
	SegmentsScanned   int // sealed segments considered
	SegmentsCompacted int
	Victims           []uint64 // compacted segment ids, ascending
	RecordsCopied     int
	BytesCopied       uint64
	BytesFreed        uint64 // victim file sizes, footers included
}

func (fv *footerView) allEntries() iter.Seq[indexEntry] {
	return func(yield func(indexEntry) bool) {
		for i := 0; i < int(fv.keyCount); i++ {
			row := fv.entries[i*indexEntrySize:]
			var e indexEntry
			copy(e.k[:], row[:32])
			e.off = binary.BigEndian.Uint64(row[32:])
			e.slen = binary.BigEndian.Uint32(row[40:])
			if !yield(e) {
				return
			}
		}
	}
}

func (info *SegmentLiveness) add(isLive bool, slen uint32) {
	n := uint64(amberpack.RecHeaderSize) + uint64(slen)
	if isLive {
		info.LiveKeys++
		info.LiveBytes += n
	} else {
		info.DeadKeys++
		info.DeadBytes += n
	}
}

func (g *sealedSegment) liveness(live func(key.Key) bool) SegmentLiveness {
	info := SegmentLiveness{ID: g.id, Sealed: true}
	for e := range g.fv.allEntries() {
		info.add(live(e.k), e.slen)
	}
	return info
}

// Liveness classifies every record by live(k) and reports per segment,
// ascending by id with the active segment last: the GC dry run.
func (s *Store) Liveness(live func(key.Key) bool) ([]SegmentLiveness, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	var report []SegmentLiveness
	for _, g := range s.sealed {
		report = append(report, g.liveness(live))
	}
	if a := s.active; a != nil {
		info := SegmentLiveness{ID: a.id}
		for k, loc := range a.index {
			info.add(live(k), loc.slen)
		}
		report = append(report, info)
	}
	for _, fa := range s.foreign { // other writers' active segments: never victims either
		info := SegmentLiveness{ID: fa.id}
		for k, loc := range fa.scan.index {
			info.add(live(k), loc.slen)
		}
		report = append(report, info)
	}
	return report, nil
}

// Compact seals the active segment, rewrites eligible sealed segments
// whose dead ratio reaches opts.MinDeadRatio (re-verifying live records
// while copying), and deletes victims only after the copies are durable.
// The caller must guarantee no ingest or reference publication overlaps
// (specs/gc.qnt). A grey set captured since BeginBarrier is consumed and
// kept alongside live. Under opts.MaxCopyBytes a segment the cap does not
// cover is left for a later pass, and at a cap of 0 the active segment is
// not sealed either.
func (s *Store) Compact(live func(key.Key) bool, opts CompactOpts) (CompactStats, error) {
	// Other stores' writers wait for the whole pass and look at the directory
	// again afterwards (gate.go); inside a collector's BeginSweep the gate is
	// held already. This store's own writes wait too: a duplicate check must
	// not race the removal of a segment, and those that bypass the collector
	// are held by nothing else.
	end, err := s.gate.beginExclusive(context.Background())
	if err != nil {
		return CompactStats{}, err
	}
	defer end()
	s.gate.pauseLocal()
	defer s.gate.resumeLocal()
	s.appendMu.Lock()
	defer s.appendMu.Unlock()

	if grey := s.takeGrey(); len(grey) > 0 {
		marked := live
		live = func(k key.Key) bool {
			_, ok := grey[k]
			return ok || marked(k)
		}
	}

	s.mu.RLock()
	closed, failed := s.closed, s.failed
	s.mu.RUnlock()
	var stats CompactStats
	if closed {
		return stats, ErrClosed
	}
	if failed != nil {
		return stats, failed
	}
	// Sealing writes a footer, which grows the store. A zero budget asks for
	// no growth, and must still reach the dead segments it can reclaim
	// without copying. sealIdleLocked is skipped with it: it lets go of this
	// store's segment on the premise that the seal emptied it, then adopts
	// and seals whatever it finds, this one included.
	if opts.copyBudget() > 0 {
		if err := s.sealActiveLocked(); err != nil {
			return stats, err
		}
		if err := s.sealIdleLocked(); err != nil {
			return stats, err
		}
	}

	victims, err := s.selectVictims(live, opts, &stats)
	if err != nil {
		return stats, err
	}
	if len(victims) == 0 {
		return stats, nil
	}
	if err := s.copyLive(victims, live, opts.Pace, &stats); err != nil {
		return stats, err
	}
	if s.cfg.sync && s.active != nil {
		if err := s.active.f.Sync(); err != nil {
			s.setFailed(err)
			return stats, err
		}
		s.active.sc.synced(s.active.size)
	}
	return stats, s.removeVictims(victims, &stats)
}

func (s *Store) selectVictims(live func(key.Key) bool, opts CompactOpts, stats *CompactStats) ([]*sealedSegment, error) {
	s.mu.RLock()
	segs := make([]*sealedSegment, len(s.sealed))
	copy(segs, s.sealed)
	s.mu.RUnlock()
	stats.SegmentsScanned = len(segs)

	copyLeft := opts.copyBudget()
	var victims []*sealedSegment
	for _, g := range segs {
		if !opts.Horizon.IsZero() {
			st, err := os.Stat(g.path)
			if err != nil {
				return nil, err
			}
			if !st.ModTime().Before(opts.Horizon) {
				continue
			}
		}
		info := g.liveness(live)
		total := info.LiveBytes + info.DeadBytes
		if info.DeadKeys > 0 && float64(info.DeadBytes) >= opts.MinDeadRatio*float64(total) {
			// More to copy than is left of the budget: skipped. A smaller
			// segment after it is still considered.
			if info.LiveBytes > copyLeft {
				continue
			}
			copyLeft -= info.LiveBytes
			victims = append(victims, g)
			stats.Victims = append(stats.Victims, g.id)
		}
	}
	return victims, nil
}

// copyLive appends every live victim record that has no copy in a surviving
// segment. Verification (decompress + re-hash) dominates, so workers verify
// in parallel while the calling goroutine appends whatever is ready.
func (s *Store) copyLive(victims []*sealedSegment, live func(key.Key) bool, pace func(int), stats *CompactStats) error {
	victimID := make(map[uint64]bool, len(victims))
	for _, g := range victims {
		victimID[g.id] = true
	}
	survivorHas := func(k key.Key) bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if s.reliableActiveLocked(k) { // not a copy a live writer has yet to sync: the victim's may be the durable one
			return true
		}
		for _, g := range s.sealed {
			if !victimID[g.id] && g.has(k) {
				return true
			}
		}
		return false
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cands := make(chan copyRec, 4*runtime.GOMAXPROCS(0))
	verified := make(chan copyRec, 4*runtime.GOMAXPROCS(0))
	var pipe errgroup.Group
	pipe.Go(func() error {
		defer close(cands)
		for _, g := range victims {
			bodyLen := uint64(g.fv.bodyLen)
			for e := range g.fv.allEntries() {
				if !live(e.k) || survivorHas(e.k) {
					continue
				}
				if e.off < uint64(len(magicHeader)) || e.off > bodyLen ||
					uint64(amberpack.RecHeaderSize)+uint64(e.slen) > bodyLen-e.off {
					cancel()
					return fmt.Errorf("%w: %s: index entry out of bounds", ErrCorrupt, g.path)
				}
				rec := g.mm[e.off : e.off+uint64(amberpack.RecHeaderSize)+uint64(e.slen)]
				select {
				case cands <- copyRec{g, e, rec}:
				case <-ctx.Done():
					return nil
				}
			}
		}
		return nil
	})
	for range runtime.GOMAXPROCS(0) {
		pipe.Go(func() error {
			for c := range cands {
				if err := verifyRecord(c.g, c.e, c.rec); err != nil {
					cancel()
					return err
				}
				select {
				case verified <- c:
				case <-ctx.Done():
				}
			}
			return nil
		})
	}
	go func() {
		pipe.Wait()
		close(verified)
	}()

	var appendErr error
	for c := range verified {
		if appendErr != nil {
			continue // drain
		}
		if appendErr = s.appendLocked(c.e.k, c.rec, false); appendErr != nil {
			cancel()
			continue
		}
		stats.RecordsCopied++
		stats.BytesCopied += uint64(len(c.rec))
		if pace != nil {
			pace(len(c.rec))
		}
	}
	return cmp.Or(appendErr, pipe.Wait())
}

func (s *Store) removeVictims(victims []*sealedSegment, stats *CompactStats) error {
	victimID := make(map[uint64]bool, len(victims))
	for _, g := range victims {
		victimID[g.id] = true
	}
	// A lookup that finds nothing lists the directory (view.go). Between the
	// victims leaving the view and leaving the directory, such a listing
	// would map them again, and the duplicate check would go on finding what
	// is gone. No listing runs in between: refreshMu is held. Unlinking does
	// not have to wait for scrubs, only unmapping does — and must not wait
	// here: a scrub's callback may be waiting for refreshMu.
	s.refreshMu.Lock()
	s.mu.Lock()
	// Fresh slice: never mutate an array a concurrent reader may still
	// hold (same discipline as Remove).
	kept := make([]*sealedSegment, 0, len(s.sealed)-len(victims))
	for _, g := range s.sealed {
		if !victimID[g.id] {
			kept = append(kept, g)
		}
	}
	s.sealed = kept
	s.mu.Unlock()
	if s.afterDetach != nil {
		s.afterDetach()
	}
	var firstErr error
	for _, g := range victims {
		if err := os.Remove(g.path); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.cfg.sync {
		if err := s.dirF.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.refreshMu.Unlock()

	s.waitScrubs() // in-flight scrub walks may still read the victims' mmaps
	for _, g := range victims {
		stats.BytesFreed += uint64(len(g.mm))
		if err := g.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	stats.SegmentsCompacted = len(victims)
	return firstErr
}

// verifyRecord checks rec against its index entry: framing CRC, key match,
// and the payload rehashed against the key.
func verifyRecord(g *sealedSegment, e indexEntry, rec []byte) error {
	if err := checkRecord(e.k, rec); err != nil {
		return fmt.Errorf("%w: %s: record at offset %d: %w", ErrCorrupt, g.path, e.off, err)
	}
	return nil
}

// checkRecord is the full record scrub: framing CRC, key match, and the
// payload rehashed against the key.
func checkRecord(k key.Key, rec []byte) error {
	parsed, err := amberpack.ParseRecord(rec)
	if err != nil {
		return err
	}
	if parsed.Key != k {
		return fmt.Errorf("record keyed %s, expected %s", parsed.Key, k)
	}
	payload, err := amberpack.DecodePayload(parsed.Flags, parsed.Ulen, rec[amberpack.RecHeaderSize:])
	if err != nil {
		return err
	}
	return verifyObject(Object{Key: k, Data: payload})
}
