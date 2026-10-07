package packstore

import (
	"slices"

	"github.com/draganm/rebma/key"
)

// MarkSet is a liveness mark over a snapshot: one bit per sealed record,
// slotted by footer index, plus a map for the active segment. Not
// concurrency-safe. Run it under the writer quiesce GC requires.
// Snapshots own their footer indexes, using 44 additional bytes per sealed key.
// This keeps lookups valid after a repair replaces and unmaps a segment.
type MarkSet struct {
	segs   []*footerView
	bits   [][]uint64
	active map[key.Key]bool // present in active segment, value = marked
	marked int              // keys marked so far
}

func (s *Store) NewMarkSet() *MarkSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := &MarkSet{active: map[key.Key]bool{}}
	for _, g := range s.sealed {
		// Own the index bytes: repair or removal can retire the original mmap.
		footer := *g.fv
		footer.entries = slices.Clone(footer.entries)
		m.segs = append(m.segs, &footer)
		m.bits = append(m.bits, make([]uint64, (g.fv.keyCount+63)/64))
	}
	if s.active != nil {
		for k := range s.active.index {
			m.active[k] = false
		}
	}
	for _, fa := range s.foreign { // present, and as little a victim as the store's own
		for k := range fa.scan.index {
			m.active[k] = false
		}
	}
	return m
}

// locate finds k newest-first, matching the read path.
func (m *MarkSet) locate(k key.Key) (seg, pos int, inActive, ok bool) {
	if _, ok := m.active[k]; ok {
		return 0, 0, true, true
	}
	for i := len(m.segs) - 1; i >= 0; i-- {
		g := m.segs[i]
		if !g.filter.Contains(filterKey(k)) {
			continue
		}
		if p, found := g.lookupPos(k); found {
			return i, p, false, true
		}
	}
	return 0, 0, false, false
}

// Mark marks k, reporting whether it was unmarked before and whether it is
// present in the snapshot at all.
func (m *MarkSet) Mark(k key.Key) (newly, present bool) {
	seg, pos, inActive, ok := m.locate(k)
	if !ok {
		return false, false
	}
	if inActive {
		newly = !m.active[k]
		m.active[k] = true
		if newly {
			m.marked++
		}
		return newly, true
	}
	w, b := &m.bits[seg][pos/64], uint64(1)<<(pos%64)
	newly = *w&b == 0
	*w |= b
	if newly {
		m.marked++
	}
	return newly, true
}

func (m *MarkSet) Contains(k key.Key) bool {
	seg, pos, inActive, ok := m.locate(k)
	if !ok {
		return false
	}
	if inActive {
		return m.active[k]
	}
	return m.bits[seg][pos/64]&(1<<(pos%64)) != 0
}

// Marked returns the number of distinct keys marked so far.
func (m *MarkSet) Marked() int {
	return m.marked
}
