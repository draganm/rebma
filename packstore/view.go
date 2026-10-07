package packstore

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

// A store sees a directory that other stores — other processes — write to.
// Its view is the sealed segments it has mapped plus a read-only index of
// every active segment it does not own. The view is built at Open and goes
// stale; a lookup that misses refreshes it (refreshAfterMiss). A reader never
// takes a lock and never modifies a file.

// errStaleView reports that a foreign active segment's index named a record
// that is not there. The lookup rebuilds the view and tries once more.
var errStaleView = errors.New("packstore: an active segment's index does not match its data")

// tmpSuffix marks an active segment that is still being created (active.go).
const tmpSuffix = ".tmp"

// racyWindow is how old the directory's modification time must be, when the
// directory is listed, before an unchanged time is taken to mean an unchanged
// directory. On a filesystem with one-second timestamps a segment created in
// the same second as the listing would otherwise go unnoticed for good. (The
// trick, and the name, are git's, for its index.) A variable for the tests.
var racyWindow = 2 * time.Second

// foreignActive is an active segment this store does not own — another
// writer's, or one nobody holds at the moment — indexed for reading.
type foreignActive struct {
	id   uint64
	path string
	f    *os.File    // read-only; follows the file through a seal's rename
	fi   os.FileInfo // identity: an id can come back as another file
	scan *segmentScan
	// seenSize is the data file's length at the last look; a different one
	// now means its owner appended. Written under the store's mu.
	seenSize int64
}

type segFile struct {
	path string
	info os.FileInfo
}

type dirListing struct {
	sealed, active map[uint64]segFile
	tmp, sidecars  []string // file names
	maxID          uint64   // highest segment id of any kind
	vanished       bool     // a name was gone again before it could be looked at: the listing is out of date
}

func listSegments(dir string) (dirListing, error) {
	ls := dirListing{sealed: map[uint64]segFile{}, active: map[uint64]segFile{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ls, err
	}
	for _, e := range entries {
		name := e.Name()
		var into map[uint64]segFile
		var suffix string
		switch {
		case strings.HasSuffix(name, activeSuffix+tmpSuffix):
			ls.tmp = append(ls.tmp, name)
			if id, err := parseSegmentID(name, activeSuffix+tmpSuffix); err == nil {
				ls.maxID = max(ls.maxID, id)
			}
			continue
		case strings.HasSuffix(name, activeSuffix+sidecarSuffix):
			ls.sidecars = append(ls.sidecars, name)
			continue
		case strings.HasSuffix(name, activeSuffix):
			into, suffix = ls.active, activeSuffix
		case strings.HasSuffix(name, sealedSuffix):
			into, suffix = ls.sealed, sealedSuffix
		default:
			continue // anything else (.DS_Store, gc.lock, a repair's temporary) is not a segment
		}
		id, err := parseSegmentID(name, suffix)
		if err != nil {
			return ls, err
		}
		info, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) {
			ls.vanished = true // sealed, reaped or renamed since the directory was read
			continue
		}
		if err != nil {
			return ls, err
		}
		into[id] = segFile{path: filepath.Join(dir, name), info: info}
		ls.maxID = max(ls.maxID, id)
	}
	return ls, nil
}

// openForeign indexes the active segment at sf for reading. A file that
// carries a whole footer — a seal that crashed before its rename — is mapped
// as the sealed segment it is, under its active name; its next owner renames
// it. Both results are nil when the file is gone.
func openForeign(id uint64, sf segFile) (*foreignActive, *sealedSegment, error) {
	res, err := recoverSegment(sf.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if res.sealed {
		seg, err := openSealed(sf.path, id)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, nil
		}
		return nil, seg, err
	}
	f, err := os.Open(sf.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return &foreignActive{id: id, path: sf.path, f: f, fi: fi, scan: res.scan(), seenSize: res.fileSize}, nil, nil
}

// poll reads what the segment's owner appended since the view last looked
// and returns the entries to add and how far it read. ok is false when the
// view has to be built again. The view itself is not touched: the caller
// commits entries and position together under the store's lock, or, when the
// refresh fails on something else, neither — a position that moved on without
// its entries would lose them for good.
func (fa *foreignActive) poll() (added []sidecarRec, next segmentScan, size int64, ok bool, err error) {
	var tail []byte
	sf, err := os.Open(fa.path + sidecarSuffix)
	switch {
	case errors.Is(err, fs.ErrNotExist): // no sidecar, or gone with a seal: the data's tail still reads
	case err != nil:
		return nil, segmentScan{}, 0, false, err
	default:
		defer sf.Close()
		st, err := sf.Stat()
		if err != nil {
			return nil, segmentScan{}, 0, false, err
		}
		if st.Size() < fa.scan.sidecarEnd {
			return nil, segmentScan{}, 0, false, nil // started over by a new owner
		}
		if tail, err = readRange(sf, fa.scan.sidecarEnd, st.Size()-fa.scan.sidecarEnd); err != nil {
			return nil, segmentScan{}, 0, false, err
		}
	}
	st, err := fa.f.Stat() // after the sidecar: the data covers whatever it speaks of
	if err != nil {
		return nil, segmentScan{}, 0, false, err
	}
	next = *fa.scan // a copy moves on, not the view's: the index is only read
	added, _, ok, err = next.advance(fa.f, st.Size(), tail)
	return added, next, st.Size(), ok, err
}

// read returns the stored payload of the record loc names, after checking
// that the record there is k's: a view of somebody else's segment is only as
// good as the last look at it.
func (fa *foreignActive) read(k key.Key, loc activeLoc) ([]byte, error) {
	raw, err := fa.readRecord(k, loc)
	if err != nil {
		return nil, err
	}
	return raw[amberpack.RecHeaderSize:], nil
}

func (fa *foreignActive) readRecord(k key.Key, loc activeLoc) ([]byte, error) {
	raw := make([]byte, amberpack.RecHeaderSize+int(loc.slen))
	if _, err := fa.f.ReadAt(raw, loc.off); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errStaleView
		}
		return nil, err
	}
	if !bytes.Equal(raw[1:1+key.Size], k[:]) || binary.BigEndian.Uint32(raw[38:42]) != loc.slen {
		return nil, errStaleView
	}
	return raw, nil
}

// refreshAfterMiss looks at the directory again after a lookup found nothing
// (or, with rebuild, found a foreign index out of step with its data).
// Callers that waited for somebody else's refresh do not repeat it: that
// listing is newer than their miss. looked is false when the view was found
// current without listing anything, so that the caller need not search it a
// second time.
func (s *Store) refreshAfterMiss(rebuild bool) (looked bool, err error) {
	if !rebuild && s.viewIsCurrent() {
		return false, nil
	}
	seq := s.refreshSeq.Load()
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	if !rebuild && s.refreshSeq.Load() != seq {
		return true, nil
	}
	return true, s.refreshLocked(rebuild)
}

// viewIsCurrent reports, for the price of a stat or two, that nothing another
// store did can have changed what this view holds: the directory has not been
// modified since it was listed, so no segment appeared, vanished or was
// replaced, and no active segment this store reads has grown. Listing the
// directory costs a stat per segment; a lookup that finds nothing is common
// enough (any "do you have this?") that it must not pay that every time.
func (s *Store) viewIsCurrent() bool {
	type probe struct {
		f    *os.File
		size int64
	}
	s.mu.RLock()
	mtime, listedAt := s.dirMtime, s.listedAt
	probes := make([]probe, len(s.foreign))
	for i, fa := range s.foreign {
		probes[i] = probe{fa.f, fa.seenSize}
	}
	s.mu.RUnlock()
	if listedAt.Sub(mtime) < racyWindow {
		return false // modified too close to the listing for its time to prove anything
	}
	st, err := os.Stat(s.dir)
	if err != nil || !st.ModTime().Equal(mtime) {
		return false
	}
	for _, p := range probes {
		st, err := p.f.Stat() // a view dropped meanwhile fails here, which is an answer too
		if err != nil || st.Size() != p.size {
			return false
		}
	}
	return true
}

// Refresh brings the store's view of the directory up to date: the segments
// other stores sealed, created or reaped, and what they appended. Lookups do
// it by themselves when they find nothing. A caller that is about to work
// from a snapshot of the view, as gc's advisory mark does, asks for it.
func (s *Store) Refresh() error { return s.refresh() }

// refresh brings the view up to date unconditionally.
func (s *Store) refresh() error {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	return s.refreshLocked(false)
}

// maxRelist bounds how often one refresh lists the directory, when what it
// listed keeps vanishing under it.
const maxRelist = 3

// refreshLocked re-lists the directory and brings the view in line: sealed
// segments that appeared are mapped, those that vanished or were replaced
// are let go, foreign active segments are read further, indexed or dropped.
// With rebuild every foreign index is built again. The caller holds
// refreshMu (or, in Open, is alone).
//
// What this store itself publishes meanwhile — a seal, an adoption, a repair
// — is not in the listing. Such changes bump structEpoch; when it moved, the
// refresh only adds and leaves the dropping to the next one.
func (s *Store) refreshLocked(rebuild bool) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrClosed
	}
	epoch := s.structEpoch
	have := make(map[uint64]*sealedSegment, len(s.sealed))
	for _, g := range s.sealed {
		have[g.id] = g
	}
	haveForeign := make(map[uint64]*foreignActive, len(s.foreign))
	for _, fa := range s.foreign {
		haveForeign[fa.id] = fa
	}
	own, hasOwn := uint64(0), s.active != nil
	if hasOwn {
		own = s.active.id
	}
	s.mu.RUnlock()

	type delta struct {
		fa    *foreignActive
		added []sidecarRec
		scan  segmentScan // how far the poll read: committed with the entries
		size  int64
	}
	var (
		dirInfo       os.FileInfo
		listedAt      time.Time
		ls            dirListing
		opened        []*sealedSegment
		openedForeign []*foreignActive
		deltas        []delta
		keepSealed    map[uint64]bool
		keepForeign   map[uint64]bool
	)
	discard := func() {
		for _, g := range opened {
			g.close()
		}
		for _, fa := range openedForeign {
			fa.f.Close()
		}
		opened, openedForeign, deltas = nil, nil, nil
	}
	for attempt := 1; ; attempt++ {
		keepSealed, keepForeign = map[uint64]bool{}, map[uint64]bool{}
		// The directory's time before its listing: a change that falls
		// between the two then shows as a time this view has not caught up
		// with.
		var err error
		if dirInfo, err = os.Stat(s.dir); err != nil {
			return err
		}
		listedAt = time.Now()
		if ls, err = listSegments(s.dir); err != nil {
			return err
		}
		if s.afterList != nil {
			s.afterList()
		}
		vanished := ls.vanished
		for id, sf := range ls.sealed {
			if g := have[id]; g != nil && os.SameFile(g.fi, sf.info) {
				keepSealed[id] = true
				continue
			}
			seg, err := openSealed(sf.path, id)
			if errors.Is(err, fs.ErrNotExist) {
				vanished = true // reaped between the listing and now
				continue
			}
			if err != nil {
				discard()
				return err
			}
			opened = append(opened, seg)
		}
		for id, sf := range ls.active {
			if hasOwn && id == own {
				continue
			}
			if g := have[id]; g != nil && os.SameFile(g.fi, sf.info) {
				keepSealed[id] = true // a crashed seal, mapped under its active name
				continue
			}
			if fa := haveForeign[id]; fa != nil && !rebuild && os.SameFile(fa.fi, sf.info) {
				added, next, size, ok, err := fa.poll()
				if errors.Is(err, os.ErrClosed) {
					// This store took the segment for its own, or sealed
					// it, under this refresh (dropForeignLocked). The epoch
					// moved with that, so this round drops nothing.
					continue
				}
				if err != nil {
					discard()
					return err
				}
				if ok {
					keepForeign[id] = true
					deltas = append(deltas, delta{fa, added, next, size})
					continue
				}
			}
			fa, seg, err := openForeign(id, sf)
			if err != nil {
				discard()
				return err
			}
			switch {
			case seg != nil:
				opened = append(opened, seg)
			case fa != nil:
				openedForeign = append(openedForeign, fa)
			default:
				vanished = true // sealed or reaped between the listing and now
			}
		}
		// Something listed was gone when it came to be opened, so the
		// listing is out of date, and what the missing segment held may be
		// in one created since: a compaction's copy, a seal's new name. Look
		// again rather than settle for a view with a hole in it.
		if !vanished || attempt == maxRelist {
			break
		}
		discard()
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		discard()
		return ErrClosed
	}
	stable := s.structEpoch == epoch
	var sealed, retire []*sealedSegment
	present := map[uint64]bool{}
	for _, g := range s.sealed {
		switch {
		case keepSealed[g.id]:
			if sf, ok := ls.sealed[g.id]; ok {
				g.path = sf.path // a crashed seal that its adopter has renamed
			}
		case stable:
			retire = append(retire, g)
			continue
		}
		sealed = append(sealed, g)
		present[g.id] = true
	}
	for _, g := range opened {
		if present[g.id] {
			retire = append(retire, g) // this store published it first
			continue
		}
		sealed = append(sealed, g)
		present[g.id] = true
	}
	slices.SortFunc(sealed, func(a, b *sealedSegment) int { return cmp.Compare(a.id, b.id) })

	for _, d := range deltas {
		for _, r := range d.added {
			d.fa.scan.index[r.k] = r.loc()
		}
		d.fa.scan.pos, d.fa.scan.sidecarEnd, d.fa.scan.durable = d.scan.pos, d.scan.sidecarEnd, d.scan.durable
		d.fa.seenSize = d.size
	}
	s.dirMtime, s.listedAt = dirInfo.ModTime(), listedAt
	var foreign, dropped []*foreignActive
	isOwn := func(id uint64) bool { return s.active != nil && s.active.id == id }
	for _, fa := range s.foreign {
		if isOwn(fa.id) || present[fa.id] || !(keepForeign[fa.id] || !stable) {
			dropped = append(dropped, fa)
			continue
		}
		foreign = append(foreign, fa)
		present[fa.id] = true
	}
	for _, fa := range openedForeign {
		if isOwn(fa.id) || present[fa.id] {
			dropped = append(dropped, fa)
			continue
		}
		foreign = append(foreign, fa)
		present[fa.id] = true
	}
	s.sealed, s.foreign = sealed, foreign
	s.mu.Unlock()

	// Readers hold mu for a whole lookup, so none is inside a dropped view.
	for _, fa := range dropped {
		fa.f.Close()
	}
	s.retire(retire)
	s.refreshSeq.Add(1)
	s.refreshes.Add(1)
	return nil
}

// retire unmaps segments that left the view, once no scrub can be inside one.
func (s *Store) retire(segs []*sealedSegment) {
	if len(segs) == 0 {
		return
	}
	s.scrubMu.Lock()
	defer s.scrubMu.Unlock()
	if s.scrubN == 0 {
		for _, g := range segs {
			g.close()
		}
		return
	}
	s.retired = append(s.retired, segs...)
}

// publishSealedLocked adds a segment this store just sealed, adopted or
// repaired to the view. The caller holds mu for writing. A refresh may have
// mapped the same file a moment earlier; that mapping goes.
func (s *Store) publishSealedLocked(seg *sealedSegment) {
	s.structEpoch++
	for i, g := range s.sealed {
		if g.id == seg.id {
			s.sealed[i] = seg
			s.retire([]*sealedSegment{g})
			return
		}
	}
	s.sealed = append(s.sealed, seg)
	slices.SortFunc(s.sealed, func(a, b *sealedSegment) int { return cmp.Compare(a.id, b.id) })
	s.dropForeignLocked(seg.id)
}

// dropForeignLocked forgets the read-only view of a segment this store now
// owns or has sealed. The caller holds mu for writing.
func (s *Store) dropForeignLocked(id uint64) {
	for i, fa := range s.foreign {
		if fa.id == id {
			fa.f.Close()
			s.foreign = slices.Delete(slices.Clone(s.foreign), i, i+1)
			return
		}
	}
}

// removeOrphanSidecars deletes the index of a segment that was sealed or
// wiped: a crash fell between the segment's rename or removal and the
// index's. A segment's data file exists before its sidecar does and outlives
// it, so an index without one is never somebody's work in progress.
func removeOrphanSidecars(dir string, ls dirListing) {
	for _, name := range ls.sidecars {
		id, err := parseSegmentID(name, activeSuffix+sidecarSuffix)
		if err != nil {
			continue
		}
		if _, ok := ls.active[id]; !ok {
			os.Remove(filepath.Join(dir, name))
		}
	}
}

func segmentName(id uint64, suffix string) string {
	return fmt.Sprintf("%016x%s", id, suffix)
}
