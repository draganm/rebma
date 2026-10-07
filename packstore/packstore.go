package packstore

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
	"golang.org/x/sys/unix"
)

// ErrNotFound is returned by Get for a key that is not present in the store.
var ErrNotFound = errors.New("packstore: object not found")

// ErrClosed is returned by operations on a closed store.
var ErrClosed = errors.New("packstore: store closed")

// DefaultSegmentSize is the default rotation threshold: the active segment is
// sealed once it reaches this many bytes.
const DefaultSegmentSize = 2 << 30 // 2 GiB

const (
	sealedSuffix = ".seg"
	activeSuffix = ".seg.active"
)

// Option configures a Store at Open time.
type Option func(*config)

type config struct {
	segmentSize int64
	sync        bool
}

func defaultConfig() config {
	return config{segmentSize: DefaultSegmentSize, sync: true}
}

// WithSegmentSize sets the rotation threshold in bytes. A single oversized
// record may push one segment past it.
func WithSegmentSize(n int64) Option {
	return func(c *config) { c.segmentSize = n }
}

// WithSync controls whether writes are fsynced for crash durability. Default
// is true; disabling it speeds bulk loads and tests.
func WithSync(b bool) Option {
	return func(c *config) { c.sync = b }
}

// activeSegment is the append-only segment this store owns and writes to.
// Other stores on the directory own theirs (active.go).
type activeSegment struct {
	id    uint64
	path  string
	f     *os.File
	size  int64 // accessed only under appendMu
	index map[key.Key]activeLoc
	// sc mirrors index on disk (sidecar.go), so that the next open does not
	// have to scan the data. Nil when it could not be written. Under appendMu.
	sc *sidecarWriter
}

// Store is an on-disk content-addressable store over segment files. It is
// safe for concurrent use, by goroutines and — several stores on one
// directory — by processes (active.go, view.go). Lock ordering: appendMu,
// then refreshMu, then mu, never the reverse. appendMu serializes the write
// path (append, fsync, seal, Close); refreshMu serializes refreshes of the
// view; mu guards sealed/active/foreign/closed for readers.
type Store struct {
	dir  string
	dirF *os.File // holds the shared directory flock; also used for directory fsyncs
	cfg  config

	appendMu sync.Mutex

	// write-barrier grey capture, see barrier.go
	capturing atomic.Bool
	greyMu    sync.Mutex
	grey      map[key.Key]struct{}

	mu      sync.RWMutex
	sealed  []*sealedSegment // ascending id
	active  *activeSegment   // the segment this store owns; nil until its first write
	foreign []*foreignActive // active segments it does not own, indexed for reading
	// structEpoch counts the changes this store itself made to the view, so
	// that a refresh that raced one only adds (view.go). Guarded by mu.
	structEpoch uint64
	// dirMtime is the directory's modification time as of the view's last
	// listing, taken at listedAt; together they let a lookup that finds
	// nothing skip the next listing (viewIsCurrent). Guarded by mu.
	dirMtime, listedAt time.Time
	nextID             uint64 // a floor for new segment ids; under appendMu
	closed             bool
	failed             error // sticky write-path failure; written under appendMu+mu, read under either

	// scrubMu/scrubN/scrubC track in-flight lock-free mmap walks (Verify,
	// ScanIndex, Record): Close/Wipe/Remove wait for scrubN to reach 0 before
	// munmap. A plain sync.WaitGroup does not work here: Remove and Wipe
	// release mu before waiting, so a new scrub can register (Add) while the
	// wait is in progress, and WaitGroup treats a concurrent Add-during-Wait
	// as misuse and panics. The cond var tolerates that: a late scrub cannot
	// reach a segment already detached from the probe list, so the waiter
	// just keeps waiting until scrubN drops back to 0. See beginScrub,
	// endScrub, waitScrubs.
	scrubMu sync.Mutex
	scrubN  int
	retired []*sealedSegment // protected by scrubMu; unmapped when the last scrub ends
	scrubC  *sync.Cond

	writesMu sync.Mutex
	writes   map[*writeToken]time.Time // in-flight Put/WriteBatch/WriteParallel starts

	fsyncs atomic.Int64 // active-segment fsyncs issued, for tests
	// deferred is set while PutVerifiedDeferred has appended records that no
	// fsync has covered yet. A write that promises durability and finds its
	// key already in the active segment checks it.
	deferred atomic.Bool

	// refreshSeq counts refreshes of the view, so that a lookup that waited
	// for one does not repeat it; refreshes counts them for tests.
	refreshMu   sync.Mutex
	refreshSeq  atomic.Uint64
	refreshes   atomic.Int64
	afterList   func()       // test hook: runs when a refresh has listed the directory, before it opens anything
	afterDetach func()       // test hook: runs when Compact or Remove took its victims out of the view, before it unlinks them
	afterFooter func() error // test hook: runs when a seal wrote the footer, before it syncs; an error it returns stands in for the write's

	gate *gate // gc.lock: writers against a sweep, across processes (gate.go)
}

// beginScrub registers a lock-free mmap walk. Call while holding mu.RLock
// after the closed check, so registration is ordered before Close, Wipe and
// Remove detach segments.
func (s *Store) beginScrub() {
	s.scrubMu.Lock()
	s.scrubN++
	s.scrubMu.Unlock()
}

// endScrub deregisters a walk started with beginScrub.
func (s *Store) endScrub() {
	s.scrubMu.Lock()
	s.scrubN--
	if s.scrubN == 0 {
		for _, seg := range s.retired {
			seg.close()
		}
		s.retired = nil
		s.scrubC.Broadcast()
	}
	s.scrubMu.Unlock()
}

// waitScrubs blocks until no scrub is in flight. Unlike WaitGroup.Wait it
// tolerates concurrent registrations: a late scrub cannot reach a segment
// already detached from the probe list, and the waiter just keeps waiting.
func (s *Store) waitScrubs() {
	s.scrubMu.Lock()
	for s.scrubN > 0 {
		s.scrubC.Wait()
	}
	s.scrubMu.Unlock()
}

// Open opens (creating if necessary) a store rooted at dir. Any number of
// stores, in any number of processes, may have a directory open at once.
// Opening locks no segment and modifies none: sealed segments are mmap'd and
// validated, active ones indexed from their sidecars for reading. The store
// takes an active segment of its own at its first write (active.go).
//
// The directory is flocked shared for the store's life. Releases from before
// stores could share a directory take that lock exclusively and assume they
// own the one active segment; this keeps them out, and keeps this store out
// while one of them is in.
func Open(dir string, opts ...Option) (*Store, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("packstore: creating %s: %w", dir, err)
	}
	dirF, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(dirF.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		dirF.Close()
		return nil, fmt.Errorf("packstore: %s is held by an older release, which needs the store to itself: %w", dir, err)
	}
	s := &Store{dir: dir, dirF: dirF, cfg: cfg, nextID: 1, writes: make(map[*writeToken]time.Time)}
	s.scrubC = sync.NewCond(&s.scrubMu)
	if s.gate, err = openGate(dir, s.refresh); err != nil {
		dirF.Close()
		return nil, err
	}
	if ls, err := listSegments(dir); err == nil {
		removeOrphanSidecars(dir, ls)
	}
	if err := s.refreshLocked(true); err != nil {
		s.releaseDir()
		return nil, err
	}
	return s, nil
}

func (s *Store) releaseDir() {
	for _, seg := range s.sealed {
		seg.close()
	}
	for _, fa := range s.foreign {
		fa.f.Close()
	}
	s.gate.close()
	s.dirF.Close() // releases the flock
}

func parseSegmentID(name, suffix string) (uint64, error) {
	hex := strings.TrimSuffix(name, suffix)
	id, err := strconv.ParseUint(hex, 16, 64)
	if err != nil || len(hex) != 16 {
		return 0, fmt.Errorf("%w: bad segment file name %q", ErrCorrupt, name)
	}
	return id, nil
}

// append writes one encoded record to the active segment (creating it if
// needed), publishes it in the active index, optionally fsyncs, and seals the
// segment if it reached the rotation threshold.
func (s *Store) append(k key.Key, rec []byte, syncNow bool) error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	return s.appendLocked(k, rec, syncNow)
}

// appendLocked is append's body. The caller must hold appendMu.
func (s *Store) appendLocked(k key.Key, rec []byte, syncNow bool) error {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if s.failed != nil {
		return s.failed
	}
	if err := s.ensureActiveLocked(); err != nil {
		return err
	}
	a := s.active
	if _, ok := a.index[k]; ok {
		// Lost a Put race for this key; the record is already appended. The
		// winner may have been a deferred put, which has not synced it.
		if syncNow && s.cfg.sync && s.deferred.Load() {
			if err := a.f.Sync(); err != nil {
				s.setFailed(err)
				return err
			}
			a.sc.synced(a.size)
			s.deferred.Store(false)
		}
		return nil
	}
	off := a.size
	if _, err := a.f.WriteAt(rec, off); err != nil {
		return err
	}
	loc := activeLoc{
		off:   off,
		flags: rec[33],
		ulen:  binary.BigEndian.Uint32(rec[34:38]),
		slen:  binary.BigEndian.Uint32(rec[38:42]),
	}
	s.mu.Lock()
	a.index[k] = loc
	s.mu.Unlock()
	a.size = off + int64(len(rec))
	a.sc.entry(k, loc) // only now: an entry never precedes its record

	if syncNow && s.cfg.sync {
		if err := a.f.Sync(); err != nil {
			s.setFailed(err)
			return err
		}
		a.sc.synced(a.size)
		s.deferred.Store(false)
	}
	if a.size >= s.cfg.segmentSize {
		return s.sealActiveLocked()
	}
	return nil
}

// syncActive fsyncs the active segment, if syncing is enabled and one exists.
func (s *Store) syncActive() error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if s.failed != nil {
		return s.failed
	}
	if !s.cfg.sync {
		return nil
	}
	if s.active == nil {
		// Sealing synced whatever a deferred put left behind.
		s.deferred.Store(false)
		return nil
	}
	if err := s.active.f.Sync(); err != nil {
		s.setFailed(err)
		return err
	}
	s.active.sc.synced(s.active.size)
	s.deferred.Store(false)
	s.fsyncs.Add(1)
	return nil
}

// setFailed poisons the write path after an fsync failure: a failed fsync may
// have dropped dirty pages, so later appends could be acknowledged while
// sitting behind a garbage hole that tail-scan recovery would truncate. Reads
// stay available; only new acknowledgments stop. Called under appendMu.
func (s *Store) setFailed(err error) {
	s.mu.Lock()
	if s.failed == nil {
		s.failed = fmt.Errorf("packstore: write path failed: %w", err)
	}
	s.mu.Unlock()
}

// sealActiveLocked seals the active segment: build the footer from the
// in-RAM index (no body re-read), append it, fsync, rename to .seg, fsync the
// directory, and swap in the mmap'd sealed segment. Called under appendMu.
//
// A failure poisons the write path: a mid-seal failure can leave a
// renamed-but-unpublished segment or an un-mmap'd sealed file; reads stay
// correct (the fd is still open), but accepting further writes could append
// past a footer; reopen recovers cleanly. Running out of room for the footer
// does not poison: nothing was renamed yet, so cutting the footer off leaves
// a valid active segment, and a later seal tries again.
func (s *Store) sealActiveLocked() error {
	a := s.active
	if a == nil || len(a.index) == 0 {
		return nil
	}
	entries := make([]indexEntry, 0, len(a.index))
	for k, loc := range a.index {
		entries = append(entries, indexEntry{k: k, off: uint64(loc.off), slen: loc.slen})
	}
	footer, err := buildFooter(a.size, entries)
	if err != nil {
		s.setFailed(err)
		return err
	}
	// The footer is located from EOF, so drop anything a failed WriteAt
	// left past a.size.
	err = a.f.Truncate(a.size)
	if err == nil {
		_, err = a.f.WriteAt(footer, a.size)
	}
	if err == nil && s.afterFooter != nil {
		err = s.afterFooter()
	}
	if err != nil {
		noRoom := errors.Is(err, unix.ENOSPC) || errors.Is(err, unix.EDQUOT)
		if !noRoom || a.f.Truncate(a.size) != nil {
			s.setFailed(err)
		}
		return err
	}
	if err := s.finishSealLocked(a); err != nil {
		s.setFailed(err)
		return err
	}
	return nil
}

// finishSealLocked is sealActiveLocked's part after the footer is written.
func (s *Store) finishSealLocked(a *activeSegment) error {
	if err := a.f.Sync(); err != nil {
		return err
	}
	sealedPath := strings.TrimSuffix(a.path, ".active")
	if err := os.Rename(a.path, sealedPath); err != nil {
		return err
	}
	if err := s.dirF.Sync(); err != nil {
		return err
	}
	// The footer indexes the segment from here on. A crash before the
	// removal leaves an orphan that the next open deletes.
	a.sc.close()
	os.Remove(a.path + sidecarSuffix)
	seg, err := openSealed(sealedPath, a.id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.publishSealedLocked(seg)
	s.active = nil
	s.mu.Unlock()
	// Close the fd only after the swap: in-flight readers hold mu.RLock for
	// their whole pread, so the Lock above drained them, and post-swap
	// readers route to the sealed mmap. A close error fails the triggering
	// Put even though the object is sealed and durable — harmless; a retried
	// Put dedups via Has.
	return a.f.Close()
}

// WriteBatch stores every object the iterator yields, fsyncing once at the
// end (when WithSync is enabled): on return, all yielded objects are durable.
// It is NOT atomic — a crash or iterator error
// can leave a valid prefix stored. In a content-addressed store that prefix
// is harmless: identical re-pushed content deduplicates. Objects repeated
// within the batch, or already present, are written once. When WriteBatch
// returns an error after appending part of the batch, it best-effort fsyncs
// that prefix first, so Has-visible records never stay non-durable.
func (s *Store) WriteBatch(seq iter.Seq2[Object, error]) error {
	w, gerr := s.beginWrite()
	if gerr != nil {
		return gerr
	}
	defer s.endWrite(w)
	seen := make(map[key.Key]struct{})
	appended := false
	fail := func(err error) error {
		if appended {
			s.syncActive() // best-effort; an fsync failure poisons the store
		}
		return err
	}
	for obj, err := range seq {
		if err != nil {
			return fail(err)
		}
		if _, dup := seen[obj.Key]; dup {
			continue
		}
		seen[obj.Key] = struct{}{}
		s.observe(obj.Key)
		has, err := s.hasDurably(obj.Key)
		if err != nil {
			return fail(fmt.Errorf("exists (%s): %w", obj.Key, err))
		}
		if has {
			continue
		}
		rec, _, err := prepare(obj, false)
		if err != nil {
			return fail(err)
		}
		if err := s.append(obj.Key, rec, false); err != nil {
			return fail(err)
		}
		appended = true
	}
	return s.syncActive()
}

// Put stores a single object under k, deduplicating against existing content.
// A dedup hit returns success without fsyncing; if the matching record was
// appended by a still-running batch, its durability rides on that batch's commit.
// Records that PutVerifiedDeferred left unsynced are the exception: a dedup
// hit syncs them first.
func (s *Store) Put(k key.Key, data []byte) error {
	w, gerr := s.beginWrite()
	if gerr != nil {
		return gerr
	}
	defer s.endWrite(w)
	s.mu.RLock()
	failed := s.failed
	s.mu.RUnlock()
	if failed != nil {
		return failed
	}
	s.observe(k)
	has, err := s.hasDurably(k)
	if err != nil {
		return err
	}
	if has {
		// "Durably" trusts this store's own active records, and a deferred
		// put leaves those unsynced.
		if s.deferred.Load() {
			return s.syncActive()
		}
		return nil
	}
	rec, err := amberpack.EncodeRecord(k, data)
	if err != nil {
		return err
	}
	return s.append(k, rec, true)
}

// activeLookupLocked finds k in the active segment this store owns (own) or
// in one it only reads (fa). The caller holds mu.
func (s *Store) activeLookupLocked(k key.Key) (own *activeSegment, fa *foreignActive, loc activeLoc, ok bool) {
	if s.active != nil {
		if loc, ok := s.active.index[k]; ok {
			return s.active, nil, loc, true
		}
	}
	for _, fa := range s.foreign {
		if loc, ok := fa.scan.index[k]; ok {
			return nil, fa, loc, true
		}
	}
	return nil, nil, activeLoc{}, false
}

// lookup runs find and, if it found nothing, once more after a fresh look at
// the directory: another store may have written k since this one last looked.
func lookup[T any](s *Store, k key.Key, find func() (T, error)) (T, error) {
	v, err := find()
	stale := errors.Is(err, errStaleView)
	if stale || errors.Is(err, ErrNotFound) {
		looked, rerr := s.refreshAfterMiss(stale)
		if rerr != nil {
			var zero T
			return zero, rerr
		}
		if looked {
			v, err = find()
		}
	}
	if errors.Is(err, errStaleView) {
		var zero T
		return zero, fmt.Errorf("%w: %s: another writer's active segment does not hold the record its index names", ErrCorrupt, k)
	}
	return v, err
}

// Get returns the bytes stored under k, or ErrNotFound if k is absent. The
// returned slice is caller-owned.
func (s *Store) Get(k key.Key) ([]byte, error) {
	return lookup(s, k, func() ([]byte, error) { return s.get(k) })
}

func (s *Store) get(k key.Key) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	if own, fa, loc, ok := s.activeLookupLocked(k); ok {
		var stored []byte
		if fa != nil {
			var err error
			if stored, err = fa.read(k, loc); err != nil {
				return nil, err
			}
		} else {
			stored = make([]byte, loc.slen)
			if _, err := own.f.ReadAt(stored, loc.off+amberpack.RecHeaderSize); err != nil {
				return nil, err
			}
		}
		return amberpack.DecodePayload(loc.flags, loc.ulen, stored)
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		data, found, err := s.sealed[i].get(k)
		// A corrupt segment fails the read loudly rather than falling back to
		// older copies: masking corruption would hide real damage from scrub.
		if err != nil {
			return nil, err
		}
		if found {
			return data, nil
		}
	}
	return nil, ErrNotFound
}

// GetRecord returns a caller-owned copy of the full on-disk record stored under
// k — its 46-byte header plus the stored (still-compressed) payload, exactly as
// written by amberpack.EncodeRecord — or ErrNotFound if k is absent. This is the
// zero-copy push path: the record is wire-format-identical, so a caller can hand
// it to amberpack.Writer.AddRecord without decompressing and re-encoding. Like
// Get, it does not CRC-check; the receiving Reader validates framing and CRC.
func (s *Store) GetRecord(k key.Key) ([]byte, error) {
	return lookup(s, k, func() ([]byte, error) { return s.getRecord(k) })
}

func (s *Store) getRecord(k key.Key) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	if own, fa, loc, ok := s.activeLookupLocked(k); ok {
		if fa != nil {
			return fa.readRecord(k, loc)
		}
		rec := make([]byte, amberpack.RecHeaderSize+int(loc.slen))
		if _, err := own.f.ReadAt(rec, loc.off); err != nil {
			return nil, err
		}
		return rec, nil
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		rec, found, err := s.sealed[i].getRecord(k)
		if err != nil {
			return nil, err
		}
		if found {
			return rec, nil
		}
	}
	return nil, ErrNotFound
}

// StoredSize returns the stored (post-compression) payload length of the object
// under k and whether k was found, reading only the index — no payload read.
// It sizes objects for byte-balanced push batching against the bytes that
// actually travel.
func (s *Store) StoredSize(k key.Key) (uint64, bool, error) {
	n, ok, err := s.storedSize(k)
	if err == nil && !ok {
		looked, rerr := s.refreshAfterMiss(false)
		if rerr != nil {
			return 0, false, rerr
		}
		if looked {
			n, ok, err = s.storedSize(k)
		}
	}
	return n, ok, err
}

func (s *Store) storedSize(k key.Key) (uint64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, false, ErrClosed
	}
	if _, _, loc, ok := s.activeLookupLocked(k); ok {
		return uint64(loc.slen), true, nil
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		if slen, ok := s.sealed[i].storedSize(k); ok {
			return uint64(slen), true, nil
		}
	}
	return 0, false, nil
}

// locateLocked returns the segment id and record offset where k lives, for
// ordering reads by physical layout. The caller must hold s.mu (read or write).
func (s *Store) locateLocked(k key.Key) (seg, off uint64, ok bool) {
	if own, fa, loc, ok := s.activeLookupLocked(k); ok {
		if fa != nil {
			return fa.id, uint64(loc.off), true
		}
		return own.id, uint64(loc.off), true
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		if o, found := s.sealed[i].locate(k); found {
			return s.sealed[i].id, o, true
		}
	}
	return 0, 0, false
}

// SortByLocation reorders keys in place to follow the store's on-disk layout —
// grouped by segment, ascending offset within a segment — so reading them in
// order is a near-sequential sweep per segment rather than scattered random
// access. Absent keys sort last (their reads surface ErrNotFound later). It is
// a no-op on a closed store.
func (s *Store) SortByLocation(keys []key.Key) {
	type located struct {
		k        key.Key
		seg, off uint64
		ok       bool
	}
	items := make([]located, len(keys))
	s.mu.RLock()
	closed := s.closed
	if !closed {
		for i, k := range keys {
			seg, off, ok := s.locateLocked(k)
			items[i] = located{k, seg, off, ok}
		}
	}
	s.mu.RUnlock()
	if closed {
		return
	}
	slices.SortFunc(items, func(a, b located) int {
		if a.ok != b.ok {
			if a.ok { // present keys before absent ones
				return -1
			}
			return 1
		}
		if c := cmp.Compare(a.seg, b.seg); c != 0 {
			return c
		}
		return cmp.Compare(a.off, b.off)
	})
	for i := range items {
		keys[i] = items[i].k
	}
}

// Has reports whether an object is stored under k.
func (s *Store) Has(k key.Key) (bool, error) {
	has, err := s.hasLocal(k)
	if err == nil && !has {
		looked, rerr := s.refreshAfterMiss(false)
		if rerr != nil {
			return false, rerr
		}
		if looked {
			has, err = s.hasLocal(k)
		}
	}
	return has, err
}

// hasDurably is the write path's duplicate check: whether the view holds a
// copy of k that a write may rely on instead of making one. It does not look
// at the directory again. A duplicate it fails to see — written by another
// store a moment ago — costs a redundant record, which compaction folds;
// listing the directory for every new object would cost every ingest dearly.
func (s *Store) hasDurably(k key.Key) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, ErrClosed
	}
	if s.reliableActiveLocked(k) {
		return true, nil
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		if s.sealed[i].has(k) {
			return true, nil
		}
	}
	return false, nil
}

// reliableActiveLocked reports whether an active segment holds a copy of k
// that a write, or a compaction, may rely on instead of making its own. A
// record in another store's segment counts only as far as its owner has
// synced it, when this store syncs: this store's fsync covers its own segment
// alone, and acknowledging a write against bytes somebody else has yet to
// sync would promise what nobody has delivered. The price is a second copy
// now and then. The caller holds mu.
func (s *Store) reliableActiveLocked(k key.Key) bool {
	if s.active != nil {
		if _, ok := s.active.index[k]; ok {
			return true // this store's own fsync covers it
		}
	}
	for _, fa := range s.foreign {
		if loc, ok := fa.scan.index[k]; ok && (!s.cfg.sync || loc.off+amberpack.RecHeaderSize+int64(loc.slen) <= fa.scan.durable) {
			return true
		}
	}
	return false
}

// hasLocal is Has without the second look: what this store's view holds,
// synced or not. Reads ask it; writes ask hasDurably.
func (s *Store) hasLocal(k key.Key) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, ErrClosed
	}
	if _, _, _, ok := s.activeLookupLocked(k); ok {
		return true, nil
	}
	for i := len(s.sealed) - 1; i >= 0; i-- {
		if s.sealed[i].has(k) {
			return true, nil
		}
	}
	return false, nil
}

// Wipe deletes every object: every segment, sealed or active, is closed and
// its file removed, leaving an empty, still-open store (the store-wipe
// operation). It refuses, deleting nothing, while another store owns an
// active segment: that writer would go on appending to a file that is gone.
// Readers are drained via the write lock before segments are detached;
// in-flight Verify walks are waited out before unmapping, exactly like Close.
func (s *Store) Wipe() error {
	end, err := s.gate.beginExclusive(context.Background()) // other stores' writers wait, and look again afterwards
	if err != nil {
		return err
	}
	defer end()
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.refreshMu.Lock() // held throughout: the view must not grow behind the locks taken below
	defer s.refreshMu.Unlock()
	if err := s.refreshLocked(false); err != nil {
		return err
	}
	s.mu.RLock()
	foreign := slices.Clone(s.foreign)
	s.mu.RUnlock()
	held, err := s.lockForeign(foreign)
	if err != nil {
		return err
	}
	defer func() {
		for _, f := range held {
			f.Close()
		}
	}()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	active := s.active
	sealed := s.sealed
	s.active, s.sealed, s.foreign = nil, nil, nil
	s.structEpoch++
	// A sticky write-path failure (setFailed after a bad fsync) poisons the
	// data the fsync may have torn — data the wipe is about to destroy. The
	// reset clears it: the reopened-empty store must accept writes again.
	s.failed = nil
	// From here readers see an empty store; a Verify that started after this
	// unlock walks an empty snapshot. Pre-existing scrubs still hold the old
	// mmaps, so wait before unmapping (see Close). waitScrubs tolerates a
	// scrub registering after this unlock (unlike WaitGroup.Wait): it can
	// only ever reach the empty snapshot above, never the detached segments.
	s.mu.Unlock()
	s.waitScrubs()

	var firstErr error
	note := func(err error) {
		if err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	if active != nil {
		active.sc.close()
		note(active.f.Close())
		note(os.Remove(active.path))
		note(os.Remove(active.path + sidecarSuffix))
	}
	for _, fa := range foreign {
		fa.f.Close()
		note(os.Remove(fa.path))
		note(os.Remove(fa.path + sidecarSuffix))
	}
	for _, seg := range sealed {
		note(seg.close())
		note(os.Remove(seg.path))
	}
	if s.cfg.sync {
		note(s.dirF.Sync())
	}
	return firstErr
}

// Close fsyncs and closes the active segment this store owns (without sealing
// it, so that the next writer goes on filling it), unmaps all sealed
// segments, and releases the segment and directory locks.
func (s *Store) Close() error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	var firstErr error
	if s.active != nil {
		// Synced whatever the sync option says, so that a store closed
		// cleanly always reopens from its sidecar alone.
		if err := s.active.f.Sync(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		} else {
			s.active.sc.synced(s.active.size)
		}
		s.active.sc.close()
		if err := s.active.f.Close(); err != nil && firstErr == nil { // releases the segment
			firstErr = err
		}
		s.active = nil
	}
	foreign := s.foreign
	s.foreign = nil
	// Wait for in-flight Verify walks before unmapping: the scrub reads the
	// mmaps lock-free, and munmap under it is an uncatchable SIGSEGV. New
	// scrubs cannot start (closed is set; Verify checks it under mu.RLock).
	// Callers wanting a faster Close cancel Verify's context first.
	s.mu.Unlock()
	s.waitScrubs()

	for _, fa := range foreign {
		fa.f.Close()
	}
	for _, seg := range s.sealed {
		if err := seg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.sealed = nil
	if err := s.gate.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.dirF.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
