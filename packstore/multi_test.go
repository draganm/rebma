package packstore

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/draganm/rebma/key"
	"golang.org/x/sys/unix"
)

// Two stores opened on one directory stand in for two processes: every lock
// here is a flock, which belongs to the open file, not to the process.

func activeFiles(t *testing.T, dir string) []string {
	t.Helper()
	found, err := filepath.Glob(filepath.Join(dir, "*"+activeSuffix))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(found)
	return found
}

func sealedFiles(t *testing.T, dir string) []string {
	t.Helper()
	found, err := filepath.Glob(filepath.Join(dir, "*"+sealedSuffix))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(found)
	return found
}

func putAll(t *testing.T, s *Store, objs []Object) {
	t.Helper()
	for _, o := range objs {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTwoStoresOpenOneDirectory(t *testing.T) {
	dir := t.TempDir()
	a, b := openStore(t, dir, WithSync(false)), openStore(t, dir, WithSync(false))
	objs := testObjects(t, 4)
	putAll(t, a, objs[:2])
	wantObjects(t, b, objs[:2]) // b opened before the writes: a miss, a refresh, a hit
	putAll(t, b, objs[2:])
	wantObjects(t, a, objs[2:])
}

func TestManyActiveSegmentsOpen(t *testing.T) {
	dir := t.TempDir()
	objs := testObjects(t, 6)
	for i, name := range []string{"0000000000000001", "0000000000000002", "0000000000000007"} {
		body, _ := buildBody(t, objs[2*i:2*i+2])
		if err := os.WriteFile(filepath.Join(dir, name+activeSuffix), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	wantObjects(t, openStore(t, dir), objs)
}

// A release from before this change takes the directory lock exclusively and
// assumes it owns the one active segment. It must not get in while a store
// is open, and a store must not open while it is in.
func TestOldExclusiveDirectoryLockIsRefused(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	old, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err := unix.Flock(int(old.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
		t.Fatal("the old exclusive directory lock succeeded while a store is open")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(old.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), "older release") {
		t.Fatalf("Open while an older release holds the directory = %v", err)
	}
}

func TestSecondWriterCreatesItsOwnSegment(t *testing.T) {
	dir := t.TempDir()
	a, b := openStore(t, dir, WithSync(false)), openStore(t, dir, WithSync(false))
	objs := testObjects(t, 2)
	putAll(t, a, objs[:1])
	putAll(t, b, objs[1:])
	if got := activeFiles(t, dir); len(got) != 2 {
		t.Fatalf("active segments = %v, want one per writer", got)
	}
	if a.active.id == b.active.id {
		t.Fatalf("both writers own segment %x", a.active.id)
	}
}

func TestWriterAdoptsTheLargestUnlockedSegment(t *testing.T) {
	dir := t.TempDir()
	a, b := openStore(t, dir, WithSync(false)), openStore(t, dir, WithSync(false))
	objs := testObjects(t, 7)
	putAll(t, a, objs[:1])
	putAll(t, b, objs[1:6])
	larger := b.active.id
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	c := openStore(t, dir, WithSync(false))
	putAll(t, c, objs[6:])
	if c.active.id != larger {
		t.Fatalf("the new writer owns segment %x, want the larger idle one, %x", c.active.id, larger)
	}
	if got := activeFiles(t, dir); len(got) != 2 {
		t.Fatalf("active segments = %v, want the two that existed", got)
	}
	wantObjects(t, c, objs)
}

func TestSerialWritersFillOneSegment(t *testing.T) {
	dir := t.TempDir()
	objs := testObjects(t, 6)
	for round := range 3 {
		s, err := Open(dir, WithSync(false))
		if err != nil {
			t.Fatal(err)
		}
		putAll(t, s, objs[2*round:2*round+2])
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if a, sl := activeFiles(t, dir), sealedFiles(t, dir); len(a) != 1 || len(sl) != 0 {
		t.Fatalf("active %v, sealed %v; want one active segment that every writer reused", a, sl)
	}
	wantObjects(t, openStore(t, dir), objs)
}

func TestConcurrentCreatorsGetDistinctIDs(t *testing.T) {
	dir := t.TempDir()
	const n = 8
	objs := testObjects(t, n)
	stores := make([]*Store, n)
	for i := range stores {
		stores[i] = openStore(t, dir, WithSync(false))
	}
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { errs[i] = stores[i].Put(objs[i].Key, objs[i].Data) })
	}
	wg.Wait()
	ids := map[uint64]bool{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
		ids[stores[i].active.id] = true
	}
	if len(ids) != n || len(activeFiles(t, dir)) != n {
		t.Fatalf("%d distinct ids, files %v; want %d of each", len(ids), activeFiles(t, dir), n)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(left) != 0 {
		t.Fatalf("temporary files left behind: %v", left)
	}
	wantObjects(t, openStore(t, dir), objs)
}

// crashSeal appends a complete footer to the directory's only active
// segment, as a seal that crashed before its rename leaves it.
func crashSeal(t *testing.T, dir string) {
	t.Helper()
	data := onlyActive(t, dir)
	res, err := scanActive(data)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]indexEntry, 0, len(res.index))
	for k, loc := range res.index {
		entries = append(entries, indexEntry{k: k, off: uint64(loc.off), slen: loc.slen})
	}
	footer, err := buildFooter(res.size, entries)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(data, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(footer); err != nil {
		t.Fatal(err)
	}
}

func TestAdopterCompletesACrashedSeal(t *testing.T) {
	dir := t.TempDir()
	objs := testObjects(t, 5)
	w, err := Open(dir, WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	putAll(t, w, objs[:4])
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	crashSeal(t, dir)

	reader := openStore(t, dir) // before anybody has finished the seal
	wantObjects(t, reader, objs[:4])
	if got := sealedFiles(t, dir); len(got) != 0 {
		t.Fatalf("a reader finished the seal: %v", got)
	}

	w2 := openStore(t, dir, WithSync(false))
	putAll(t, w2, objs[4:])
	if s, a := sealedFiles(t, dir), activeFiles(t, dir); len(s) != 1 || len(a) != 1 {
		t.Fatalf("sealed %v, active %v; want the crashed segment sealed and one new active", s, a)
	}
	wantObjects(t, reader, objs)
	wantObjects(t, w2, objs)
}

func TestReaderSeesWhatAnotherWroteAfterItOpened(t *testing.T) {
	dir := t.TempDir()
	reader := openStore(t, dir)
	objs := testObjects(t, 3)

	sealer := openStore(t, dir, WithSync(false), WithSegmentSize(1)) // every record seals its segment
	putAll(t, sealer, objs[:2])
	wantObjects(t, reader, objs[:2]) // in segments sealed after the reader opened

	w := openStore(t, dir, WithSync(false))
	putAll(t, w, objs[2:])
	wantObjects(t, reader, objs[2:]) // in another writer's active segment
}

func TestReaderHoldsNoLock(t *testing.T) {
	dir := t.TempDir()
	objs := testObjects(t, 2)
	w, err := Open(dir, WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	putAll(t, w, objs[:1])
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reader := openStore(t, dir) // say, a pager left open
	wantObjects(t, reader, objs[:1])

	w2 := openStore(t, dir, WithSync(false))
	putAll(t, w2, objs[1:])
	if got := activeFiles(t, dir); len(got) != 1 {
		t.Fatalf("active segments = %v: the reader kept the writer from adopting the idle one", got)
	}
	wantObjects(t, reader, objs)
}

func TestLongLivedReaderSurvivesSealAndCompaction(t *testing.T) {
	s, objs := compactStore(t) // objs[0..1] and objs[2..3] sealed, objs[4] active
	reader := openStore(t, s.dir)
	wantObjects(t, reader, objs)

	if _, err := s.Compact(liveSet(objs, 0, 2, 4), CompactOpts{MinDeadRatio: 0.1}); err != nil {
		t.Fatal(err)
	}
	// The two half-dead segments are gone from disk. The reader's mappings
	// of them stay valid, and the survivors are readable either way.
	wantObjects(t, reader, []Object{objs[0], objs[2], objs[4]})

	data := incompressible(4 << 10)
	data[0] = 0xee
	fresh := blobObj(t, data)
	putAll(t, s, []Object{fresh})
	wantObjects(t, reader, []Object{fresh}) // a miss: the reader looks again

	reader.mu.RLock()
	defer reader.mu.RUnlock()
	for _, g := range reader.sealed {
		if _, err := os.Stat(g.path); err != nil {
			t.Errorf("the reader still lists segment %x, which is gone: %v", g.id, err)
		}
	}
}

func TestDedupDoesNotRefresh(t *testing.T) {
	s := openStore(t, t.TempDir(), WithSync(false))
	objs := testObjects(t, 61)
	putAll(t, s, objs[60:]) // a store's first write span looks at the directory once: it may have opened during a sweep (gate.go)
	before := s.refreshes.Load()
	if err := s.WriteBatch(objSeq(objs[:50], -1)); err != nil {
		t.Fatal(err)
	}
	putAll(t, s, objs[50:60])
	if got := s.refreshes.Load(); got != before {
		t.Fatalf("writing new objects listed the directory %d times; the duplicate check must not", got-before)
	}
}

func TestMissingRefreshesOnce(t *testing.T) {
	s := openStore(t, t.TempDir(), WithSync(false))
	objs := testObjects(t, 100)
	keys := make([]key.Key, len(objs))
	for i, o := range objs {
		keys[i] = o.Key
	}
	before := s.refreshes.Load()
	missing, err := s.Missing(keys)
	if err != nil || len(missing) != len(keys) {
		t.Fatalf("Missing = %d keys, %v; want all %d", len(missing), err, len(keys))
	}
	if got := s.refreshes.Load() - before; got != 1 {
		t.Fatalf("Missing listed the directory %d times, want once", got)
	}
}

// A view of somebody else's active segment is checked on every read: if the
// record is not the one the entry promised, the read fails instead of
// returning another object's bytes.
func TestForeignReadNeverReturnsTheWrongRecord(t *testing.T) {
	dir := t.TempDir()
	w := openStore(t, dir)
	o := testObjects(t, 1)[0]
	putAll(t, w, []Object{o})
	reader := openStore(t, dir)
	wantObjects(t, reader, []Object{o})

	flipByte(t, onlyActive(t, dir), int64(len(magicHeader))+1+5) // a byte of the record's key
	if got, err := reader.Get(o.Key); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get = %d bytes, %v; want ErrCorrupt", len(got), err)
	}
}

func TestWipeRefusesWhileAnotherStoreOwnsASegment(t *testing.T) {
	dir := t.TempDir()
	a, b := openStore(t, dir, WithSync(false)), openStore(t, dir, WithSync(false))
	objs := testObjects(t, 2)
	putAll(t, a, objs[:1])
	putAll(t, b, objs[1:])
	if err := a.Wipe(); err == nil {
		t.Fatal("Wipe deleted a segment that another store is writing to")
	}
	wantObjects(t, a, objs)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Wipe(); err != nil {
		t.Fatal(err)
	}
	if left := append(activeFiles(t, dir), sealedFiles(t, dir)...); len(left) != 0 {
		t.Fatalf("Wipe left %v", left)
	}
	if has, err := a.Has(objs[1].Key); err != nil || has {
		t.Fatalf("Has after Wipe = %v, %v", has, err)
	}
}

func TestStaleTemporarySegmentIsRemoved(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "00000000000000ff"+activeSuffix+".tmp")
	if err := os.WriteFile(stale, magicHeader, 0o644); err != nil {
		t.Fatal(err)
	}
	s := openStore(t, dir, WithSync(false))
	putAll(t, s, testObjects(t, 1))
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("the temporary file of a crashed creation is still there: %v", err)
	}
}

// zeroRacyWindow lets the tests below reach the fast path at once: a fresh
// test directory was modified a moment ago, which the default window, meant
// for filesystems with coarse timestamps, does not trust yet.
func zeroRacyWindow(t *testing.T) {
	t.Helper()
	old := racyWindow
	racyWindow = 0
	t.Cleanup(func() { racyWindow = old })
}

// A lookup that finds nothing must not list the directory when nothing in it
// can have changed: listing costs a stat per segment, a thousand times a miss.
func TestMissesDoNotListAnUnchangedDirectory(t *testing.T) {
	zeroRacyWindow(t)
	dir := t.TempDir()
	w, err := Open(dir, WithSync(false), WithSegmentSize(1))
	if err != nil {
		t.Fatal(err)
	}
	objs := testObjects(t, 40)
	putAll(t, w, objs[:3])
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	s := openStore(t, dir)
	before := s.refreshes.Load()
	for _, o := range objs[3:] {
		if has, err := s.Has(o.Key); err != nil || has {
			t.Fatalf("Has = %v, %v", has, err)
		}
		if _, err := s.Get(o.Key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get = %v", err)
		}
	}
	if got := s.refreshes.Load() - before; got != 0 {
		t.Fatalf("%d lookups that found nothing listed an unchanged directory %d times", 2*len(objs[3:]), got)
	}
}

func TestFastPathStillSeesOtherStoresWrites(t *testing.T) {
	zeroRacyWindow(t)
	dir := t.TempDir()
	reader := openStore(t, dir)
	objs := testObjects(t, 5)

	w := openStore(t, dir, WithSync(false))
	putAll(t, w, objs[:1])
	wantObjects(t, reader, objs[:1]) // a new active segment: the directory changed
	putAll(t, w, objs[1:2])
	wantObjects(t, reader, objs[1:2]) // the same segment grew: only its size changed

	sealer := openStore(t, dir, WithSync(false), WithSegmentSize(1))
	putAll(t, sealer, objs[2:3])
	wantObjects(t, reader, objs[2:3]) // a segment created and sealed in one go

	before := reader.refreshes.Load()
	if has, err := reader.Has(objs[4].Key); err != nil || has {
		t.Fatalf("Has = %v, %v", has, err)
	}
	if got := reader.refreshes.Load() - before; got != 0 {
		t.Fatal("a miss listed a directory that had not changed")
	}
	putAll(t, w, objs[3:4])
	wantObjects(t, reader, objs[3:4]) // and the fast path did not stick
}
