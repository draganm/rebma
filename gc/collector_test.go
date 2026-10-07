package gc

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
	"github.com/draganm/rebma/reference"
	"github.com/draganm/rebma/refstore"
)

// testStore is an open packstore+refstore pair in one temp dir.
type testStore struct {
	dir     string
	objects *packstore.Store
	refs    *refstore.Store
}

func newTestStore(t *testing.T, segSize int64) *testStore {
	t.Helper()
	dir := t.TempDir()
	objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSegmentSize(segSize))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { objects.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	return &testStore{dir: dir, objects: objects, refs: refs}
}

func (ts *testStore) openCollector(t *testing.T, opts Options) *Collector {
	t.Helper()
	c, err := Open(filepath.Join(ts.dir, "closures"), ts.objects, ts.refs, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// storeTree stores a FileNode root over n distinct incompressible 256-byte
// blobs derived from seed and returns the root and every key. Incompressible
// payloads keep on-disk sizes predictable so small segment sizes actually
// rotate.
func storeTree(t *testing.T, st *packstore.Store, seed string, n int) (key.Key, []key.Key) {
	t.Helper()
	base := uint64(crc32.ChecksumIEEE([]byte(seed)))
	var children []key.Key
	var all []key.Key
	for i := 0; i < n; i++ {
		rng := rand.New(rand.NewPCG(base, uint64(i)))
		data := make([]byte, 256)
		for j := range data {
			data[j] = byte(rng.UintN(256))
		}
		o, err := fstree.EncodeBlob(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
		children = append(children, o.Key)
		all = append(all, o.Key)
	}
	rootObj, err := fstree.EncodeFileNode(children)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(rootObj.Key, rootObj.Bytes); err != nil {
		t.Fatal(err)
	}
	all = append(all, rootObj.Key)
	return rootObj.Key, all
}

// putTestRef writes a reference through the collector exactly as a
// CLI/daemon PUT does.
func putTestRef(t *testing.T, c *Collector, refs *refstore.Store, name string, root key.Key) {
	t.Helper()
	rec := reference.Reference{Name: name, Key: root[:], CreatedAt: time.Now().UnixNano()}
	raw, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var old *key.Key
	if prev, err := refs.Get(name); err == nil {
		prevRef, err := reference.Decode(prev)
		if err != nil {
			t.Fatal(err)
		}
		k, err := key.Parse(prevRef.Key)
		if err != nil {
			t.Fatal(err)
		}
		old = &k
	} else if !errors.Is(err, refstore.ErrNotFound) {
		t.Fatal(err)
	}
	commit, abort, err := c.PrepareRef(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := refs.Put(name, raw); err != nil {
		abort()
		t.Fatal(err)
	}
	commit()
	if old != nil {
		if err := c.ReleaseRef(*old); err != nil {
			t.Fatal(err)
		}
	}
}

func rmTestRef(t *testing.T, c *Collector, refs *refstore.Store, name string, root key.Key) {
	t.Helper()
	if err := refs.Delete(name); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseRef(root); err != nil {
		t.Fatal(err)
	}
}

// backdatePacks pushes every sealed pack's mtime behind any grace period.
func backdatePacks(t *testing.T, ts *testStore) {
	t.Helper()
	dir := filepath.Join(ts.dir, "packstore")
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".seg") {
			if err := os.Chtimes(filepath.Join(dir, e.Name()), old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestPrepareRefMissingObjectFails(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{})
	// A root whose child blob was never stored: the completeness walk is
	// the caller's 404.
	blob, err := fstree.EncodeBlob([]byte("never stored"))
	if err != nil {
		t.Fatal(err)
	}
	rootObj, err := fstree.EncodeFileNode([]key.Key{blob.Key})
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.objects.Put(rootObj.Key, rootObj.Bytes); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.PrepareRef(rootObj.Key); err == nil {
		t.Fatal("PrepareRef accepted a root with a missing object")
	}
}

func TestOpenSweepsStaleClosureState(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	dir := filepath.Join(ts.dir, "closures")
	if err := os.MkdirAll(filepath.Join(dir, "tmp"), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, "deadbeef.tails")
	if err := os.WriteFile(stale, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	ts.openCollector(t, Options{})
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 {
		t.Fatalf("stale closure state survived Open: %v", ents)
	}
}

func TestStatusScoresAndCounts(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	c := ts.openCollector(t, Options{})
	rootA, _ := storeTree(t, ts.objects, "a", 30)
	rootB, keysB := storeTree(t, ts.objects, "b", 30)
	putTestRef(t, c, ts.refs, "a", rootA)
	putTestRef(t, c, ts.refs, "b", rootB)

	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Refs != 2 {
		t.Errorf("Refs = %d, want 2", st.Refs)
	}
	if st.Marked == 0 {
		t.Error("Marked = 0")
	}
	if st.GarbageBytes != 0 {
		t.Errorf("GarbageBytes = %d before any delete", st.GarbageBytes)
	}

	rmTestRef(t, c, ts.refs, "b", rootB)
	st, err = c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Refs != 1 {
		t.Errorf("Refs = %d after delete, want 1", st.Refs)
	}
	if st.GarbageBytes == 0 {
		t.Error("GarbageBytes = 0 after deleting a ref with unique data")
	}
	_ = keysB
}

func TestWhy(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{})
	rootA, keysA := storeTree(t, ts.objects, "a", 4)
	rootB, _ := storeTree(t, ts.objects, "b", 4)
	putTestRef(t, c, ts.refs, "va", rootA)
	putTestRef(t, c, ts.refs, "vb", rootB)

	names, err := c.Why(keysA[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "va" {
		t.Fatalf("Why = %v, want [va]", names)
	}
	rmTestRef(t, c, ts.refs, "va", rootA)
	names, err = c.Why(keysA[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Fatalf("Why after rm = %v, want none", names)
	}
}

func TestUnreachableFromSeparatesReachedFromUnreached(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{})
	rootA, keysA := storeTree(t, ts.objects, "a", 4)
	rootB, keysB := storeTree(t, ts.objects, "b", 4)
	ctx := context.Background()

	candidates := slices.Concat(keysA, keysB)
	dead, err := c.UnreachableFrom(ctx, []key.Key{rootA}, candidates)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keysA {
		if slices.Contains(dead, k) {
			t.Fatalf("a-key %s reported unreachable from root a", k)
		}
	}
	for _, k := range keysB {
		if !slices.Contains(dead, k) {
			t.Fatalf("b-key %s reported reachable from root a", k)
		}
	}

	// Both roots together reach everything.
	dead, err = c.UnreachableFrom(ctx, []key.Key{rootA, rootB}, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 0 {
		t.Fatalf("dead = %v, want none", dead)
	}

	// An empty root set reaches nothing, so every candidate comes back, in order.
	dead, err = c.UnreachableFrom(ctx, nil, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dead, candidates) {
		t.Fatalf("with no roots: dead = %v, want %v", dead, candidates)
	}
}

func TestUnreachableFromTakesNoReferencesIntoAccount(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{})
	root, keys := storeTree(t, ts.objects, "a", 4)
	putTestRef(t, c, ts.refs, "held", root)

	// A reference protects the tree from a real cycle, but it is not a root
	// the caller passed, so it does not enter this answer.
	dead, err := c.UnreachableFrom(context.Background(), nil, keys)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dead, keys) {
		t.Fatalf("a reference must not act as a root here: dead = %v, want %v", dead, keys)
	}
}

// TestUnreachableFromDoesNotWaitForCycle pins UnreachableFrom off cycleMu,
// where Status is pinned to it. The slot is held here as it is by a cycle
// that waits for the caller's own write span: an UnreachableFrom that waited
// for the slot in turn would never return, and this test would hang.
func TestUnreachableFromDoesNotWaitForCycle(t *testing.T) {
	ts := newTestStore(t, 1<<20)
	c := ts.openCollector(t, Options{})
	root, keys := storeTree(t, ts.objects, "a", 4)
	c.cycleMu.Lock()
	defer c.cycleMu.Unlock()
	dead, err := c.UnreachableFrom(context.Background(), []key.Key{root}, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 0 {
		t.Fatalf("dead = %v, want none", dead)
	}
}

// Wipe must not reset the stores while a cycle is still marking.
func TestWipeWaitsForRunningCycleBeforeReset(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	c := ts.openCollector(t, Options{})
	root, _ := storeTree(t, ts.objects, "a", 10)
	putTestRef(t, c, ts.refs, "a", root)

	entered := make(chan struct{})
	release := make(chan struct{})
	c.mu.Lock()
	c.midMark = func() { close(entered); <-release }
	c.mu.Unlock()
	cycleDone := make(chan struct{})
	go func() {
		defer close(cycleDone)
		c.Run(context.Background(), -1) //nolint:errcheck // cancelled by Wipe
	}()
	<-entered

	resetCalled := make(chan struct{})
	wipeDone := make(chan error, 1)
	go func() {
		wipeDone <- c.Wipe(func() error { close(resetCalled); return nil })
	}()
	select {
	case <-resetCalled:
		t.Fatal("reset ran before the cycle finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-wipeDone; err != nil {
		t.Fatalf("wipe: %v", err)
	}
	<-resetCalled
	<-cycleDone
}

// TestBeginWriteDedupHitSurvivesCycle re-puts an object that lives only in
// a condemned pack while a cycle sits between its mark and its sweep: the
// dedup hit writes nothing, so only the write barrier's grey set (mark) and
// the BeginWrite gate (sweep) keep the record from vanishing with its pack.
func TestBeginWriteDedupHitSurvivesCycle(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	c := ts.openCollector(t, Options{})
	rootA, _ := storeTree(t, ts.objects, "live", 30)
	putTestRef(t, c, ts.refs, "live", rootA)
	_, keysB := storeTree(t, ts.objects, "dead", 30) // never referenced
	deadKey := keysB[0]
	want, err := ts.objects.Get(deadKey)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.midMark = func() {
		done := c.BeginWrite()
		defer done()
		if err := ts.objects.Put(deadKey, want); err != nil {
			t.Errorf("dedup-hit put during cycle: %v", err)
		}
	}
	c.mu.Unlock()
	backdatePacks(t, ts)
	stats, err := c.Run(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Reaped) == 0 {
		t.Fatal("cycle reaped nothing; the dedup-hit race was never exercised")
	}
	got, err := ts.objects.Get(deadKey)
	if err != nil {
		t.Fatalf("dedup-hit object lost to the sweep: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Error("dedup-hit object corrupted by the sweep")
	}
}

// TestStatusSerializesWithCycle pins Status to cycleMu: the advisory mark
// probes sealed footers without scrub registration, so it must never
// overlap a sweep's munmap. Status called mid-cycle returns only after the
// cycle does.
func TestStatusSerializesWithCycle(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	c := ts.openCollector(t, Options{})
	root, _ := storeTree(t, ts.objects, "a", 10)
	putTestRef(t, c, ts.refs, "a", root)

	entered := make(chan struct{})
	release := make(chan struct{})
	c.mu.Lock()
	c.midMark = func() { close(entered); <-release }
	c.mu.Unlock()
	cycleDone := make(chan struct{})
	go func() {
		defer close(cycleDone)
		if _, err := c.Run(context.Background(), -1); err != nil {
			t.Errorf("cycle: %v", err)
		}
	}()
	<-entered
	statusDone := make(chan struct{})
	go func() {
		defer close(statusDone)
		if _, err := c.Status(context.Background()); err != nil {
			t.Errorf("status: %v", err)
		}
	}()
	select {
	case <-statusDone:
		t.Error("Status returned while a cycle was mid-mark")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-cycleDone
	<-statusDone
}
