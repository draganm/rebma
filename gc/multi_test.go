package gc

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
	"github.com/draganm/rebma/refstore"
)

// Two store/refstore/collector triples on one directory stand in for two
// processes: every lock between them is a file lock or the reference
// database's own.

const (
	multiWait  = 20 * time.Second       // a wait that must end
	multiQuiet = 150 * time.Millisecond // how long "still waiting" is watched
)

func async(f func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	return done
}

func stillWaiting(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("%s did not wait", what)
	case <-time.After(multiQuiet):
	}
}

func finishes(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(multiWait):
		t.Fatalf("%s is still waiting", what)
	}
}

// openTestStoreAt opens another store pair on an existing test directory.
func openTestStoreAt(t *testing.T, dir string, segSize int64) *testStore {
	t.Helper()
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

func activeSegments(t *testing.T, ts *testStore) []string {
	t.Helper()
	found, err := filepath.Glob(filepath.Join(ts.dir, "packstore", "*.seg.active"))
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func TestCycleWaitsForAForeignWriteSpan(t *testing.T) {
	a := newTestStore(t, 4<<10)
	b := openTestStoreAt(t, a.dir, 4<<10)
	cb := b.openCollector(t, Options{Grace: time.Hour})

	endSpan, err := a.objects.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	var runErr error
	ran := async(func() { _, runErr = cb.Run(context.Background(), 0) })
	stillWaiting(t, ran, "a cycle, while another store is in a write span,")
	endSpan()
	finishes(t, ran, "the cycle, after the write span ended,")
	if runErr != nil {
		t.Fatal(runErr)
	}
}

// A reference put in another store must not land between a cycle's snapshot
// of the roots and its sweep: the cycle would not know the new root, and the
// barrier that protects such a put lives in the cycle's own process.
func TestForeignPrepareRefWaitsForACycle(t *testing.T) {
	a := newTestStore(t, 4<<10)
	b := openTestStoreAt(t, a.dir, 4<<10)
	ca := a.openCollector(t, Options{Grace: time.Hour})
	cb := b.openCollector(t, Options{Grace: time.Hour})
	root, _ := storeTree(t, a.objects, "tree", 8)

	reached, hold := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(hold) }) }
	defer release() // also when an assertion fails: the collector's Close waits for the cycle
	cb.mu.Lock()
	cb.midMark = func() {
		close(reached)
		<-hold
	}
	cb.mu.Unlock()
	var runErr error
	ran := async(func() { _, runErr = cb.Run(context.Background(), 0) })
	finishes(t, reached, "the cycle, on its way to the mark,")

	var commit func()
	var prepErr error
	prepared := async(func() { commit, _, prepErr = ca.PrepareRef(root) })
	stillWaiting(t, prepared, "a reference put in another store, during a cycle,")
	release()
	finishes(t, ran, "the cycle")
	finishes(t, prepared, "the reference put, after the cycle ended,")
	if err := errors.Join(runErr, prepErr); err != nil {
		t.Fatal(err)
	}
	commit()
}

// One store ingests, publishes and deletes while the other collects as fast
// as it can, with no grace period to hide behind. The writer brackets each
// ingest and its reference put in one write span, so that a cycle never
// finds objects whose reference is still to come.
func TestCollectWhileAnotherStoreIngests(t *testing.T) {
	a := newTestStore(t, 4<<10)
	b := openTestStoreAt(t, a.dir, 4<<10)
	ca := a.openCollector(t, Options{Grace: time.Nanosecond})
	cb := b.openCollector(t, Options{Grace: time.Nanosecond})
	ctx := context.Background()

	stop := make(chan struct{})
	cycles, reaped := 0, 0
	var cycleErr error
	collecting := async(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			stats, err := cb.Run(ctx, 0)
			if err != nil {
				cycleErr = err
				return
			}
			cycles++
			reaped += len(stats.Reaped)
		}
	})

	type tree struct {
		root key.Key
		keys []key.Key
	}
	var kept, dead []tree
	for i := range 16 {
		endSpan, err := a.objects.BeginWrite()
		if err != nil {
			t.Fatal(err)
		}
		root, keys := storeTree(t, a.objects, fmt.Sprintf("tree-%d", i), 30)
		name := fmt.Sprintf("ref-%d", i)
		putTestRef(t, ca, a.refs, name, root)
		endSpan()
		if i%2 == 1 {
			rmTestRef(t, ca, a.refs, name, root)
			dead = append(dead, tree{root, keys})
		} else {
			kept = append(kept, tree{root, keys})
		}
	}
	close(stop)
	finishes(t, collecting, "the collecting store")
	if cycleErr != nil {
		t.Fatalf("a cycle failed: %v", cycleErr)
	}
	if _, err := cb.Run(ctx, 0); err != nil { // a quiet one, for the last deletions
		t.Fatal(err)
	}
	t.Logf("%d cycles ran alongside the ingest, reaping %d segments", cycles, reaped)

	for _, ts := range []*testStore{a, b} {
		for _, tr := range kept {
			if _, err := fstree.CheckComplete(tr.root, ts.objects.Get, ts.objects.Has, 4); err != nil {
				t.Fatalf("a kept reference is no longer complete: %v", err)
			}
		}
	}
	gone := 0
	for _, tr := range dead {
		gone += countGone(t, b.objects, tr.keys)
	}
	if gone == 0 {
		t.Fatal("nothing was collected, so the test proves nothing")
	}
}

// A small store's segments never fill, and nobody may be around to seal
// them: a cycle seals the active segments no writer holds, so that they can
// be collected.
func TestIdleForeignActiveSegmentIsCollected(t *testing.T) {
	a := newTestStore(t, 1<<20)
	ca := a.openCollector(t, Options{Grace: time.Hour})
	root, keys := storeTree(t, a.objects, "dead", 40)
	putTestRef(t, ca, a.refs, "dead", root)
	rmTestRef(t, ca, a.refs, "dead", root)
	rootKeep, keysKeep := storeTree(t, a.objects, "keep", 4)
	putTestRef(t, ca, a.refs, "keep", rootKeep)
	if err := a.objects.Close(); err != nil { // the writer is gone; its segment stays, unsealed
		t.Fatal(err)
	}
	if got := activeSegments(t, a); len(got) != 1 {
		t.Fatalf("active segments = %v, want the writer's one", got)
	}

	b := openTestStoreAt(t, a.dir, 1<<20)
	cb := b.openCollector(t, Options{Grace: time.Hour})
	if _, err := cb.Run(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if got := activeSegments(t, a); len(got) != 0 {
		t.Fatalf("active segments after a cycle = %v: the idle one was not sealed", got)
	}
	backdatePacks(t, a)
	if _, err := cb.Run(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if gone := countGone(t, b.objects, keys); gone == 0 {
		t.Fatal("nothing of the idle segment's garbage was collected")
	}
	if gone := countGone(t, b.objects, keysKeep); gone != 0 {
		t.Fatalf("%d live objects were collected with it", gone)
	}
}

func TestOwnedForeignActiveSegmentIsLeftAlone(t *testing.T) {
	a := newTestStore(t, 1<<20)
	ca := a.openCollector(t, Options{Grace: time.Hour})
	root, keys := storeTree(t, a.objects, "dead", 40)
	putTestRef(t, ca, a.refs, "dead", root)
	rmTestRef(t, ca, a.refs, "dead", root)
	owned := activeSegments(t, a)
	if len(owned) != 1 {
		t.Fatalf("active segments = %v", owned)
	}

	b := openTestStoreAt(t, a.dir, 1<<20)
	cb := b.openCollector(t, Options{Grace: time.Hour})
	for range 2 {
		if _, err := cb.Run(context.Background(), 0); err != nil {
			t.Fatal(err)
		}
		backdatePacks(t, a)
	}
	if got := activeSegments(t, a); len(got) != 1 || got[0] != owned[0] {
		t.Fatalf("active segments = %v, want the live writer's %v untouched", got, owned)
	}
	if gone := countGone(t, a.objects, keys); gone != 0 {
		t.Fatalf("%d objects vanished from a segment its writer still holds", gone)
	}
	if _, keysMore := storeTree(t, a.objects, "more", 4); countGone(t, b.objects, keysMore) != 0 {
		t.Fatal("the writer's later objects are not visible to the other store")
	}
}
