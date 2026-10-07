package packstore

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/draganm/rebma/key"
)

// A lookup that finds nothing lists the directory. While Compact or Remove
// deletes segments — out of the view already, not yet out of the directory —
// such a listing must not map one again: the duplicate check would go on
// finding objects that are gone, and a write of one would be skipped.
func TestALookupBetweenDetachAndUnlinkDoesNotBringVictimsBack(t *testing.T) {
	removals := map[string]func(*Store, []Object) error{
		"Compact": func(s *Store, objs []Object) error {
			_, err := s.Compact(liveSet(objs, 0, 2, 3, 4), CompactOpts{MinDeadRatio: 0.1})
			return err
		},
		"Remove": func(s *Store, _ []Object) error {
			s.mu.RLock()
			id := s.sealed[0].id
			s.mu.RUnlock()
			return s.Remove(id)
		},
	}
	for name, remove := range removals {
		t.Run(name, func(t *testing.T) {
			s, objs := compactStore(t) // objs[0..1] in the first sealed segment
			absent := blobObj(t, []byte("nobody stored this"))
			looked := make(chan struct{})
			s.afterDetach = func() {
				go func() {
					defer close(looked)
					s.Has(absent.Key)
				}()
				time.Sleep(150 * time.Millisecond) // ample for the lookup to list the directory, were it let
			}
			if err := remove(s, objs); err != nil {
				t.Fatal(err)
			}
			<-looked
			if has, err := s.hasLocal(objs[1].Key); err != nil || has {
				t.Fatalf("hasLocal = %v, %v: a listing between the segment leaving the view and leaving the directory mapped it again", has, err)
			}
		})
	}
}

// The same under load, for the race detector: lookups that find nothing, all
// through a pass that removes every segment.
func TestLookupsDuringCompact(t *testing.T) {
	s := openStore(t, t.TempDir(), WithSync(false), WithSegmentSize(8<<10))
	objs := make([]Object, 300)
	for i := range objs {
		data := incompressible(4 << 10)
		data[0], data[1] = byte(i), byte(i>>8)
		objs[i] = blobObj(t, data)
	}
	putAll(t, s, objs) // two to a segment
	live := map[key.Key]bool{}
	for i := 0; i < len(objs); i += 2 {
		live[objs[i].Key] = true // every segment half dead: all of them victims
	}
	absent := blobObj(t, []byte("nobody stored this"))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if has, err := s.Has(absent.Key); err != nil || has {
					t.Errorf("Has = %v, %v", has, err)
					return
				}
			}
		}()
	}
	_, err := s.Compact(func(k key.Key) bool { return live[k] }, CompactOpts{MinDeadRatio: 0.1})
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	ghosts := 0
	for i := 1; i < len(objs); i += 2 {
		has, err := s.hasLocal(objs[i].Key)
		if err != nil {
			t.Fatal(err)
		}
		if has {
			ghosts++
		}
	}
	if ghosts > 0 {
		t.Fatalf("the duplicate check still finds %d reaped objects: a listing between the victims leaving the view and leaving the directory mapped them again", ghosts)
	}
}

// The store's first write takes an idle segment for its own and drops the
// read-only view it had of it. A lookup's refresh may be reading that view at
// that moment; it must not fail the lookup.
func TestLookupsDuringTheFirstWriteSeeNoError(t *testing.T) {
	objs := testObjects(t, 3)
	absent := blobObj(t, []byte("nobody stored this"))
	for round := range 40 {
		dir := t.TempDir()
		w, err := Open(dir, WithSync(false))
		if err != nil {
			t.Fatal(err)
		}
		putAll(t, w, objs[:2])
		if err := w.Close(); err != nil { // leaves an idle segment, which the first write below adopts
			t.Fatal(err)
		}
		s := openStore(t, dir, WithSync(false))
		stop := make(chan struct{})
		failed := make(chan error, 1)
		go func() {
			defer close(failed)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := s.Has(absent.Key); err != nil {
					failed <- err
					return
				}
			}
		}()
		if err := s.Put(objs[2].Key, objs[2].Data); err != nil {
			t.Fatal(err)
		}
		close(stop)
		if err := <-failed; err != nil {
			t.Fatalf("round %d: a lookup during the store's first write: %v", round, err)
		}
	}
}

// A refresh that fails half way must leave the view as it was. One that had
// already read another store's new records, and then failed on something
// else, used to keep its place in that segment and drop the records: no later
// refresh found them again.
func TestAFailedRefreshLosesNothing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a file that grants nobody anything")
	}
	objs := testObjects(t, 2)
	for round := range 24 { // the refresh visits segments in map order: either one may come first
		dir := t.TempDir()
		w := openStore(t, dir, WithSync(false))
		reader := openStore(t, dir)
		putAll(t, w, objs[:1])
		wantObjects(t, reader, objs[:1]) // the reader follows w's segment from here on
		putAll(t, w, objs[1:])

		bad := filepath.Join(dir, segmentName(0xff, activeSuffix))
		if err := os.WriteFile(bad, magicHeader, 0); err != nil {
			t.Fatal(err)
		}
		reader.Get(objs[1].Key) // may fail, fairly: the refresh cannot read that file
		if err := os.Remove(bad); err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Get(objs[1].Key); err != nil {
			t.Fatalf("round %d: after the unreadable file was removed: %v", round, err)
		}
	}
}

// A segment that was listed and is gone when the refresh comes to open it
// means the listing is out of date: what the segment held may have moved to
// one created since. The refresh lists again rather than settle for less.
func TestRefreshListsAgainWhenAListedSegmentVanished(t *testing.T) {
	dir := t.TempDir()
	reader := openStore(t, dir) // an empty view
	c := openStore(t, dir, WithSync(false), WithSegmentSize(8<<10))
	objs := make([]Object, 5)
	for i := range objs {
		data := incompressible(4 << 10)
		data[0] = byte(i)
		objs[i] = blobObj(t, data)
	}
	putAll(t, c, objs) // objs[0..1] and objs[2..3] sealed, objs[4] active

	var once sync.Once
	var compactErr error
	reader.afterList = func() {
		once.Do(func() { // a whole pass between the listing and the opening of what it lists
			_, compactErr = c.Compact(liveSet(objs, 0, 2, 3, 4), CompactOpts{MinDeadRatio: 0.1})
		})
	}
	got, err := reader.Get(objs[0].Key) // moved by the pass, into a segment the listing does not have
	if compactErr != nil {
		t.Fatal(compactErr)
	}
	if err != nil || len(got) != len(objs[0].Data) {
		t.Fatalf("Get = %d bytes, %v: the object is in the store", len(got), err)
	}
}
