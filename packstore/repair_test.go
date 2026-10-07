package packstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

func repairStore(t *testing.T, opts ...Option) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func damageRepairRecord(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := []byte{0}
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestPutVerifiedRepairsAndReopens(t *testing.T) {
	for _, data := range [][]byte{[]byte("small raw value"), compressible(8192)} {
		for _, sealed := range []bool{false, true} {
			t.Run(string(data[:3])+map[bool]string{false: "active", true: "sealed"}[sealed], func(t *testing.T) {
				s := repairStore(t)
				o := blobObj(t, data)
				later := blobObj(t, []byte("later unrelated object"))
				if err := s.Put(o.Key, o.Data); err != nil {
					t.Fatal(err)
				}
				if err := s.Put(later.Key, later.Data); err != nil {
					t.Fatal(err)
				}
				path, off := s.active.path, s.active.index[o.Key].off
				if sealed {
					if err := s.sealActiveLocked(); err != nil {
						t.Fatal(err)
					}
					path = s.sealed[0].path
				}
				damageRepairRecord(t, path, off+amberpack.RecHeaderSize)
				if err := s.PutVerified(o.Key, o.Data); err != nil {
					t.Fatal(err)
				}
				if err := s.Verify(context.Background()); err != nil {
					t.Fatal(err)
				}
				dir := s.dir
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				for _, obj := range []Object{o, later} {
					got, err := reopened.Get(obj.Key)
					if err != nil || !bytes.Equal(got, obj.Data) {
						t.Fatalf("Get: %q %v", got, err)
					}
				}
			})
		}
	}
}

func TestPutVerifiedAllCopiesAndReaderLifetime(t *testing.T) {
	s := repairStore(t, WithSegmentSize(1))
	o := blobObj(t, []byte("duplicate value"))
	raw, err := amberpack.EncodeRecord(o.Key, o.Data)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := s.AppendRecord(o.Key, raw); err != nil {
			t.Fatal(err)
		}
	}
	for _, seg := range s.sealed[:2] {
		off, _, _ := seg.fv.lookup(o.Key)
		damageRepairRecord(t, seg.path, int64(off)+amberpack.RecHeaderSize)
	}
	old, err := s.pinSegment(s.sealed[0].id)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(old.path+".repair", []byte("crashed repair"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.PutVerified(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if old == s.sealed[0] {
		t.Fatal("mapping was not replaced")
	}
	if !bytes.Equal(old.mm[:len(magicHeader)], magicHeader) {
		t.Fatal("old mapping lost")
	}
	s.endScrub()
	if len(s.retired) != 0 {
		t.Fatal("retired mappings leaked")
	}
	if err := s.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPutVerifiedRejectsBadInputAndRepairsWrongHash(t *testing.T) {
	s := repairStore(t)
	o := blobObj(t, []byte("correct"))
	if err := s.PutVerified(o.Key, []byte("incorrect")); !errors.Is(err, ErrVerify) {
		t.Fatal(err)
	}
	if found, _ := s.Has(o.Key); found {
		t.Fatal("invalid input persisted")
	}
	// Put accepts bytes without hashing; framing and CRC are valid here.
	if err := s.Put(o.Key, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	if err := s.PutVerified(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if err := s.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.PutVerified(o.Key, []byte("incorrect")); !errors.Is(err, ErrVerify) {
		t.Fatal(err)
	}
	s.setFailed(errors.New("injected failure"))
	if err := s.PutVerified(o.Key, o.Data); err == nil {
		t.Fatal("failed store accepted write")
	}
	s.Close()
	if err := s.PutVerified(o.Key, o.Data); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestPutVerifiedCallbackAndBarrier(t *testing.T) {
	s := repairStore(t, WithSegmentSize(1))
	o := blobObj(t, []byte("repair in callback"))
	if err := s.Put(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	seg := s.sealed[0]
	off, _, _ := seg.fv.lookup(o.Key)
	damageRepairRecord(t, seg.path, int64(off)) // damaged record header
	s.BeginBarrier()
	done := make(chan error, 1)
	go func() {
		var repairErr error
		err := s.ScanIndex(seg.id, func(_ key.Key, _ uint64, _ uint32) { repairErr = s.PutVerified(o.Key, o.Data) })
		if err == nil {
			err = repairErr
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("repair deadlocked with index callback")
	}
	if _, err := s.Compact(func(key.Key) bool { return false }, CompactOpts{}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(o.Key); err != nil || !bytes.Equal(got, o.Data) {
		t.Fatalf("grey key lost: %v", err)
	}
}

func TestRecoverFooterBeforeDamagedBody(t *testing.T) {
	s := repairStore(t, WithSegmentSize(1<<20))
	o := blobObj(t, []byte("first object"))
	later := blobObj(t, []byte("later object"))
	for _, obj := range []Object{o, later} {
		if err := s.Put(obj.Key, obj.Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.sealActiveLocked(); err != nil {
		t.Fatal(err)
	}
	path, dir := s.sealed[0].path, s.dir
	off, _, _ := s.sealed[0].fv.lookup(o.Key)
	s.Close()
	damageRepairRecord(t, path, int64(off)+amberpack.RecHeaderSize)
	if err := os.Rename(path, path+".active"); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Get(later.Key); err != nil || !bytes.Equal(got, later.Data) {
		t.Fatalf("later record lost: %v", err)
	}
	if err := reopened.PutVerified(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPutVerifiedPreservesMarkSet(t *testing.T) {
	s := repairStore(t)
	repaired := blobObj(t, []byte("correct value longer than damaged value"))
	other := blobObj(t, []byte("unrelated value"))
	if err := s.Put(repaired.Key, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(other.Key, other.Data); err != nil {
		t.Fatal(err)
	}
	if err := s.sealActiveLocked(); err != nil {
		t.Fatal(err)
	}
	m := s.NewMarkSet()
	m.Mark(other.Key)
	if err := s.PutVerified(repaired.Key, repaired.Data); err != nil {
		t.Fatal(err)
	}
	if !m.Contains(other.Key) {
		t.Fatal("unrelated mark lost")
	}
	if newly, present := m.Mark(repaired.Key); !newly || !present {
		t.Fatal("repaired key lost")
	}
	if _, err := s.Compact(m.Contains, CompactOpts{}); err != nil {
		t.Fatal(err)
	}
	for _, obj := range []Object{repaired, other} {
		if got, err := s.Get(obj.Key); err != nil || !bytes.Equal(got, obj.Data) {
			t.Fatalf("marked record lost: %v", err)
		}
	}
}

func TestPutVerifiedNewAndHealthy(t *testing.T) {
	s := repairStore(t)
	o := blobObj(t, []byte("new verified object"))
	if err := s.PutVerified(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	before := s.active.size
	if err := s.PutVerified(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if s.active.size != before {
		t.Fatal("healthy duplicate appended")
	}
	if err := s.sealActiveLocked(); err != nil {
		t.Fatal(err)
	}
	original := s.sealed[0]
	if err := s.PutVerified(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if s.sealed[0] != original || s.active != nil {
		t.Fatal("healthy sealed record rewritten")
	}
}

func TestPutVerifiedActiveReadError(t *testing.T) {
	s := repairStore(t)
	o := blobObj(t, []byte("truncated record"))
	if err := s.Put(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if err := s.active.f.Truncate(int64(len(magicHeader))); err != nil {
		t.Fatal(err)
	}
	if err := s.PutVerified(o.Key, o.Data); err == nil {
		t.Fatal("truncated active read was accepted")
	}
	if len(s.sealed) != 0 {
		t.Fatal("read failure sealed the active file")
	}
}

func TestPutVerifiedConcurrentReadersAndWriters(t *testing.T) {
	s := repairStore(t)
	repaired := blobObj(t, []byte("correct repaired value"))
	other := blobObj(t, []byte("unchanged object"))
	if err := s.Put(repaired.Key, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(other.Key, other.Data); err != nil {
		t.Fatal(err)
	}
	if err := s.sealActiveLocked(); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			<-start
			for range 100 {
				got, err := s.Get(repaired.Key)
				if err != nil || (!bytes.Equal(got, repaired.Data) && !bytes.Equal(got, []byte("bad"))) {
					t.Errorf("repaired read: %q %v", got, err)
					return
				}
				got, err = s.Get(other.Key)
				if err != nil || !bytes.Equal(got, other.Data) {
					t.Errorf("unrelated read: %q %v", got, err)
					return
				}
				if _, err := s.GetRecord(repaired.Key); err != nil {
					t.Error(err)
					return
				}
			}
		})
		wg.Go(func() {
			<-start
			for range 25 {
				if err := s.PutVerified(repaired.Key, repaired.Data); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	if err := s.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDeferredVerifiedPutsAreDurableAfterOneSync(t *testing.T) {
	s := repairStore(t)
	objs := testObjects(t, 3)
	before := s.fsyncs.Load()
	if err := s.PutVerifiedDeferred(objs[0].Key, []byte("wrong")); !errors.Is(err, ErrVerify) {
		t.Fatal(err)
	}
	for _, o := range objs {
		for range 2 { // a new record, then a healthy one
			if err := s.PutVerifiedDeferred(o.Key, o.Data); err != nil {
				t.Fatal(err)
			}
		}
		wantObjects(t, s, []Object{o})
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if n := s.fsyncs.Load() - before; n != 1 {
		t.Fatalf("%d fsyncs counted for a run of deferred puts and its Sync, want 1", n)
	}
	dir := s.dir
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, dir)
	wantObjects(t, reopened, objs)
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestADurablePutSyncsARecordADeferredPutLeftBehind(t *testing.T) {
	s := repairStore(t)
	o := blobObj(t, []byte("deferred, then promised"))
	if err := s.PutVerifiedDeferred(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	before := s.fsyncs.Load()
	if err := s.Put(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if n := s.fsyncs.Load() - before; n != 1 {
		t.Fatalf("%d fsyncs for a Put that found a deferred record, want 1", n)
	}
	if err := s.Put(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if n := s.fsyncs.Load() - before; n != 1 {
		t.Fatalf("%d fsyncs after a Put that found the record synced, want still 1", n)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// Another store that syncs counts a record as a duplicate only as far as its
// writer has synced it. A deferred put's record, new or found healthy by a
// second deferred put, is synced by nobody until Sync.
func TestDeferredVerifiedPutIsNotAdvertisedAsSyncedBeforeSync(t *testing.T) {
	o := testObjects(t, 1)[0]
	deferred := func(dir string) *Store {
		w := openStore(t, dir)
		for range 2 { // a new record, then a healthy one
			if err := w.PutVerifiedDeferred(o.Key, o.Data); err != nil {
				t.Fatal(err)
			}
		}
		return w
	}

	dir := t.TempDir()
	deferred(dir) // written, its writer still open and yet to sync
	s := openStore(t, dir)
	wantObjects(t, s, []Object{o})
	putAll(t, s, []Object{o})
	if !ownCopy(s, o.Key) {
		t.Fatal("a syncing store acknowledged a write whose only copy is a deferred record its writer has yet to sync")
	}

	dir = t.TempDir()
	if err := deferred(dir).Sync(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, dir)
	wantObjects(t, s, []Object{o})
	putAll(t, s, []Object{o})
	if ownCopy(s, o.Key) {
		t.Fatal("a second copy was written of a deferred record its writer had synced")
	}
}

// syncedLen is the data length the sidecar of the store's own active segment
// knows durable.
func syncedLen(t *testing.T, s *Store) int64 {
	t.Helper()
	recs, _ := readSidecarFile(t, s.active.path+sidecarSuffix)
	var n int64
	for _, r := range recs {
		if r.kind == sidecarSynced {
			n = max(n, int64(r.off))
		}
	}
	return n
}

// A Put that loses the race for its key to a deferred put finds the record in
// the append path, past its duplicate check, and must not acknowledge it
// unsynced.
func TestAppendSyncsARecordADeferredPutLeftBehind(t *testing.T) {
	s := repairStore(t)
	o := blobObj(t, []byte("deferred, then raced"))
	if err := s.PutVerifiedDeferred(o.Key, o.Data); err != nil {
		t.Fatal(err)
	}
	if n := syncedLen(t, s); n >= s.active.size {
		t.Fatalf("the sidecar knows %d of %d bytes synced before anything synced the deferred record", n, s.active.size)
	}
	rec, err := amberpack.EncodeRecord(o.Key, o.Data)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.append(o.Key, rec, true); err != nil { // Put, having lost the race
		t.Fatal(err)
	}
	if n := syncedLen(t, s); n != s.active.size {
		t.Fatalf("the sidecar knows %d of %d bytes synced after a durable append found the deferred record", n, s.active.size)
	}
}
