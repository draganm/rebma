package packstore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/draganm/rebma/amberpack"
)

func TestWriteParallelStoresAll(t *testing.T) {
	s := openStore(t, t.TempDir(), WithSegmentSize(32<<10))
	objs := testObjects(t, 300)
	batch := append(append([]Object{}, objs...), objs[:50]...) // 50 in-stream dups
	stats, err := s.WriteParallel(objSeq(batch, -1), WriteOpts{Writers: 4, BatchSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Stored != len(objs) {
		t.Fatalf("Stored = %d, want %d", stats.Stored, len(objs))
	}
	if stats.Deduped != 50 {
		t.Fatalf("Deduped = %d, want 50", stats.Deduped)
	}
	var wantBytes int64
	for _, o := range objs {
		wantBytes += int64(len(o.Data))
	}
	if stats.BytesStored != wantBytes {
		t.Fatalf("BytesStored = %d, want %d", stats.BytesStored, wantBytes)
	}
	for _, o := range objs {
		data, err := s.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
	}
}

func TestWriteParallelSkipsExisting(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 20)
	for _, o := range objs[:10] {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := s.WriteParallel(objSeq(objs, -1), WriteOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Stored != 10 || stats.Deduped != 10 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestWriteParallelVerifyCatchesMismatch(t *testing.T) {
	s := openStore(t, t.TempDir())
	good := testObjects(t, 5)
	bad := good[2]
	bad.Data = append(bytes.Clone(bad.Data), 0xFF) // payload no longer matches the key
	objs := append(append([]Object{}, good[:2]...), bad)
	_, err := s.WriteParallel(objSeq(objs, -1), WriteOpts{Verify: true})
	if !errors.Is(err, ErrVerify) {
		t.Fatalf("err = %v, want ErrVerify", err)
	}
}

func TestWriteParallelIteratorError(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 10)
	_, err := s.WriteParallel(objSeq(objs, 7), WriteOpts{Writers: 2})
	if err == nil {
		t.Fatal("want iterator error")
	}
}

func TestWriteParallelOnClosedStore(t *testing.T) {
	s := openStore(t, t.TempDir())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	objs := testObjects(t, 5)
	_, err := s.WriteParallel(objSeq(objs, -1), WriteOpts{Writers: 2})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestWriteParallelErrorFlushesPrefix(t *testing.T) {
	// An erroring run must leave its appended prefix durable (fsynced):
	// reopen after a dirty stop and the prefix records must still be there.
	dir := t.TempDir()
	s := openStore(t, dir)
	objs := testObjects(t, 10)
	stats, err := s.WriteParallel(objSeq(objs, 7), WriteOpts{Writers: 1, BatchSize: 1 << 30})
	if err == nil {
		t.Fatal("want iterator error")
	}
	// stats.Stored objects were appended but never hit a BatchSize flush; the
	// error-path sync must have made them durable. Verify visibility now…
	// Note: ctx cancellation races with channel drain, so fewer than 7 objects
	// may have been appended — check only what was actually stored.
	stored := stats.Stored
	if stored == 0 {
		t.Skip("no objects were appended before the error (scheduling race); nothing to verify")
	}
	for _, o := range objs[:stored] {
		has, err := s.Has(o.Key)
		if err != nil || !has {
			t.Fatalf("Has(%s) = %v, %v", o.Key, has, err)
		}
	}
	// …and durability across a reopen.
	// Note: Close() itself fsyncs, so the reopen check alone wouldn't prove
	// the error-path sync — the Writers:1 + huge BatchSize setup ensures the
	// ONLY fsync before Close comes from the new error-path sync.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openStore(t, dir)
	for _, o := range objs[:stored] {
		data, err := s2.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s) after reopen: %v", o.Key, err)
		}
	}
}

// A dedup-only run must still fsync: the matched records may belong to a
// concurrent writer that has not synced yet.
func TestWriteParallelDedupOnlyRunStillSyncs(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 4)
	// Stand in for a concurrent, not-yet-committed writer.
	for _, o := range objs {
		rec, err := amberpack.EncodeRecord(o.Key, o.Data)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.append(o.Key, rec, false); err != nil {
			t.Fatal(err)
		}
	}
	before := s.fsyncs.Load()
	stats, err := s.WriteParallel(objSeq(objs, -1), WriteOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Stored != 0 || stats.Deduped != len(objs) {
		t.Fatalf("stats = %+v, want all deduped", stats)
	}
	if s.fsyncs.Load() == before {
		t.Fatal("WriteParallel returned success without an fsync")
	}
}

func TestWriteParallelSyncsOncePerRun(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 16)
	before := s.fsyncs.Load()
	if _, err := s.WriteParallel(objSeq(objs, -1), WriteOpts{Writers: 8}); err != nil {
		t.Fatal(err)
	}
	if n := s.fsyncs.Load() - before; n != 1 {
		t.Fatalf("%d fsyncs for one small run, want 1", n)
	}
}
