package packstore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/draganm/rebma/amberpack"
)

// recordObj is o as a pre-encoded record: what a caller that already holds
// the record bytes (a staged pack) offers instead of Data.
func recordObj(t *testing.T, o Object) Object {
	t.Helper()
	rec, err := amberpack.EncodeRecord(o.Key, o.Data)
	if err != nil {
		t.Fatal(err)
	}
	return Object{Key: o.Key, Record: rec}
}

func TestWriteParallelRecordsStoredAndReadable(t *testing.T) {
	s := openStore(t, t.TempDir(), WithSegmentSize(32<<10))
	objs := testObjects(t, 60)
	recs := make([]Object, len(objs))
	for i, o := range objs {
		recs[i] = recordObj(t, o)
	}
	batch := append(append([]Object{}, recs...), recs[:7]...) // in-stream dups
	stats, err := s.WriteParallel(objSeq(batch, -1), WriteOpts{Writers: 3, BatchSize: 8 << 10, Verify: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Stored != len(objs) || stats.Deduped != 7 {
		t.Fatalf("stats = %+v, want %d stored, 7 deduped", stats, len(objs))
	}
	var wantBytes int64
	for _, o := range objs {
		wantBytes += int64(len(o.Data))
	}
	if stats.BytesStored != wantBytes {
		t.Fatalf("BytesStored = %d, want %d (the records' ulen)", stats.BytesStored, wantBytes)
	}
	for i, o := range objs {
		data, err := s.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
		// The record went in verbatim: the stored bytes are the offered ones.
		got, err := s.GetRecord(o.Key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, recs[i].Record) {
			t.Fatalf("object %d: stored record differs from the offered one", i)
		}
	}
}

func TestWriteParallelRecordsDedupAgainstPresent(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 20)
	for _, o := range objs[:10] {
		if err := s.Put(o.Key, o.Data); err != nil {
			t.Fatal(err)
		}
	}
	recs := make([]Object, len(objs))
	for i, o := range objs {
		recs[i] = recordObj(t, o)
	}
	stats, err := s.WriteParallel(objSeq(recs, -1), WriteOpts{Verify: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Stored != 10 || stats.Deduped != 10 {
		t.Fatalf("stats = %+v, want 10/10", stats)
	}
}

func TestWriteParallelRecordVerifyCatchesWrongPayload(t *testing.T) {
	// The record is well formed (its CRC is right) but its payload does not
	// hash to its key: only Verify can tell, exactly as for Data.
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 3)
	rec, err := amberpack.EncodeRecord(objs[0].Key, append(bytes.Clone(objs[0].Data), 0xFF))
	if err != nil {
		t.Fatal(err)
	}
	bad := Object{Key: objs[0].Key, Record: rec}
	_, err = s.WriteParallel(objSeq([]Object{recordObj(t, objs[1]), bad}, -1), WriteOpts{Verify: true})
	if !errors.Is(err, ErrVerify) {
		t.Fatalf("err = %v, want ErrVerify", err)
	}
	if has, _ := s.Has(objs[0].Key); has {
		t.Fatal("mismatching record was stored")
	}
	// Without Verify the record is taken on trust, as Data is.
	if _, err := s.WriteParallel(objSeq([]Object{bad}, -1), WriteOpts{}); err != nil {
		t.Fatalf("unverified write: %v", err)
	}
}

func TestWriteParallelRecordCorruptFails(t *testing.T) {
	s := openStore(t, t.TempDir())
	o := recordObj(t, testObjects(t, 1)[0])
	o.Record[len(o.Record)-1] ^= 0x01 // CRC no longer matches
	if _, err := s.WriteParallel(objSeq([]Object{o}, -1), WriteOpts{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if has, _ := s.Has(o.Key); has {
		t.Fatal("corrupt record was stored")
	}
}

func TestWriteParallelRecordKeyMismatchFails(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 2)
	o := recordObj(t, objs[0])
	o.Key = objs[1].Key // record says objs[0], object says objs[1]
	if _, err := s.WriteParallel(objSeq([]Object{o}, -1), WriteOpts{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestWriteParallelDataAndRecordFails(t *testing.T) {
	s := openStore(t, t.TempDir())
	o := testObjects(t, 1)[0]
	both := recordObj(t, o)
	both.Data = o.Data
	if _, err := s.WriteParallel(objSeq([]Object{both}, -1), WriteOpts{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestWriteBatchRecords(t *testing.T) {
	s := openStore(t, t.TempDir())
	objs := testObjects(t, 30)
	if err := s.Put(objs[0].Key, objs[0].Data); err != nil {
		t.Fatal(err)
	}
	recs := make([]Object, 0, len(objs)+1)
	for _, o := range objs {
		recs = append(recs, recordObj(t, o))
	}
	recs = append(recs, recs[5]) // in-batch duplicate
	if err := s.WriteBatch(objSeq(recs, -1)); err != nil {
		t.Fatal(err)
	}
	for _, o := range objs {
		data, err := s.Get(o.Key)
		if err != nil || !bytes.Equal(data, o.Data) {
			t.Fatalf("Get(%s): %v", o.Key, err)
		}
	}
	bad := recordObj(t, testObjects(t, 40)[39])
	bad.Record[len(bad.Record)-1] ^= 0x01
	if err := s.WriteBatch(objSeq([]Object{bad}, -1)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("WriteBatch of a corrupt record: err = %v, want ErrCorrupt", err)
	}
}

// TestAppendRecordRejectsNilRecord: a nil raw must not slip through as an
// Object carrying no Record, which prepare would take for a Data write and
// append as an empty payload under k, quietly manufacturing an object the
// GC copier never held.
func TestAppendRecordRejectsNilRecord(t *testing.T) {
	s := openStore(t, t.TempDir())
	o := testObjects(t, 1)[0]
	if err := s.AppendRecord(o.Key, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("AppendRecord(k, nil): err = %v, want ErrCorrupt", err)
	}
	if has, _ := s.Has(o.Key); has {
		t.Fatal("a nil record stored an object")
	}
}
