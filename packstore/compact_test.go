package packstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/key"
)

// compactStore builds a store with objs[0..1] and objs[2..3] in two sealed
// segments and objs[4] in the active one.
func compactStore(t *testing.T) (*Store, []Object) {
	t.Helper()
	s, err := Open(t.TempDir(), WithSegmentSize(8<<10), WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	objs := make([]Object, 5)
	for i := range objs {
		data := incompressible(4 << 10)
		data[0] = byte(i)
		objs[i] = blobObj(t, data)
		if err := s.Put(objs[i].Key, objs[i].Data); err != nil {
			t.Fatal(err)
		}
	}
	return s, objs
}

func liveSet(objs []Object, idx ...int) func(key.Key) bool {
	m := map[key.Key]bool{}
	for _, i := range idx {
		m[objs[i].Key] = true
	}
	return func(k key.Key) bool { return m[k] }
}

func TestLiveness(t *testing.T) {
	s, objs := compactStore(t)
	report, err := s.Liveness(liveSet(objs, 0, 2, 4))
	if err != nil {
		t.Fatal(err)
	}
	if len(report) != 3 {
		t.Fatalf("got %d segments, want 3: %+v", len(report), report)
	}
	recBytes := uint64(amberpack.RecHeaderSize) + uint64(len(objs[0].Data))
	for i, seg := range report[:2] {
		if !seg.Sealed {
			t.Errorf("segment %d not sealed", i)
		}
		if seg.LiveKeys != 1 || seg.DeadKeys != 1 {
			t.Errorf("segment %d: live=%d dead=%d, want 1/1", i, seg.LiveKeys, seg.DeadKeys)
		}
		if seg.LiveBytes != recBytes || seg.DeadBytes != recBytes {
			t.Errorf("segment %d: liveBytes=%d deadBytes=%d, want %d", i, seg.LiveBytes, seg.DeadBytes, recBytes)
		}
	}
	act := report[2]
	if act.Sealed || act.LiveKeys != 1 || act.DeadKeys != 0 {
		t.Errorf("active segment: %+v", act)
	}
}

func TestCompactRemovesDeadObjects(t *testing.T) {
	s, objs := compactStore(t)
	live := liveSet(objs, 0, 2, 4)
	stats, err := s.Compact(live, CompactOpts{MinDeadRatio: 0.4})
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsCompacted != 2 {
		t.Errorf("SegmentsCompacted = %d, want 2", stats.SegmentsCompacted)
	}
	if stats.RecordsCopied != 2 {
		t.Errorf("RecordsCopied = %d, want 2", stats.RecordsCopied)
	}
	if stats.BytesFreed == 0 {
		t.Error("BytesFreed = 0")
	}
	if len(stats.Victims) != 2 {
		t.Errorf("Victims = %v, want 2 ids", stats.Victims)
	}
	for i, o := range objs {
		data, err := s.Get(o.Key)
		if live(o.Key) {
			if err != nil {
				t.Fatalf("live object %d: %v", i, err)
			}
			if string(data) != string(o.Data) {
				t.Errorf("live object %d corrupted", i)
			}
		} else if !errors.Is(err, ErrNotFound) {
			t.Errorf("dead object %d: err = %v, want ErrNotFound", i, err)
		}
	}
	if err := s.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
	dir := s.dir
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, i := range []int{0, 2, 4} {
		if _, err := s2.Get(objs[i].Key); err != nil {
			t.Fatalf("after reopen, object %d: %v", i, err)
		}
	}
}

func TestCompactSkipsBelowThreshold(t *testing.T) {
	s, objs := compactStore(t)
	stats, err := s.Compact(liveSet(objs, 0, 2, 4), CompactOpts{MinDeadRatio: 0.9})
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsCompacted != 0 {
		t.Errorf("SegmentsCompacted = %d, want 0", stats.SegmentsCompacted)
	}
	for _, o := range objs {
		if _, err := s.Get(o.Key); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompactAllDead(t *testing.T) {
	s, objs := compactStore(t)
	stats, err := s.Compact(func(key.Key) bool { return false }, CompactOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.RecordsCopied != 0 {
		t.Errorf("RecordsCopied = %d, want 0", stats.RecordsCopied)
	}
	for _, o := range objs {
		if _, err := s.Get(o.Key); !errors.Is(err, ErrNotFound) {
			t.Errorf("err = %v, want ErrNotFound", err)
		}
	}
}

func TestCompactHorizonSparesYoungSegments(t *testing.T) {
	s, objs := compactStore(t)
	// Every segment was just written, so a horizon in the past spares all.
	stats, err := s.Compact(func(key.Key) bool { return false },
		CompactOpts{Horizon: time.Now().Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsCompacted != 0 {
		t.Errorf("SegmentsCompacted = %d, want 0", stats.SegmentsCompacted)
	}
	for _, o := range objs {
		if _, err := s.Get(o.Key); err != nil {
			t.Fatalf("young segment reaped past the horizon: %v", err)
		}
	}
}

func TestCompactRejectsCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithSegmentSize(8<<10), WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	objs := make([]Object, 2)
	for i := range objs {
		data := incompressible(4 << 10)
		data[0] = byte(i)
		objs[i] = blobObj(t, data)
		if err := s.Put(objs[i].Key, objs[i].Data); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	segs, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil || len(segs) != 1 {
		t.Fatalf("segs = %v, err = %v", segs, err)
	}
	// The footer CRC does not cover the body, so the store reopens cleanly.
	raw, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	raw[len(magicHeader)+amberpack.RecHeaderSize] ^= 0xFF
	if err := os.WriteFile(segs[0], raw, 0o644); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir, WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// objs[1] is dead, so the segment is a victim; copying corrupted objs[0]
	// must fail.
	if _, err := s.Compact(liveSet(objs, 0), CompactOpts{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	// The victim must survive a failed compaction.
	if segs, _ = filepath.Glob(filepath.Join(dir, "*.seg")); len(segs) != 1 {
		t.Fatalf("segment deleted after failed compaction: %v", segs)
	}
}

func TestCompactKeepsBarrierGrey(t *testing.T) {
	s, objs := compactStore(t)
	s.BeginBarrier()
	novel := blobObj(t, incompressible(4<<10))
	batch := func(yield func(Object, error) bool) {
		_ = yield(objs[0], nil) && yield(novel, nil) // objs[0] is a dedup hit
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(liveSet(objs, 2), CompactOpts{}); err != nil {
		t.Fatal(err)
	}
	for i, k := range []key.Key{objs[0].Key, novel.Key, objs[2].Key} {
		if has, err := s.Has(k); err != nil || !has {
			t.Fatalf("grey/live object %d gone (has=%v, err=%v)", i, has, err)
		}
	}
	for _, i := range []int{1, 3, 4} {
		if has, err := s.Has(objs[i].Key); err != nil || has {
			t.Fatalf("dead object %d survived (has=%v, err=%v)", i, has, err)
		}
	}

	// Compact consumed the capture: the grey objects are dead now.
	if _, err := s.Compact(liveSet(objs, 2), CompactOpts{}); err != nil {
		t.Fatal(err)
	}
	if has, _ := s.Has(objs[0].Key); has {
		t.Fatal("grey set survived its Compact")
	}
	if has, _ := s.Has(objs[2].Key); !has {
		t.Fatal("live object lost")
	}
}

func TestObserveKeysProtectsClosure(t *testing.T) {
	s, objs := compactStore(t)
	s.BeginBarrier()
	// A reference PUT racing the mark greys its whole walked closure.
	s.ObserveKeys([]key.Key{objs[1].Key, objs[3].Key})
	if _, err := s.Compact(func(key.Key) bool { return false }, CompactOpts{}); err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{1, 3} {
		if has, err := s.Has(objs[i].Key); err != nil || !has {
			t.Fatalf("greyed closure key %d gone (has=%v, err=%v)", i, has, err)
		}
	}
	for _, i := range []int{0, 2, 4} {
		if has, err := s.Has(objs[i].Key); err != nil || has {
			t.Fatalf("dead object %d survived (has=%v, err=%v)", i, has, err)
		}
	}
}

func TestAbortBarrier(t *testing.T) {
	s, objs := compactStore(t)
	s.BeginBarrier()
	if err := s.Put(objs[0].Key, objs[0].Data); err != nil { // dedup observe
		t.Fatal(err)
	}
	s.AbortBarrier()
	if _, err := s.Compact(func(key.Key) bool { return false }, CompactOpts{}); err != nil {
		t.Fatal(err)
	}
	if has, _ := s.Has(objs[0].Key); has {
		t.Fatal("aborted capture still protected an object")
	}
}
