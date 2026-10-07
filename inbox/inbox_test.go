package inbox

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/draganm/rebma/amberpack"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
	"github.com/zeebo/blake3"
)

func blobObject(t *testing.T, data []byte) fstree.Object {
	t.Helper()
	sum := blake3.Sum256(data)
	k, err := key.NewFromHash(key.Blob, uint64(len(data)), sum)
	if err != nil {
		t.Fatalf("NewFromHash: %v", err)
	}
	return fstree.Object{Key: k, Bytes: data}
}

func packBody(t *testing.T, objs ...fstree.Object) []byte {
	t.Helper()
	var buf bytes.Buffer
	pw := amberpack.NewWriter(&buf)
	for _, o := range objs {
		if err := pw.Add(o); err != nil {
			t.Fatalf("pack add: %v", err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("pack close: %v", err)
	}
	return buf.Bytes()
}

func newTestStore(t *testing.T) *packstore.Store {
	t.Helper()
	s, err := packstore.Open(filepath.Join(t.TempDir(), "store"), packstore.WithSync(false))
	if err != nil {
		t.Fatalf("packstore.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCommitProcessesIntoStore(t *testing.T) {
	store := newTestStore(t)
	ib, err := Open(filepath.Join(t.TempDir(), "inbox"), store, 2, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	obj := blobObject(t, []byte("hello inbox"))
	root := obj.Key
	body := packBody(t, obj)

	tmp, h, _, err := ib.Stage(Meta{Ref: "r", Root: root[:]}, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	added, err := ib.Commit(tmp, h, root)
	if err != nil || !added {
		t.Fatalf("Commit: added=%v err=%v", added, err)
	}

	ib.WaitFor(root)
	has, err := store.Has(obj.Key)
	if err != nil || !has {
		t.Fatalf("object not stored after WaitFor: has=%v err=%v", has, err)
	}
}

func TestCommitIdempotent(t *testing.T) {
	store := newTestStore(t)
	ib, err := Open(filepath.Join(t.TempDir(), "inbox"), store, 1, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	obj := blobObject(t, []byte("dup payload"))
	root := obj.Key
	body := packBody(t, obj)

	tmp1, h1, _, err := ib.Stage(Meta{Root: root[:]}, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	added1, err := ib.Commit(tmp1, h1, root)
	if err != nil || !added1 {
		t.Fatalf("first commit: added=%v err=%v", added1, err)
	}
	tmp2, h2, _, err := ib.Stage(Meta{Root: root[:]}, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	added2, err := ib.Commit(tmp2, h2, root)
	if err != nil {
		t.Fatalf("second commit err: %v", err)
	}
	if added2 {
		t.Fatalf("second commit of identical body should report added=false")
	}
}

func TestRecoveryResumesEntry(t *testing.T) {
	store := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "inbox")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	obj := blobObject(t, []byte("left behind by a crash"))
	root := obj.Key
	body := packBody(t, obj)

	var entry bytes.Buffer
	if err := writeMetaHeader(&entry, Meta{Root: root[:]}); err != nil {
		t.Fatal(err)
	}
	entry.Write(body)
	sum := blake3.Sum256(body)
	name := hex.EncodeToString(sum[:]) + ".pack"
	if err := os.WriteFile(filepath.Join(dir, name), entry.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	ib, err := Open(dir, store, 1, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	ib.WaitFor(root)
	has, err := store.Has(obj.Key)
	if err != nil || !has {
		t.Fatalf("recovered entry not processed: has=%v err=%v", has, err)
	}
}

func TestCorruptPackQuarantined(t *testing.T) {
	store := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "inbox")
	ib, err := Open(dir, store, 1, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	root := blobObject(t, []byte("x")).Key
	garbage := []byte("NOT-AN-AMBERPACK-STREAM")
	tmp, h, _, err := ib.Stage(Meta{Root: root[:]}, bytes.NewReader(garbage))
	if err != nil {
		t.Fatal(err)
	}
	added, err := ib.Commit(tmp, h, root)
	if err != nil || !added {
		t.Fatalf("Commit: added=%v err=%v", added, err)
	}

	ib.WaitFor(root) // must release even though processing failed

	name := hex.EncodeToString(h) + ".pack"
	if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
		t.Fatalf("entry should have left the inbox dir; stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "failed", name)); err != nil {
		t.Fatalf("entry should be quarantined under failed/: %v", err)
	}
}

func TestWithGateBracketsDrain(t *testing.T) {
	store := newTestStore(t)
	obj := blobObject(t, []byte("gated entry"))
	var mu sync.Mutex
	acquired, released := 0, 0
	writtenAtRelease := false
	gate := func() func() {
		mu.Lock()
		acquired++
		mu.Unlock()
		return func() {
			has, _ := store.Has(obj.Key)
			mu.Lock()
			released++
			writtenAtRelease = has
			mu.Unlock()
		}
	}
	ib, err := Open(filepath.Join(t.TempDir(), "inbox"), store, 1, nil, WithGate(gate))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { ib.Close() })

	root := obj.Key
	tmp, h, _, err := ib.Stage(Meta{Ref: "r", Root: root[:]}, bytes.NewReader(packBody(t, obj)))
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if added, err := ib.Commit(tmp, h, root); err != nil || !added {
		t.Fatalf("Commit: added=%v err=%v", added, err)
	}
	ib.WaitFor(root)

	mu.Lock()
	defer mu.Unlock()
	if acquired != 1 || released != 1 {
		t.Fatalf("gate acquired %d released %d, want 1/1", acquired, released)
	}
	if !writtenAtRelease {
		t.Fatal("gate released before the entry's objects were written")
	}
}
