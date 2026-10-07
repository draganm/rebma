package fstree_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
)

// memStore is an in-memory object store for builder-emitted objects.
type memStore map[key.Key][]byte

func (m memStore) get(k key.Key) ([]byte, error) {
	b, ok := m[k]
	if !ok {
		return nil, fmt.Errorf("object %s not in store", k)
	}
	return b, nil
}

func (m memStore) emit(o fstree.Object) error {
	m[o.Key] = o.Bytes
	return nil
}

// bigDir builds a directory of n regular-file entries named e00000..e<n-1>
// into store, chunked into a multi-level prolly tree, and returns its root.
func bigDir(t *testing.T, store memStore, n int) key.Key {
	t.Helper()
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.emit(blob); err != nil {
		t.Fatal(err)
	}
	db := fstree.NewDirBuilder(chunkers.NewItemChunker(3))
	for i := range n {
		e := fstree.Entry{
			Name:       fmt.Appendf(nil, "e%05d", i),
			Mode:       0o100644,
			ContentKey: blob.Key[:],
		}
		if err := db.AddEntry(store.emit, e); err != nil {
			t.Fatal(err)
		}
	}
	root, err := db.Finish(store.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.DirNode {
		t.Fatalf("fixture root = %v, want a DirNode (raise n?)", root.Type())
	}
	return root
}

func TestLookupEntry_BigDir(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)

	for _, name := range []string{"e00000", "e00001", "e00499", "e00998", "e00999"} {
		ent, err := fstree.LookupEntry(root, []byte(name), store.get)
		if err != nil {
			t.Fatalf("LookupEntry(%s): %v", name, err)
		}
		if string(ent.Name) != name {
			t.Fatalf("LookupEntry(%s).Name = %q", name, ent.Name)
		}
	}
	for _, name := range []string{"", "a", "e004995", "e00999x", "zzz"} {
		if _, err := fstree.LookupEntry(root, []byte(name), store.get); !errors.Is(err, fstree.ErrNotFound) {
			t.Fatalf("LookupEntry(%q) err = %v, want ErrNotFound", name, err)
		}
	}
}

func TestLookupEntry_TouchesFewObjects(t *testing.T) {
	store := memStore{}
	root := bigDir(t, store, 1000)
	reads := 0
	counting := func(k key.Key) ([]byte, error) {
		reads++
		return store.get(k)
	}
	if _, err := fstree.LookupEntry(root, []byte("e00500"), counting); err != nil {
		t.Fatal(err)
	}
	// tree depth for 1000 entries at average run 8 is ~4; anything near
	// O(n) (125+ leaves) means the descent is broken.
	if reads > 8 {
		t.Fatalf("lookup read %d objects, want a logarithmic descent", reads)
	}
}

func TestLookupEntry_SingleLeaf(t *testing.T) {
	store := memStore{}
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("a"), Mode: 0o100644, ContentKey: blob.Key[:]},
		{Name: []byte("m"), Mode: 0o040755, ContentKey: leafSelfKey()},
		{Name: []byte("z"), Mode: 0o120777, LinkTarget: []byte("a")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.emit(leaf); err != nil {
		t.Fatal(err)
	}

	ent, err := fstree.LookupEntry(leaf.Key, []byte("m"), store.get)
	if err != nil {
		t.Fatalf("LookupEntry(m): %v", err)
	}
	if string(ent.Name) != "m" {
		t.Fatalf("LookupEntry(m).Name = %q", ent.Name)
	}
	if _, err := fstree.LookupEntry(leaf.Key, []byte("b"), store.get); !errors.Is(err, fstree.ErrNotFound) {
		t.Fatalf("LookupEntry(b) err = %v, want ErrNotFound", err)
	}
}

// leafSelfKey returns some canonical key bytes for fixture entries whose
// content is never fetched.
func leafSelfKey() []byte {
	o, err := fstree.EncodeDirLeaf(nil)
	if err != nil {
		panic(err)
	}
	return o.Key[:]
}

func TestLookupEntry_RejectsNonDir(t *testing.T) {
	store := memStore{}
	blob, err := fstree.EncodeBlob([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.emit(blob); err != nil {
		t.Fatal(err)
	}
	if _, err := fstree.LookupEntry(blob.Key, []byte("a"), store.get); err == nil {
		t.Fatal("expected error for a non-directory key")
	}
}
