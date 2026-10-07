package fstree_test

import (
	"fmt"
	"testing"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
)

// TestReachableKeys walks a tree exercising every object type — a DirNode root,
// DirLeaves, a multi-chunk file (FileNode over Blobs), a spilled XattrSet — and
// a blob referenced twice to check deduplication.
func TestReachableKeys(t *testing.T) {
	blobA, err := fstree.EncodeBlob([]byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	blobB, err := fstree.EncodeBlob([]byte("beta"))
	if err != nil {
		t.Fatal(err)
	}
	fileNode, err := fstree.EncodeFileNode([]key.Key{blobA.Key, blobB.Key})
	if err != nil {
		t.Fatal(err)
	}
	xattrs, err := fstree.EncodeXattrSet(map[string][]byte{"user.comment": []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	subLeaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("big"), Mode: 0o100644, ContentKey: fileNode.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	// blobA is referenced both here and as a FileNode child; it must be listed once.
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("a.txt"), Mode: 0o100644, ContentKey: blobA.Key[:], XattrsKey: xattrs.Key[:]},
		{Name: []byte("link"), Mode: 0o120777, LinkTarget: []byte("a.txt")},
		{Name: []byte("sub"), Mode: 0o040755, ContentKey: subLeaf.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	root, err := fstree.EncodeDirNode([]fstree.DirPair{
		{SepName: []byte("a.txt"), ChildKey: leaf.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	get := mapGetter(blobA, blobB, fileNode, xattrs, subLeaf, leaf, root)

	got, err := fstree.ReachableKeys(root.Key, get)
	if err != nil {
		t.Fatalf("ReachableKeys: %v", err)
	}

	want := map[key.Key]bool{
		root.Key:     true,
		leaf.Key:     true,
		blobA.Key:    true,
		xattrs.Key:   true,
		subLeaf.Key:  true,
		fileNode.Key: true,
		blobB.Key:    true,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d: %v", len(got), len(want), got)
	}
	if got[0] != root.Key {
		t.Errorf("first key = %s, want the root %s", got[0], root.Key)
	}
	for _, k := range got {
		if !want[k] {
			t.Errorf("unexpected key %s (type %v)", k, k.Type())
		}
		delete(want, k)
	}
	for k := range want {
		t.Errorf("missing key %s (type %v)", k, k.Type())
	}
}

// TestReachableKeys_FileRoot walks a bare file object: the root may be any
// object type, not only a directory.
func TestReachableKeys_FileRoot(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	fileNode, err := fstree.EncodeFileNode([]key.Key{blob.Key})
	if err != nil {
		t.Fatal(err)
	}
	got, err := fstree.ReachableKeys(fileNode.Key, mapGetter(blob, fileNode))
	if err != nil {
		t.Fatalf("ReachableKeys: %v", err)
	}
	if len(got) != 2 || got[0] != fileNode.Key || got[1] != blob.Key {
		t.Fatalf("keys = %v, want [fileNode, blob]", got)
	}
}

// TestReachableKeys_MissingObject surfaces a get failure for an absent child.
func TestReachableKeys_MissingObject(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	missing, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("x"), Mode: 0o100644, ContentKey: blob.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("gone"), Mode: 0o040755, ContentKey: missing.Key[:]},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The store holds only the parent: walking into "gone" must fail.
	if _, err := fstree.ReachableKeys(parent.Key, mapGetter(parent)); err == nil {
		t.Fatal("expected error for a missing child object")
	}
}

// TestReachableKeys_WideParallel walks a root with many subdirectories, forcing
// a wide frontier so the concurrent fetch path runs many workers at once (run
// under -race to guard the walk). Every subdirectory also references one shared
// blob, so the round that fetches the sub-leaves concurrently discovers that
// blob many times — confirming the dedup that follows the parallel round lists
// it exactly once. Asserts root-first, the full set, and that every key is unique.
func TestReachableKeys_WideParallel(t *testing.T) {
	const n = 64
	var objs []fstree.Object
	add := func(o fstree.Object) { objs = append(objs, o) }

	// A blob shared by every subdirectory: discovered concurrently from many
	// parallel fetches in the same round, it must still be listed once.
	shared, err := fstree.EncodeBlob([]byte("shared"))
	if err != nil {
		t.Fatal(err)
	}
	add(shared)

	var entries []fstree.Entry
	for i := 0; i < n; i++ {
		blob, err := fstree.EncodeBlob([]byte(fmt.Sprintf("content-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		add(blob)
		sub, err := fstree.EncodeDirLeaf([]fstree.Entry{
			{Name: []byte("uniq"), Mode: 0o100644, ContentKey: blob.Key[:]},
			{Name: []byte("shared"), Mode: 0o100644, ContentKey: shared.Key[:]},
		})
		if err != nil {
			t.Fatal(err)
		}
		add(sub)
		entries = append(entries, fstree.Entry{
			Name: []byte(fmt.Sprintf("d%02d", i)), Mode: 0o040755, ContentKey: sub.Key[:],
		})
	}
	root, err := fstree.EncodeDirLeaf(entries)
	if err != nil {
		t.Fatal(err)
	}
	add(root)

	got, err := fstree.ReachableKeys(root.Key, mapGetter(objs...))
	if err != nil {
		t.Fatalf("ReachableKeys: %v", err)
	}
	if len(got) == 0 || got[0] != root.Key {
		t.Fatalf("first key = %v, want the root %s", got, root.Key)
	}
	// root + n sub-leaves + n unique blobs + 1 shared blob (deduped from n refs).
	if want := 1 + 2*n + 1; len(got) != want {
		t.Fatalf("got %d keys, want %d", len(got), want)
	}
	seen := map[key.Key]bool{}
	for _, k := range got {
		if seen[k] {
			t.Errorf("duplicate key %s", k)
		}
		seen[k] = true
	}
}
