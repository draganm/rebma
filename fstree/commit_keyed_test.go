package fstree_test

import (
	"testing"

	"github.com/draganm/rebma/commit"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
)

// storing returns a function that puts a built object into store, and one
// that tells whether a key is there.
func storing(t *testing.T, store memStore) (put func(fstree.Object, error) fstree.Object, has func(key.Key) (bool, error)) {
	put = func(o fstree.Object, err error) fstree.Object {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		store[o.Key] = o.Bytes
		return o
	}
	has = func(k key.Key) (bool, error) {
		_, ok := store[k]
		return ok, nil
	}
	return put, has
}

// A commit's key carries its footprint. Bytes stored under another length —
// the first release's rule was the commit's own bytes — are not a commit the
// walks and readers accept: a reference could otherwise be put on history
// that the store's verification, and with it every gc copy, rejects.
func TestCommitKeyedWithoutItsFootprintIsRejected(t *testing.T) {
	store := memStore{}
	put, has := storing(t, store)
	blob := put(fstree.EncodeBlob([]byte("content")))
	tree := put(fstree.EncodeDirLeaf([]fstree.Entry{{Name: []byte("a"), Mode: 0o100644, ContentKey: blob.Key[:]}}))
	id := commit.Identity{Name: "Ann", When: 1}
	good, data, err := commit.Commit{Tree: tree.Key, Author: id, Committer: id, Message: "m"}.Object()
	if err != nil {
		t.Fatal(err)
	}
	old, err := key.New(key.Commit, uint64(len(data)), data) // own bytes only
	if err != nil {
		t.Fatal(err)
	}
	store[good], store[old] = data, data

	if _, err := fstree.ChildKeys(good, data); err != nil {
		t.Fatalf("ChildKeys of an honest commit: %v", err)
	}
	if _, err := fstree.CheckComplete(good, store.get, has, 1); err != nil {
		t.Fatalf("CheckComplete of an honest commit: %v", err)
	}
	if _, err := fstree.ChildKeys(old, data); err == nil {
		t.Error("ChildKeys accepted a commit key whose length is not the footprint")
	}
	if _, err := fstree.DirOf(old, store.get); err == nil {
		t.Error("DirOf accepted it")
	}
	if _, err := fstree.CollectEntries(old, store.get); err == nil {
		t.Error("CollectEntries accepted it")
	}
	if _, err := fstree.CheckComplete(old, store.get, has, 1); err == nil {
		t.Error("CheckComplete accepted it: a reference could be put on it")
	}
}

// A commit stands for a directory where a directory's key is expected: handed
// to a reader, or in an S_IFDIR entry. Inside a directory's own index, as the
// child of a DirNode, it is a malformed tree.
func TestCommitAsADirNodeChildIsRejected(t *testing.T) {
	store := memStore{}
	put, _ := storing(t, store)
	blob := put(fstree.EncodeBlob([]byte("content")))
	tree := put(fstree.EncodeDirLeaf([]fstree.Entry{{Name: []byte("a"), Mode: 0o100644, ContentKey: blob.Key[:]}}))
	id := commit.Identity{Name: "Ann", When: 1}
	ck, data, err := commit.Commit{Tree: tree.Key, Author: id, Committer: id, Message: "m"}.Object()
	if err != nil {
		t.Fatal(err)
	}
	store[ck] = data
	node := put(fstree.EncodeDirNode([]fstree.DirPair{{SepName: []byte("a"), ChildKey: ck[:]}}))

	if e, err := fstree.LookupEntry(node.Key, []byte("a"), store.get); err == nil {
		t.Errorf("LookupEntry read through a commit inside a directory's index: %q", e.Name)
	}
	if es, _, err := fstree.ListEntries(node.Key, nil, 10, store.get); err == nil {
		t.Errorf("ListEntries read through it: %d entries", len(es))
	}
	if es, err := fstree.CollectEntries(node.Key, store.get); err == nil {
		t.Errorf("CollectEntries read through it: %d entries", len(es))
	}
	// As a root it still reads as its tree.
	if es, err := fstree.CollectEntries(ck, store.get); err != nil || len(es) != 1 {
		t.Errorf("CollectEntries(commit) = %d entries, %v", len(es), err)
	}
}
