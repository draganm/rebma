package fstree_test

import (
	"testing"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
)

func TestChildKeysBlobHasNone(t *testing.T) {
	o, err := fstree.EncodeBlob([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	kids, err := fstree.ChildKeys(o.Key, o.Bytes)
	if err != nil || len(kids) != 0 {
		t.Fatalf("blob children = %v, %v; want none", kids, err)
	}
}

func TestChildKeysFileNode(t *testing.T) {
	b1, err := fstree.EncodeBlob([]byte("chunk one"))
	if err != nil {
		t.Fatal(err)
	}
	b2, err := fstree.EncodeBlob([]byte("chunk two"))
	if err != nil {
		t.Fatal(err)
	}
	fn, err := fstree.EncodeFileNode([]key.Key{b1.Key, b2.Key})
	if err != nil {
		t.Fatal(err)
	}
	kids, err := fstree.ChildKeys(fn.Key, fn.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 2 || kids[0] != b1.Key || kids[1] != b2.Key {
		t.Fatalf("FileNode children = %v", kids)
	}
}

func TestChildKeysDirLeaf(t *testing.T) {
	blob, err := fstree.EncodeBlob([]byte("file content"))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{{
		Name:       []byte("f"),
		Mode:       0o100644,
		ContentKey: blob.Key[:],
	}})
	if err != nil {
		t.Fatal(err)
	}
	kids, err := fstree.ChildKeys(leaf.Key, leaf.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(kids) != 1 || kids[0] != blob.Key {
		t.Fatalf("DirLeaf children = %v", kids)
	}
}
