package fstree

import (
	"fmt"
	"testing"

	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/key"
)

func TestDir_EmptyDirIsSingleEmptyLeaf(t *testing.T) {
	c := &collector{}
	db := NewDirBuilder(chunkers.NewItemChunker(7))
	root, err := db.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.DirLeaf {
		t.Errorf("empty dir root type = %v, want DirLeaf", root.Type())
	}
	if len(c.objs) != 1 || c.objs[0].Key != root {
		t.Errorf("empty dir must emit exactly one DirLeaf that is the root")
	}
	// Empty DirLeaf is the CBOR empty array 0x80.
	if len(c.objs[0].Bytes) != 1 || c.objs[0].Bytes[0] != 0x80 {
		t.Errorf("empty DirLeaf bytes = %x, want 80", c.objs[0].Bytes)
	}
}

func TestDir_SingleEntryIsLeafRoot(t *testing.T) {
	c := &collector{}
	db := NewDirBuilder(chunkers.NewItemChunker(7))
	if err := db.AddEntry(c.emit, Entry{Name: []byte("a"), Mode: 0o100644}); err != nil {
		t.Fatal(err)
	}
	root, err := db.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.DirLeaf {
		t.Errorf("root type = %v, want DirLeaf", root.Type())
	}
	if c.objs[len(c.objs)-1].Key != root {
		t.Errorf("root must be the last emitted object")
	}
}

func TestDir_ManyEntriesProduceDirNodeRootLast(t *testing.T) {
	c := &collector{}
	db := NewDirBuilder(chunkers.NewItemChunker(2)) // tiny avg → multi-level
	for i := range 5000 {
		name := []byte(fmt.Sprintf("%06d", i)) // already sorted
		if err := db.AddEntry(c.emit, Entry{Name: name, Mode: 0o100644}); err != nil {
			t.Fatal(err)
		}
	}
	root, err := db.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.DirNode {
		t.Errorf("root type = %v, want DirNode", root.Type())
	}
	if c.objs[len(c.objs)-1].Key != root {
		t.Errorf("root must be the last emitted object")
	}
	// du-style length must be positive and >= own bytes of the root.
	if root.Length() == 0 {
		t.Errorf("DirNode root length should be non-zero")
	}
}
