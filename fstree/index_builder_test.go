package fstree

import (
	"testing"

	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/key"
)

// collector records emitted objects and is the test's Emit.
type collector struct{ objs []Object }

func (c *collector) emit(o Object) error { c.objs = append(c.objs, o); return nil }

func blobKey(t *testing.T, n int) key.Key {
	t.Helper()
	o, err := EncodeBlob(make([]byte, n))
	if err != nil {
		t.Fatal(err)
	}
	return o.Key
}

func TestFileIndex_SingleChildIsRootNoNode(t *testing.T) {
	c := &collector{}
	ib := NewFileIndexBuilder(chunkers.NewItemChunker(7))
	k := blobKey(t, 10)
	if err := ib.AddChild(c.emit, k, nil); err != nil {
		t.Fatal(err)
	}
	root, err := ib.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root != k {
		t.Errorf("root = %s, want the single blob %s", root, k)
	}
	if len(c.objs) != 0 {
		t.Errorf("no FileNode should be emitted for a single child, got %d objects", len(c.objs))
	}
}

func TestFileIndex_MultipleChildrenProduceFileNodeRoot(t *testing.T) {
	c := &collector{}
	ib := NewFileIndexBuilder(chunkers.NewItemChunker(7))
	var sum uint64
	for _, n := range []int{100, 200, 300} {
		k := blobKey(t, n)
		sum += k.Length()
		if err := ib.AddChild(c.emit, k, nil); err != nil {
			t.Fatal(err)
		}
	}
	root, err := ib.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.FileNode {
		t.Errorf("root type = %v, want FileNode", root.Type())
	}
	if root.Length() != sum {
		t.Errorf("root length = %d, want %d", root.Length(), sum)
	}
	// The root must be the LAST emitted object.
	if len(c.objs) == 0 || c.objs[len(c.objs)-1].Key != root {
		t.Errorf("root must be the last emitted object")
	}
}

func TestFileIndex_ManyChildrenMultiLevel(t *testing.T) {
	c := &collector{}
	ib := NewFileIndexBuilder(chunkers.NewItemChunker(2)) // tiny avg → forces multiple levels
	for i := range 2000 {
		if err := ib.AddChild(c.emit, blobKey(t, i+1), nil); err != nil {
			t.Fatal(err)
		}
	}
	root, err := ib.Finish(c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if root.Type() != key.FileNode {
		t.Errorf("root type = %v, want FileNode", root.Type())
	}
	if c.objs[len(c.objs)-1].Key != root {
		t.Errorf("root must be the last emitted object")
	}
}
