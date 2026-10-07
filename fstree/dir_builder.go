package fstree

import (
	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/key"
)

// DirBuilder builds one directory's prolly tree by streaming its entries (which
// the caller supplies already sorted bytewise by name). It chunks entries into
// DirLeaf objects and promotes their keys through a DirNode index. Objects are
// emitted children-before-parents; the directory root is emitted last.
type DirBuilder struct {
	ic      chunkers.ItemChunker
	idx     *IndexBuilder
	leaf    []Entry
	leafMax []byte
	runLen  int
}

// NewDirBuilder returns a DirBuilder using ic for both the entry stream and the
// DirNode index stream.
func NewDirBuilder(ic chunkers.ItemChunker) *DirBuilder {
	return &DirBuilder{ic: ic, idx: newDirIndexBuilder(ic)}
}

// AddEntry appends one directory entry (in sorted order).
func (db *DirBuilder) AddEntry(emit Emit, e Entry) error {
	enc, err := encMode.Marshal(e)
	if err != nil {
		return err
	}
	db.leaf = append(db.leaf, e)
	db.leafMax = e.Name
	db.runLen++
	if db.ic.IsBoundary(enc, db.runLen) {
		return db.closeLeaf(emit)
	}
	return nil
}

func (db *DirBuilder) closeLeaf(emit Emit) error {
	obj, err := EncodeDirLeaf(db.leaf)
	if err != nil {
		return err
	}
	sep := db.leafMax
	db.leaf = nil
	db.leafMax = nil
	db.runLen = 0
	if err := emit(obj); err != nil {
		return err
	}
	return db.idx.AddChild(emit, obj.Key, sep)
}

// Finish closes the trailing leaf run (emitting an empty DirLeaf for an empty
// directory) and returns the directory's root key.
func (db *DirBuilder) Finish(emit Emit) (key.Key, error) {
	if db.runLen > 0 || !db.idx.hasAny() {
		if err := db.closeLeaf(emit); err != nil {
			return key.Key{}, err
		}
	}
	return db.idx.Finish(emit)
}
