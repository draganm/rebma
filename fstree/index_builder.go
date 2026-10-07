package fstree

import (
	"errors"

	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/key"
)

// IndexBuilder builds the index levels above a leaf level by streaming child
// references and item-chunking them into FileNode (files) or DirNode (dirs)
// objects, level by level, until one object remains (the root). Objects are
// emitted children-before-parents; the root is emitted last. Memory is
// O(levels x MaxRun) — it never holds a whole level's bytes.
type IndexBuilder struct {
	ic     chunkers.ItemChunker
	isDir  bool
	levels []*idxLevel
}

type idxLevel struct {
	keys   []key.Key
	seps   [][]byte // populated only for directory indexes
	runLen int
}

// NewFileIndexBuilder builds FileNode levels over Blob/FileNode child keys.
func NewFileIndexBuilder(ic chunkers.ItemChunker) *IndexBuilder {
	return &IndexBuilder{ic: ic, isDir: false}
}

// newDirIndexBuilder builds DirNode levels over DirLeaf/DirNode child keys, each
// carrying the greatest entry name (sepName) in its subtree.
func newDirIndexBuilder(ic chunkers.ItemChunker) *IndexBuilder {
	return &IndexBuilder{ic: ic, isDir: true}
}

func (ib *IndexBuilder) ensure(l int) *idxLevel {
	for len(ib.levels) <= l {
		ib.levels = append(ib.levels, &idxLevel{})
	}
	return ib.levels[l]
}

// hasAny reports whether any child has been added (level 0 exists).
func (ib *IndexBuilder) hasAny() bool { return len(ib.levels) > 0 }

// AddChild adds one leaf-level child reference. sep is the child subtree's
// greatest entry name for directory indexes, and is ignored for file indexes.
func (ib *IndexBuilder) AddChild(emit Emit, childKey key.Key, sep []byte) error {
	return ib.add(emit, 0, childKey, sep)
}

func (ib *IndexBuilder) add(emit Emit, l int, ck key.Key, sep []byte) error {
	L := ib.ensure(l)
	L.keys = append(L.keys, ck)
	if ib.isDir {
		L.seps = append(L.seps, sep)
	}
	L.runLen++
	enc, err := ib.itemEnc(ck, sep)
	if err != nil {
		return err
	}
	if ib.ic.IsBoundary(enc, L.runLen) {
		return ib.closeLevel(emit, l)
	}
	return nil
}

func (ib *IndexBuilder) itemEnc(ck key.Key, sep []byte) ([]byte, error) {
	if !ib.isDir {
		return ck[:], nil
	}
	return encMode.Marshal(DirPair{SepName: sep, ChildKey: ck[:]})
}

func (ib *IndexBuilder) closeLevel(emit Emit, l int) error {
	obj, sep, err := ib.buildNode(ib.levels[l])
	if err != nil {
		return err
	}
	ib.reset(l)
	if err := emit(obj); err != nil {
		return err
	}
	return ib.add(emit, l+1, obj.Key, sep)
}

func (ib *IndexBuilder) reset(l int) {
	L := ib.levels[l]
	L.keys = nil
	L.seps = nil
	L.runLen = 0
}

// buildNode builds the index object for a level's current run, returning the
// object and the run's greatest sepName (nil for file indexes).
func (ib *IndexBuilder) buildNode(L *idxLevel) (Object, []byte, error) {
	if ib.isDir {
		pairs := make([]DirPair, len(L.keys))
		for i := range L.keys {
			pairs[i] = DirPair{SepName: L.seps[i], ChildKey: L.keys[i][:]}
		}
		obj, err := EncodeDirNode(pairs)
		return obj, L.seps[len(L.seps)-1], err
	}
	obj, err := EncodeFileNode(L.keys)
	return obj, nil, err
}

// Finish collapses all open runs bottom-up and returns the root key. The single
// object that reaches the top with no parent is the root; a single child is
// returned without wrapping it in a degenerate one-child node.
func (ib *IndexBuilder) Finish(emit Emit) (key.Key, error) {
	for l := 0; l < len(ib.levels); l++ {
		L := ib.levels[l]
		isTop := l == len(ib.levels)-1
		switch {
		case L.runLen == 0:
			continue
		case L.runLen == 1:
			ck := L.keys[0]
			var sep []byte
			if ib.isDir {
				sep = L.seps[0]
			}
			ib.reset(l)
			if isTop {
				return ck, nil // already emitted when created
			}
			if err := ib.add(emit, l+1, ck, sep); err != nil {
				return key.Key{}, err
			}
		default:
			obj, sep, err := ib.buildNode(L)
			if err != nil {
				return key.Key{}, err
			}
			ib.reset(l)
			if err := emit(obj); err != nil {
				return key.Key{}, err
			}
			if err := ib.add(emit, l+1, obj.Key, sep); err != nil {
				return key.Key{}, err
			}
		}
	}
	return key.Key{}, errors.New("fstree: IndexBuilder.Finish with no children")
}
