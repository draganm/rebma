// Package fstree encodes the Amber-Store filesystem tree objects (FileNode,
// DirLeaf, DirNode, XattrSet, Blob) as deterministic CBOR per
// architecture/fstree.md, and builds files and directories bottom-up by
// streaming. See architecture/types.md for the length-field semantics.
//
// The object-graph walks (ChildKeys, ReachableKeys, CheckComplete) also follow
// Commit objects (package commit), whose children are their trees — one, or
// every side of a conflict — and their parent commits. The directory readers
// take a commit for the directory it records (dirof.go).
package fstree

import "github.com/draganm/rebma/key"

// Object is a built CAS object: its key and its serialized bytes.
type Object struct {
	Key   key.Key
	Bytes []byte
}

// Emit is called once per built object, in children-before-parents order. The
// consumer (the pack driver) is responsible for writing the object to the sink.
type Emit func(Object) error
