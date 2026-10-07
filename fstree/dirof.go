package fstree

import (
	"fmt"

	"github.com/draganm/rebma/commit"
	"github.com/draganm/rebma/key"
)

// A Commit key may stand where a directory's key is expected: as the key
// handed to a reader, and as the content key of an S_IFDIR directory entry.
// It reads as the directory the commit records — its tree; for a conflicted
// commit, the first side. LookupEntry, ListEntries and CollectEntries take
// one (through DirOf), and so does everything built on them. A commit's tree
// is never a commit, so one step always suffices. Inside a directory's own
// index, as the child of a DirNode, a commit is a malformed tree, and the
// readers reject it like any other key that is not a directory's.

// DirOf returns the directory object k stands for: k itself when it is a
// DirLeaf or DirNode, the tree of the commit when it is a Commit. get fetches
// the bytes stored under a key.
func DirOf(k key.Key, get func(key.Key) ([]byte, error)) (key.Key, error) {
	switch k.Type() {
	case key.DirLeaf, key.DirNode:
		return k, nil
	case key.Commit:
		data, err := get(k)
		if err != nil {
			return key.Key{}, fmt.Errorf("fstree: reading %s: %w", k, err)
		}
		c, err := decodeCommit(k, data)
		if err != nil {
			return key.Key{}, err
		}
		return c.Tree, nil
	default:
		return key.Key{}, fmt.Errorf("fstree: %s is not a directory object (type %v)", k, k.Type())
	}
}

// decodeCommit decodes the commit stored under k and holds k to the key
// rule: its length field is the commit's footprint, its own bytes plus its
// trees. The store verifies the same on its checked paths, and so on every
// record a gc pass copies; its plain Put trusts the caller. Checking here
// keeps a reference from being put on a commit — or on history built on one —
// that a later gc pass would refuse as corrupt. The first release keyed
// commits by their own bytes alone; those are what this finds in practice.
func decodeCommit(k key.Key, data []byte) (commit.Commit, error) {
	c, err := commit.Decode(data)
	if err != nil {
		return commit.Commit{}, fmt.Errorf("fstree: decoding Commit %s: %w", k, err)
	}
	want, err := commit.Footprint(uint64(len(data)), c.Trees())
	if err != nil {
		return commit.Commit{}, fmt.Errorf("fstree: Commit %s: %w", k, err)
	}
	if k.Length() != want {
		return commit.Commit{}, fmt.Errorf("fstree: Commit %s: length field %d is not the commit's footprint %d (its own %d bytes plus its trees); a commit keyed by an older rule has to be created again", k, k.Length(), want, len(data))
	}
	return c, nil
}
