package fstree

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/draganm/rebma/key"
)

// LookupEntry returns the entry called name in the directory object dir. It
// descends DirNode levels by binary search over each pair's SepName (the
// greatest entry name in that child's subtree), then scans the one DirLeaf
// that could hold the name — O(log n) objects for an n-entry directory. A
// missing name wraps ErrNotFound; get fetches the bytes stored under a key.
// dir may be the key of a Commit, which stands for its tree (DirOf).
func LookupEntry(dir key.Key, name []byte, get func(key.Key) ([]byte, error)) (Entry, error) {
	k, err := DirOf(dir, get) // a commit stands for its tree: here, and nowhere further down
	if err != nil {
		return Entry{}, err
	}
	for {
		data, err := get(k)
		if err != nil {
			return Entry{}, fmt.Errorf("fstree: reading %s: %w", k, err)
		}
		switch k.Type() {
		case key.DirLeaf:
			// A leaf in the form the encoder writes is answered by one
			// pass over its bytes (scan.go). Any other body is left to the
			// decoder below.
			switch e, res := scanLeafEntry(data, name); res {
			case scanFound:
				return e, nil
			case scanMissing:
				return Entry{}, fmt.Errorf("fstree: %q: %w", name, ErrNotFound)
			}
			entries, err := DecodeDirLeaf(data)
			if err != nil {
				return Entry{}, fmt.Errorf("fstree: decoding DirLeaf %s: %w", k, err)
			}
			i := sort.Search(len(entries), func(i int) bool {
				return bytes.Compare(entries[i].Name, name) >= 0
			})
			if i < len(entries) && bytes.Equal(entries[i].Name, name) {
				return entries[i], nil
			}
			return Entry{}, fmt.Errorf("fstree: %q: %w", name, ErrNotFound)
		case key.DirNode:
			switch child, res := scanNode(data, name); res {
			case scanFound:
				ck, err := key.Parse(child)
				if err != nil {
					return Entry{}, fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
				}
				k = ck
				continue
			case scanMissing:
				return Entry{}, fmt.Errorf("fstree: %q: %w", name, ErrNotFound)
			}
			pairs, err := DecodeDirNode(data)
			if err != nil {
				return Entry{}, fmt.Errorf("fstree: decoding DirNode %s: %w", k, err)
			}
			// The first pair whose SepName >= name roots the only subtree
			// that can contain name.
			i := sort.Search(len(pairs), func(i int) bool {
				return bytes.Compare(pairs[i].SepName, name) >= 0
			})
			if i == len(pairs) {
				return Entry{}, fmt.Errorf("fstree: %q: %w", name, ErrNotFound)
			}
			ck, err := key.Parse(pairs[i].ChildKey)
			if err != nil {
				return Entry{}, fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
			}
			k = ck
		default:
			return Entry{}, fmt.Errorf("fstree: %s is not a directory object (type %v)", k, k.Type())
		}
	}
}
