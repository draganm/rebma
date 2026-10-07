package fstree

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/draganm/rebma/key"
)

// ListEntries returns up to limit entries of the directory object dir whose
// names sort strictly after `after` (nil or empty lists from the start), in
// name order, and whether more such entries follow. When more is true the
// returned page is full: len(entries) == limit. It descends only the
// subtrees that can hold qualifying names — a DirNode pair is skipped when
// its SepName (the subtree's greatest name) is not after `after` — touching
// O(log n + limit) objects. limit must be positive.
func ListEntries(dir key.Key, after []byte, limit int, get func(key.Key) ([]byte, error)) ([]Entry, bool, error) {
	if limit <= 0 {
		return nil, false, fmt.Errorf("fstree: ListEntries limit must be positive, got %d", limit)
	}
	dir, err := DirOf(dir, get) // a commit stands for its tree: here, and nowhere further down
	if err != nil {
		return nil, false, err
	}
	var out []Entry
	more, err := listInto(&out, dir, after, limit, get)
	if err != nil {
		return nil, false, err
	}
	return out, more, nil
}

// listInto appends qualifying entries of the subtree at k to out until out
// holds limit entries; it reports true the moment a qualifying entry beyond
// the limit exists.
func listInto(out *[]Entry, k key.Key, after []byte, limit int, get func(key.Key) ([]byte, error)) (bool, error) {
	data, err := get(k)
	if err != nil {
		return false, fmt.Errorf("fstree: reading %s: %w", k, err)
	}
	switch k.Type() {
	case key.DirLeaf:
		entries, err := DecodeDirLeaf(data)
		if err != nil {
			return false, fmt.Errorf("fstree: decoding DirLeaf %s: %w", k, err)
		}
		i := sort.Search(len(entries), func(i int) bool {
			return bytes.Compare(entries[i].Name, after) > 0
		})
		for ; i < len(entries); i++ {
			if len(*out) == limit {
				return true, nil
			}
			*out = append(*out, entries[i])
		}
		return false, nil
	case key.DirNode:
		pairs, err := DecodeDirNode(data)
		if err != nil {
			return false, fmt.Errorf("fstree: decoding DirNode %s: %w", k, err)
		}
		i := sort.Search(len(pairs), func(i int) bool {
			return bytes.Compare(pairs[i].SepName, after) > 0
		})
		for ; i < len(pairs); i++ {
			ck, err := key.Parse(pairs[i].ChildKey)
			if err != nil {
				return false, fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
			}
			more, err := listInto(out, ck, after, limit, get)
			if err != nil {
				return false, err
			}
			if more {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("fstree: %s is not a directory object (type %v)", k, k.Type())
	}
}
