package fstree

import (
	"fmt"

	"github.com/draganm/rebma/key"
)

// ChildKeys returns the keys directly referenced by the object with key k
// and serialized bytes data, in encounter order. Blob and XattrSet objects
// are leaves and have no children. A Commit's children are its tree, the
// further terms of a conflicted tree, then its parents, all in recorded order
// — so every walk built on ChildKeys follows history and keeps every side of
// a conflict. A DirLeaf's children are its entries' content keys whatever
// their type, so a directory entry that holds a Commit is followed too.
func ChildKeys(k key.Key, data []byte) ([]key.Key, error) {
	switch k.Type() {
	case key.Blob, key.XattrSet:
		return nil, nil
	case key.FileNode:
		children, err := DecodeFileNode(data)
		if err != nil {
			return nil, fmt.Errorf("fstree: decoding FileNode %s: %w", k, err)
		}
		return children, nil
	case key.DirNode:
		pairs, err := DecodeDirNode(data)
		if err != nil {
			return nil, fmt.Errorf("fstree: decoding DirNode %s: %w", k, err)
		}
		out := make([]key.Key, 0, len(pairs))
		for _, p := range pairs {
			ck, err := key.Parse(p.ChildKey)
			if err != nil {
				return nil, fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
			}
			out = append(out, ck)
		}
		return out, nil
	case key.DirLeaf:
		entries, err := DecodeDirLeaf(data)
		if err != nil {
			return nil, fmt.Errorf("fstree: decoding DirLeaf %s: %w", k, err)
		}
		var out []key.Key
		for _, ent := range entries {
			if len(ent.ContentKey) > 0 {
				ck, err := key.Parse(ent.ContentKey)
				if err != nil {
					return nil, fmt.Errorf("fstree: %q: content key: %w", ent.Name, err)
				}
				out = append(out, ck)
			}
			if len(ent.XattrsKey) > 0 {
				xk, err := key.Parse(ent.XattrsKey)
				if err != nil {
					return nil, fmt.Errorf("fstree: %q: xattrs key: %w", ent.Name, err)
				}
				out = append(out, xk)
			}
		}
		return out, nil
	case key.Commit:
		c, err := decodeCommit(k, data) // and hold k to the key rule: see there
		if err != nil {
			return nil, err
		}
		trees := c.Trees()
		out := make([]key.Key, 0, len(trees)+len(c.Parents))
		out = append(out, trees...)
		return append(out, c.Parents...), nil
	default:
		return nil, fmt.Errorf("fstree: unknown object type %s", k.Type())
	}
}
