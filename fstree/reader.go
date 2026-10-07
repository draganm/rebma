package fstree

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"

	"github.com/draganm/rebma/key"
)

// DefaultDirectoryReaderCapacity is the capacity of a DirectoryReader made
// without WithCapacity, in directory objects.
const DefaultDirectoryReaderCapacity = 4096

// DirectoryReader answers LookupEntry, ResolveEntry and ResolvePath as the
// functions of those names do, with the same results and the same errors, and
// keeps each directory object it decodes. The functions decode every
// directory object on the path at each call; a reader decodes it once, so it
// suits a walk of many paths in one tree. Its getter must return the same
// content for a key every time, as for LookupEntry.
//
// A reader holds at most its capacity of decoded directory objects; when
// full, it drops the least recently used half, so most misses pay nothing
// for the bound. It also holds at most its capacity of commit-to-tree answers
// (DirOf), all dropped when full. A hit does not read the object again, so
// the reader can answer for objects that gc removed after it decoded them:
// keep one for a bounded operation.
//
// A DirectoryReader is not safe for concurrent use: every call changes what
// it holds, a hit too, and it takes no lock. Give each goroutine its own, or
// guard a shared one with a mutex. The entries it returns are the caller's,
// as the functions' are: they share no memory with what the reader holds.
type DirectoryReader struct {
	get         func(key.Key) ([]byte, error)
	capacity    int
	clock       uint64 // moves at every use of a directory, to order them by recency
	directories map[key.Key]*decodedDirectory
	commits     map[key.Key]key.Key // a commit's tree
}

// decodedDirectory is a directory object as a DirectoryReader holds it.
type decodedDirectory struct {
	entries []Entry   // of a DirLeaf
	pairs   []DirPair // of a DirNode
	used    uint64    // the reader's clock at the last use
}

// DirectoryReaderOption configures a DirectoryReader as it is made.
type DirectoryReaderOption func(*DirectoryReader)

// WithCapacity sets how many decoded directory objects a DirectoryReader
// holds, and how many commit-to-tree answers. Less than 1 is taken as 1.
func WithCapacity(n int) DirectoryReaderOption {
	return func(r *DirectoryReader) { r.capacity = max(n, 1) }
}

// NewDirectoryReader returns a DirectoryReader that fetches the bytes stored
// under a key with get. It holds DefaultDirectoryReaderCapacity directory
// objects unless WithCapacity says otherwise.
func NewDirectoryReader(get func(key.Key) ([]byte, error), opts ...DirectoryReaderOption) *DirectoryReader {
	r := &DirectoryReader{
		get:         get,
		capacity:    DefaultDirectoryReaderCapacity,
		directories: map[key.Key]*decodedDirectory{},
		commits:     map[key.Key]key.Key{},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// LookupEntry returns the entry called name in the directory object dir, as
// the function LookupEntry does. dir may be the key of a Commit.
func (r *DirectoryReader) LookupEntry(dir key.Key, name []byte) (Entry, error) {
	e, err := r.lookup(dir, name)
	if err != nil {
		return Entry{}, err
	}
	return cloneEntry(e), nil
}

// ResolvePath descends from the directory object root along the
// slash-separated path and returns the content key of the directory entry it
// names, as the function ResolvePath does.
func (r *DirectoryReader) ResolvePath(root key.Key, path string) (key.Key, error) {
	return walkPath(root, path, r.lookup)
}

// ResolveEntry descends from the directory object root along the
// slash-separated path and returns the entry the final component names, nil
// for the root itself, as the function ResolveEntry does.
func (r *DirectoryReader) ResolveEntry(root key.Key, path string) (*Entry, error) {
	e, err := walkEntry(root, path, r.lookup)
	if err != nil || e == nil {
		return nil, err
	}
	*e = cloneEntry(*e) // e is the walk's own Entry; its slices were still the reader's
	return e, nil
}

// WriteContent writes the regular-file content addressed by k to w, as the
// function WriteContent does, reading through the reader's getter. The reader
// keeps none of it.
func (r *DirectoryReader) WriteContent(w io.Writer, k key.Key) error {
	return WriteContent(w, k, r.get)
}

// lookup is LookupEntry without the copy: the entry it returns shares its
// slices with what the reader holds, so it is not to be changed or handed
// out.
func (r *DirectoryReader) lookup(dir key.Key, name []byte) (Entry, error) {
	k, err := r.dirOf(dir) // a commit stands for its tree: here, and nowhere further down
	if err != nil {
		return Entry{}, err
	}
	for {
		d, err := r.directory(k)
		if err != nil {
			return Entry{}, err
		}
		if k.Type() == key.DirLeaf {
			i := sort.Search(len(d.entries), func(i int) bool {
				return bytes.Compare(d.entries[i].Name, name) >= 0
			})
			if i < len(d.entries) && bytes.Equal(d.entries[i].Name, name) {
				return d.entries[i], nil
			}
			return Entry{}, fmt.Errorf("fstree: %q: %w", name, ErrNotFound)
		}
		// A DirNode: the first pair whose SepName >= name roots the only
		// subtree that can contain name.
		i := sort.Search(len(d.pairs), func(i int) bool {
			return bytes.Compare(d.pairs[i].SepName, name) >= 0
		})
		if i == len(d.pairs) {
			return Entry{}, fmt.Errorf("fstree: %q: %w", name, ErrNotFound)
		}
		ck, err := key.Parse(d.pairs[i].ChildKey)
		if err != nil {
			return Entry{}, fmt.Errorf("fstree: child key in DirNode %s: %w", k, err)
		}
		k = ck
	}
}

// dirOf is DirOf, remembering the tree of each commit.
func (r *DirectoryReader) dirOf(k key.Key) (key.Key, error) {
	if k.Type() != key.Commit {
		return DirOf(k, r.get)
	}
	if tree, ok := r.commits[k]; ok {
		return tree, nil
	}
	tree, err := DirOf(k, r.get)
	if err != nil {
		return key.Key{}, err
	}
	if len(r.commits) >= r.capacity {
		clear(r.commits)
	}
	r.commits[k] = tree
	return tree, nil
}

// directory returns the decoded directory object k, a DirLeaf or a DirNode:
// the one the reader holds, or else read, decoded and held from here on. An
// object that cannot be read or decoded is not held.
func (r *DirectoryReader) directory(k key.Key) (*decodedDirectory, error) {
	r.clock++
	if d, ok := r.directories[k]; ok {
		d.used = r.clock
		return d, nil
	}
	data, err := r.get(k)
	if err != nil {
		return nil, fmt.Errorf("fstree: reading %s: %w", k, err)
	}
	d := &decodedDirectory{used: r.clock}
	switch k.Type() {
	case key.DirLeaf:
		if d.entries, err = DecodeDirLeaf(data); err != nil {
			return nil, fmt.Errorf("fstree: decoding DirLeaf %s: %w", k, err)
		}
	case key.DirNode:
		if d.pairs, err = DecodeDirNode(data); err != nil {
			return nil, fmt.Errorf("fstree: decoding DirNode %s: %w", k, err)
		}
	default:
		return nil, fmt.Errorf("fstree: %s is not a directory object (type %v)", k, k.Type())
	}
	if len(r.directories) >= r.capacity {
		r.evict()
	}
	r.directories[k] = d
	return d, nil
}

// evict drops the least recently used directories, keeping half the capacity
// of them: the most recently used.
func (r *DirectoryReader) evict() {
	keep := r.capacity / 2
	if keep == 0 {
		clear(r.directories)
		return
	}
	used := make([]uint64, 0, len(r.directories))
	for _, d := range r.directories {
		used = append(used, d.used)
	}
	slices.Sort(used)
	oldestKept := used[len(used)-keep] // no two are equal: the clock moves at every use
	maps.DeleteFunc(r.directories, func(_ key.Key, d *decodedDirectory) bool {
		return d.used < oldestKept
	})
}

// cloneEntry returns e with slices of its own.
func cloneEntry(e Entry) Entry {
	e.Name = slices.Clone(e.Name)
	e.ContentKey = slices.Clone(e.ContentKey)
	e.LinkTarget = slices.Clone(e.LinkTarget)
	e.Rdev = slices.Clone(e.Rdev)
	e.XattrsIn = slices.Clone(e.XattrsIn)
	e.XattrsKey = slices.Clone(e.XattrsKey)
	return e
}
