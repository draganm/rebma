package fstree

import (
	"fmt"
	"runtime"

	"github.com/draganm/rebma/key"
	"golang.org/x/sync/errgroup"
)

// ReachableKeys returns the keys of every object reachable from root — the set
// that must be transferred to hold the whole content — with root first and the
// remaining keys in unspecified order (callers must not rely on any ordering
// beyond root-first), each key listed once even when referenced repeatedly. root
// may be any object type. get fetches the bytes stored under a key; Blob and
// XattrSet objects are leaves and are not fetched.
//
// The walk is a breadth-first sweep that fetches each round's frontier
// concurrently (bounded by GOMAXPROCS), so a wide tree of small objects is read
// in parallel. The current implementation happens to be deterministic — results
// are indexed by frontier position — but that is not part of the contract. get
// must be safe for concurrent use.
func ReachableKeys(root key.Key, get func(key.Key) ([]byte, error)) ([]key.Key, error) {
	seen := map[key.Key]bool{root: true}
	out := []key.Key{root}
	frontier := []key.Key{root}

	for len(frontier) > 0 {
		// Fetch each frontier node's children concurrently. childLists[i] holds
		// frontier[i]'s children; indexing by position keeps the result
		// deterministic regardless of completion order, and needs no lock.
		childLists := make([][]key.Key, len(frontier))
		eg := &errgroup.Group{}
		eg.SetLimit(runtime.GOMAXPROCS(0))
		// i and k are per-iteration (Go 1.22+), so each goroutine captures its
		// own copy and writes a distinct childLists[i] — no shared write.
		for i, k := range frontier {
			if k.Type() == key.Blob || k.Type() == key.XattrSet {
				continue // leaf: no children, nothing to fetch
			}
			eg.Go(func() error {
				data, err := get(k)
				if err != nil {
					return fmt.Errorf("fstree: reading %s: %w", k, err)
				}
				children, err := ChildKeys(k, data)
				if err != nil {
					return err
				}
				childLists[i] = children
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}

		var next []key.Key
		for _, children := range childLists {
			for _, ck := range children {
				if seen[ck] {
					continue
				}
				seen[ck] = true
				out = append(out, ck)
				next = append(next, ck)
			}
		}
		frontier = next
	}
	return out, nil
}
