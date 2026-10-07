package fstree

import (
	"fmt"
	"runtime"

	"github.com/draganm/rebma/key"
	"golang.org/x/sync/errgroup"
)

// MissingObjectError reports a leaf object found absent by CheckComplete.
type MissingObjectError struct {
	Key key.Key
}

func (e *MissingObjectError) Error() string {
	return fmt.Sprintf("fstree: object %s is missing", e.Key)
}

// CheckComplete verifies that every object reachable from root exists and
// returns the visited keys — root first, then discovery order, each once.
// The tree is walked breadth-first, checking each level's objects with up
// to jobs concurrent lookups (jobs <= 0 means GOMAXPROCS): interior nodes
// are read with get — a failed read surfaces as the wrapped get error — and
// Blob and XattrSet leaves are tested with has — an absent leaf surfaces as
// a *MissingObjectError. On error the visited list is nil.
func CheckComplete(root key.Key, get func(key.Key) ([]byte, error), has func(key.Key) (bool, error), jobs int) ([]key.Key, error) {
	if jobs <= 0 {
		jobs = runtime.GOMAXPROCS(0)
	}
	visited := []key.Key{root}
	seen := map[key.Key]bool{root: true}
	frontier := []key.Key{root}
	for len(frontier) > 0 {
		children := make([][]key.Key, len(frontier))
		g := new(errgroup.Group)
		g.SetLimit(jobs)
		for i, k := range frontier {
			g.Go(func() error {
				if k.Type() == key.Blob || k.Type() == key.XattrSet {
					ok, err := has(k)
					if err != nil {
						return err
					}
					if !ok {
						return &MissingObjectError{Key: k}
					}
					return nil
				}
				data, err := get(k)
				if err != nil {
					return fmt.Errorf("fstree: reading %s: %w", k, err)
				}
				children[i], err = ChildKeys(k, data)
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		var next []key.Key
		for _, cks := range children {
			for _, ck := range cks {
				if !seen[ck] {
					seen[ck] = true
					visited = append(visited, ck)
					next = append(next, ck)
				}
			}
		}
		frontier = next
	}
	return visited, nil
}
