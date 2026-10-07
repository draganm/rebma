package gc

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/reference"
)

// PackStatus is one sealed pack's score against a mark.
type PackStatus struct {
	ID       uint64
	Sealed   time.Time
	Body     int64 // record bytes
	Keys     uint64
	Live     int64 // Σ (46 + slen) over marked entries
	Garbage  float64
	Eligible bool // sealed before the horizon (now − grace)
}

// Status is the gc report: per-pack scores against a fresh advisory mark,
// totals, the last cycle. The mark runs without quiescing writers, so
// concurrent churn can skew the numbers; a cycle's own mark is exact.
type Status struct {
	Packs        []PackStatus
	LiveBytes    int64
	GarbageBytes int64
	Refs         int // reference names
	Marked       int // distinct live objects marked
	Last         *CycleStats
	LastError    string
}

// Status marks from the current references and scores every sealed pack —
// a full mark walk, the cost of keeping no persistent liveness state.
func (c *Collector) Status(ctx context.Context) (Status, error) {
	// The advisory mark probes sealed footers without scrub registration,
	// so serialize with cycles: a sweep must not munmap a victim under the
	// walk. A Run racing a long Status reports ErrCycleRunning, exactly as
	// it would against another cycle.
	c.cycleMu.Lock()
	defer c.cycleMu.Unlock()
	recs, err := c.refs.All()
	if err != nil {
		return Status{}, err
	}
	roots, err := c.roots()
	if err != nil {
		return Status{}, err
	}
	// The references are everybody's; the view of the objects is this
	// store's, as of its last look. What another process wrote and named
	// since then is not in it yet, and the mark works from a snapshot of it.
	if err := c.objects.Refresh(); err != nil {
		return Status{}, err
	}
	live, err := c.markLive(ctx, roots)
	if err != nil {
		return Status{}, err
	}
	report, err := c.objects.Liveness(live.Contains)
	if err != nil {
		return Status{}, err
	}
	segs, err := c.objects.Segments()
	if err != nil {
		return Status{}, err
	}
	sealedAt := make(map[uint64]time.Time, len(segs))
	for _, seg := range segs {
		sealedAt[seg.ID] = seg.Sealed
	}
	horizon := time.Now().Add(-c.opts.Grace)
	st := Status{Refs: len(recs), Marked: live.Marked()}
	for _, sl := range report {
		if !sl.Sealed {
			continue // the active segment is never a victim
		}
		ps := PackStatus{
			ID:     sl.ID,
			Sealed: sealedAt[sl.ID],
			Body:   int64(sl.LiveBytes + sl.DeadBytes),
			Keys:   uint64(sl.LiveKeys + sl.DeadKeys),
			Live:   int64(sl.LiveBytes),
		}
		if ps.Body > 0 {
			ps.Garbage = float64(sl.DeadBytes) / float64(ps.Body)
		}
		ps.Eligible = ps.Sealed.Before(horizon)
		st.Packs = append(st.Packs, ps)
		st.LiveBytes += ps.Live
		st.GarbageBytes += int64(sl.DeadBytes)
	}
	c.mu.Lock()
	st.Last = c.last
	if c.lastErr != nil {
		st.LastError = c.lastErr.Error()
	}
	c.mu.Unlock()
	return st, nil
}

// Why returns the sorted names of the references whose tree reaches k —
// why the object is alive. Each reference's tree is walked until k is
// found; there is no persistent closure to consult.
func (c *Collector) Why(k key.Key) ([]string, error) {
	recs, err := c.refs.All()
	if err != nil {
		return nil, err
	}
	var names []string
	for _, r := range recs {
		ref, err := reference.Decode(r.Data)
		if err != nil {
			return nil, fmt.Errorf("gc: reference %q: %w", r.Name, err)
		}
		root, err := key.Parse(ref.Key)
		if err != nil {
			return nil, fmt.Errorf("gc: reference %q: %w", r.Name, err)
		}
		found, err := c.reaches(root, k)
		if err != nil {
			return nil, fmt.Errorf("gc: reference %q: %w", r.Name, err)
		}
		if found {
			names = append(names, r.Name)
		}
	}
	slices.Sort(names)
	return names, nil
}

// UnreachableFrom returns the candidates that roots does not reach, in the
// order they were given. The mark starts from roots alone: a published
// reference is not a root here unless the caller passes its key. Advisory:
// the walk takes no barrier and no reference lock, so the caller must keep
// roots, and anything published since, alive while it acts on the answer.
// Deletes nothing.
func (c *Collector) UnreachableFrom(ctx context.Context, roots, candidates []key.Key) ([]key.Key, error) {
	// No cycle slot either, unlike Status: the mark set owns its footer
	// indexes and the walk reads through the store's own lock, so a sweep
	// alongside unmaps nothing under it. Waiting for the slot would
	// deadlock a caller that asks from inside a write span against a cycle
	// that is waiting for that span.
	live, err := c.markLive(ctx, roots)
	if err != nil {
		return nil, err
	}
	var dead []key.Key
	for _, k := range candidates {
		if !live.Contains(k) {
			dead = append(dead, k)
		}
	}
	return dead, nil
}

// reaches walks root's tree until k is found, pruning revisited subtrees.
func (c *Collector) reaches(root, k key.Key) (bool, error) {
	visited := map[key.Key]bool{}
	stack := []key.Key{root}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == k {
			return true, nil
		}
		if visited[cur] {
			continue
		}
		visited[cur] = true
		if cur.Type() == key.Blob || cur.Type() == key.XattrSet {
			continue
		}
		data, err := c.objects.Get(cur)
		if err != nil {
			return false, err
		}
		children, err := fstree.ChildKeys(cur, data)
		if err != nil {
			return false, err
		}
		stack = append(stack, children...)
	}
	return false, nil
}
