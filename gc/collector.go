package gc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
	"github.com/draganm/rebma/reference"
	"github.com/draganm/rebma/refstore"
)

// Collector implements the cycle and the reference hooks over an open
// packstore and refstore pair. A process has one per pair; other processes
// may run their own on the same store (architecture/mark-sweep-gc.md, "Across
// processes"). Close it before the stores.
type Collector struct {
	objects *packstore.Store
	refs    *refstore.Store
	dir     string // former closures dir: kept for layout compat and the free-space probe
	opts    Options

	// The reference lock: a reference PUT holds it shared from its
	// completeness walk to its commit; a cycle holds it exclusively
	// around the roots snapshot and again around the sweep. Between the
	// two a PUT proceeds — its walked closure joins the write barrier's
	// grey set, so the sweep keeps it.
	refLock sync.RWMutex

	mu          sync.Mutex // guards everything below
	last        *CycleStats
	lastErr     error
	cancelCycle context.CancelFunc
	midMark     func() // test hook: runs after the mark, before the sweep

	cycleMu sync.Mutex // held for the whole cycle; cycles never overlap

	stop context.CancelFunc // background loop
	done chan struct{}
}

// Open opens the collector next to an already-open packstore and refstore.
// dir is the layout slot the simple-gc collector kept closure files in
// (<store>/closures): it is created empty and any leftover closure state
// from a previous collector is swept — closures were derived data. With
// opts.Interval > 0 a goroutine runs a cycle per interval until Close.
func Open(dir string, objects *packstore.Store, refs *refstore.Store, opts Options) (*Collector, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("gc: creating %s: %w", dir, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return nil, fmt.Errorf("gc: sweeping stale closure state: %w", err)
		}
	}
	c := &Collector{objects: objects, refs: refs, dir: dir, opts: opts}
	if opts.Interval > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		c.stop = cancel
		c.done = make(chan struct{})
		go c.loop(ctx)
	}
	return c, nil
}

// Close stops the background loop and waits out a running cycle. Close the
// collector before the stores under it.
func (c *Collector) Close() error {
	if c.stop != nil {
		c.stop()
		<-c.done
	}
	c.mu.Lock()
	if c.cancelCycle != nil {
		c.cancelCycle()
	}
	c.mu.Unlock()
	c.cycleMu.Lock()
	c.cycleMu.Unlock() //nolint:staticcheck // barrier: wait out a running cycle
	return nil
}

// Wipe cancels a running cycle, waits it out and then runs reset (the
// store wipe) while holding the cycle slot. The mark reads segment mmaps
// unpinned, so the stores must not be wiped under it.
func (c *Collector) Wipe(reset func() error) error {
	c.mu.Lock()
	if c.cancelCycle != nil {
		c.cancelCycle()
	}
	c.mu.Unlock()
	c.cycleMu.Lock() // wait out the cancelled cycle
	defer c.cycleMu.Unlock()
	if err := reset(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.last, c.lastErr = nil, nil
	return nil
}

// PrepareRef readies a reference PUT naming root under the reference lock,
// held shared until commit or abort: the tree is walked for completeness
// (a missing object aborts with the error naming it — the caller's 404)
// and the walked closure is handed to the write barrier, so a PUT landing
// while a mark runs cannot lose its objects to the sweep. Exactly one of
// commit (after the reference record is stored) or abort (it was not)
// must be called. Release of an old root needs no bookkeeping; ReleaseRef
// exists for symmetry.
func (c *Collector) PrepareRef(root key.Key) (commit, abort func(), err error) {
	sp, err := c.BeginSpan()
	if err != nil {
		return nil, nil, err
	}
	if err := c.prepare(root); err != nil {
		sp.End()
		return nil, nil, err
	}
	return sp.End, sp.End, nil
}

// prepare is the work of a reference PUT under the reference lock: the
// completeness walk, and the walked closure handed to the write barrier.
func (c *Collector) prepare(root key.Key) error {
	keys, err := fstree.CheckComplete(root, c.objects.Get, c.objects.Has, c.opts.Jobs)
	if err != nil {
		return fmt.Errorf("gc: walking root %s: %w", root, err)
	}
	c.objects.ObserveKeys(keys)
	return nil
}

// BeginWrite gates one object-write span (an ingest, a pull, an inbox
// drain) against the sweep. The span holds the reference lock shared:
// the sweep waits out in-flight writes, and a write stalls while a sweep
// runs — never during the mark, which writers pass behind the write
// barrier. Without the gate a dedup hit against a record in a condemned
// pack could report success and then lose the record to the pack's
// removal (packstore.Compact must not overlap ingests). Returns the
// release, idempotent; call it when the span's writes are durable.
func (c *Collector) BeginWrite() (done func()) {
	sp, err := c.BeginSpan()
	if err != nil {
		// The store is closing, which the span's writes will report. The
		// reference lock's half of the span is kept all the same.
		c.refLock.RLock()
		var once sync.Once
		return func() { once.Do(c.refLock.RUnlock) }
	}
	return sp.End
}

// Span is an open write span: object writes and the reference puts that name
// them, as one unit against the sweep, in this process and in every other. A
// sweep here or elsewhere waits for it to end, and it waits for one. Inside
// it references are prepared with Span.PrepareRef, which takes no lock again:
// Collector.PrepareRef inside a span would take the reference lock a second
// time, and wait for ever behind a cycle that is waiting for the span.
//
// BeginSpan takes the reference lock and then the store's gate, the order a
// cycle takes them in. A span opened on the store itself
// (packstore.Store.BeginWrite) and held across Collector.PrepareRef takes
// them the other way round; in a process that runs cycles the two can wait
// for each other.
type Span struct {
	c     *Collector
	end   func()
	once  sync.Once
	ended atomic.Bool
}

// BeginSpan opens a write span. End it when its writes are durable and its
// references are stored.
func (c *Collector) BeginSpan() (*Span, error) {
	c.refLock.RLock()
	// The same span across processes (packstore's gate): a cycle in another
	// store waits for it, and it waits for one. The barrier that lets a PUT
	// land during a mark lives in the cycle's own process, so a PUT from
	// elsewhere must not land between a cycle's roots snapshot and its sweep
	// at all.
	end, err := c.objects.BeginWrite()
	if err != nil {
		c.refLock.RUnlock()
		return nil, fmt.Errorf("gc: %w", err)
	}
	return &Span{c: c, end: end}, nil
}

// PrepareRef readies a reference PUT naming root, as Collector.PrepareRef
// does, under the locks the span already holds. The reference may be stored
// any time before End; commit and abort are there for the shape and do
// nothing.
func (sp *Span) PrepareRef(root key.Key) (commit, abort func(), err error) {
	if sp.ended.Load() {
		return nil, nil, fmt.Errorf("gc: PrepareRef on a span that has ended")
	}
	if err := sp.c.prepare(root); err != nil {
		return nil, nil, err
	}
	nop := func() {}
	return nop, nop, nil
}

// ReleaseRef is Collector.ReleaseRef.
func (sp *Span) ReleaseRef(root key.Key) error { return sp.c.ReleaseRef(root) }

// End closes the span. It may be called more than once.
func (sp *Span) End() {
	sp.once.Do(func() {
		sp.ended.Store(true)
		sp.end()
		sp.c.refLock.RUnlock()
	})
}

// ReleaseRef records that one reference naming root was deleted or
// overwritten. The mark-and-sweep collector keeps no per-root state, so
// this is a no-op: the next cycle simply no longer marks from the root.
func (c *Collector) ReleaseRef(root key.Key) error {
	return nil
}

// roots lists the root key of every reference. The caller snapshots under
// the reference lock when the result must be exact.
func (c *Collector) roots() ([]key.Key, error) {
	recs, err := c.refs.All()
	if err != nil {
		return nil, err
	}
	roots := make([]key.Key, 0, len(recs))
	for _, r := range recs {
		ref, err := reference.Decode(r.Data)
		if err != nil {
			return nil, fmt.Errorf("gc: reference %q: %w", r.Name, err)
		}
		root, err := key.Parse(ref.Key)
		if err != nil {
			return nil, fmt.Errorf("gc: reference %q: %w", r.Name, err)
		}
		roots = append(roots, root)
	}
	return roots, nil
}

// markLive walks every root into a fresh mark set. The roots must be a
// snapshot taken under the reference lock; the walk touches only
// snapshot-reachable objects and runs concurrently with ingests (their
// writes join the barrier's grey set).
func (c *Collector) markLive(ctx context.Context, roots []key.Key) (*packstore.MarkSet, error) {
	live := c.objects.NewMarkSet()
	for _, root := range roots {
		if err := c.markFrom(ctx, live, root); err != nil {
			return nil, err
		}
	}
	return live, nil
}

// markFrom prunes at already-marked keys, so shared subtrees are walked
// once. Blob and xattr payloads are marked without being read.
func (c *Collector) markFrom(ctx context.Context, live *packstore.MarkSet, root key.Key) error {
	stack := []key.Key{root}
	for n := 0; len(stack) > 0; n++ {
		if n%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		k := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		newly, present := live.Mark(k)
		if !present {
			return fmt.Errorf("gc: mark: object %s missing from store", k)
		}
		if !newly {
			continue
		}
		if k.Type() == key.Blob || k.Type() == key.XattrSet {
			continue
		}
		data, err := c.objects.Get(k)
		if err != nil {
			return err
		}
		children, err := fstree.ChildKeys(k, data)
		if err != nil {
			return err
		}
		stack = append(stack, children...)
	}
	return nil
}

func (c *Collector) loop(ctx context.Context) {
	defer close(c.done)
	t := time.NewTicker(c.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.Run(ctx, -1) // errors land in Status.LastError
		}
	}
}
