package packstore

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/draganm/rebma/key"
	"golang.org/x/sync/errgroup"
)

// DefaultBatchSize is the byte threshold at which a writer fsyncs the active
// segment, making everything appended so far durable.
const DefaultBatchSize = 16 << 20 // 16 MiB

// WriteStats summarizes one WriteParallel run.
// On a non-nil error, the stats reflect the work done before the abort.
type WriteStats struct {
	Stored      int   // objects newly written
	Deduped     int   // objects skipped (already present, or duplicated in the stream)
	BytesStored int64 // payload bytes of newly-written objects (uncompressed)
}

// WriteOpts configures WriteParallel.
type WriteOpts struct {
	Writers   int  // concurrent writers; <= 0 means GOMAXPROCS
	BatchSize int  // fsync when a writer has appended this many bytes; <= 0 means DefaultBatchSize
	Verify    bool // recompute and check each new object's key before storing it
}

// WriteParallel stores every object the iterator yields using multiple
// concurrent workers. Compression and (optional) verification run in
// parallel; appends serialize on the active segment. Each worker fsyncs after
// appending BatchSize bytes, and the run fsyncs once more before returning.
//
// Like WriteBatch, WriteParallel is durable-on-return but NOT atomic: on
// error or crash a valid prefix remains, which a content-addressed re-run
// deduplicates. The final fsync also covers dedup hits against records a
// concurrent, uncommitted run appended. If the iterator yields
// an error, WriteParallel stops and returns it. With opts.Verify, a
// key/payload mismatch stops the run with a wrapped ErrVerify. An Object may
// carry a pre-encoded Record instead of Data; it is validated and appended
// as is (see Object).
func (s *Store) WriteParallel(seq iter.Seq2[Object, error], opts WriteOpts) (WriteStats, error) {
	w, gerr := s.beginWrite()
	if gerr != nil {
		return WriteStats{}, gerr
	}
	defer s.endWrite(w)
	writers := opts.Writers
	if writers <= 0 {
		writers = runtime.GOMAXPROCS(0)
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan Object, writers*2)
	seen := newSeenSet()
	eg := &errgroup.Group{}
	var stored, deduped, bytesStored atomic.Int64

	// Distributor: forward objects from the iterator to the worker pool. A
	// yielded error cancels the run and propagates as the group's error.
	eg.Go(func() error {
		defer close(ch)
		for obj, err := range seq {
			if err != nil {
				cancel()
				return err
			}
			select {
			case ch <- obj:
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})

	for range writers {
		eg.Go(func() error {
			err := s.runWriter(ctx, ch, seen, batchSize, opts.Verify, &stored, &deduped, &bytesStored)
			if err != nil {
				cancel() // stop the distributor and sibling workers
			}
			return err
		})
	}

	err := eg.Wait()
	// Always fsync, as WriteBatch does. Even with nothing appended a dedup
	// hit may have matched another run's unsynced record. On error the
	// appended prefix is Has-visible and must become durable too.
	if serr := s.syncActive(); err == nil {
		err = serr
	}
	return WriteStats{
		Stored:      int(stored.Load()),
		Deduped:     int(deduped.Load()),
		BytesStored: bytesStored.Load(),
	}, err
}

// runWriter consumes objects, encoding (compressing, optionally verifying)
// them concurrently with its siblings and appending them to the store. It
// fsyncs after every batchSize appended bytes. The final fsync is
// WriteParallel's.
func (s *Store) runWriter(ctx context.Context, ch <-chan Object, seen *seenSet, batchSize int, verify bool, stored, deduped, bytesStored *atomic.Int64) error {
	pending := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case obj, ok := <-ch:
			if !ok {
				return nil
			}
			s.observe(obj.Key)
			if !seen.addIfAbsent(obj.Key) {
				deduped.Add(1)
				continue
			}
			has, err := s.hasDurably(obj.Key)
			if err != nil {
				return err
			}
			if has {
				deduped.Add(1)
				continue
			}
			rec, ulen, err := prepare(obj, verify)
			if err != nil {
				return err
			}
			if err := s.append(obj.Key, rec, false); err != nil {
				return err
			}
			stored.Add(1)
			bytesStored.Add(ulen)
			pending += len(rec)
			if pending >= batchSize {
				pending = 0
				if err := s.syncActive(); err != nil {
					return err
				}
			}
		}
	}
}

// seenSet is a concurrency-safe set of keys, sharded on the key's first byte
// (uniformly distributed) to spread lock contention across writers.
type seenSet struct {
	shards [256]seenShard
}

type seenShard struct {
	mu sync.Mutex
	m  map[key.Key]struct{}
}

func newSeenSet() *seenSet {
	s := &seenSet{}
	for i := range s.shards {
		s.shards[i].m = make(map[key.Key]struct{})
	}
	return s
}

// addIfAbsent records k and reports true if it was not already present.
func (s *seenSet) addIfAbsent(k key.Key) bool {
	sh := &s.shards[k[0]]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.m[k]; ok {
		return false
	}
	sh.m[k] = struct{}{}
	return true
}
