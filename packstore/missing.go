package packstore

import (
	"fmt"
	"runtime"

	"github.com/draganm/rebma/key"
	"golang.org/x/sync/errgroup"
)

// minMissingChunk caps how many goroutines a small key list spawns: a chunk
// never holds fewer keys than this, so the goroutine overhead stays amortized.
const minMissingChunk = 64

const maxParallel = 16

// Missing reports which of keys are absent from the store, preserving the
// input's order and multiplicity. Lookups run concurrently over contiguous
// chunks of the input.
func (s *Store) Missing(keys []key.Key) ([]key.Key, error) {
	workers := runtime.GOMAXPROCS(0)
	if m := (len(keys) + minMissingChunk - 1) / minMissingChunk; m < workers {
		workers = m
	}
	if workers == 0 {
		return nil, nil
	}
	// Misses are what this call expects, so it looks at the directory once,
	// up front, rather than after each.
	if _, err := s.refreshAfterMiss(false); err != nil {
		return nil, err
	}
	chunkLen := (len(keys) + workers - 1) / workers
	results := make([][]key.Key, workers)

	eg := &errgroup.Group{}
	eg.SetLimit(maxParallel)

	for i := range workers {
		// Both bounds clamp: with many workers the rounded-up chunkLen can
		// push a late worker's window past the end of keys.
		lo := min(i*chunkLen, len(keys))
		chunk := keys[lo:min((i+1)*chunkLen, len(keys))]
		eg.Go(func() error {
			var miss []key.Key
			for _, k := range chunk {
				has, err := s.hasLocal(k)
				if err != nil {
					return fmt.Errorf("missing-check %s: %w", k, err)
				}
				if !has {
					miss = append(miss, k)
				}
			}
			results[i] = miss
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	var missing []key.Key
	for _, r := range results {
		missing = append(missing, r...)
	}
	return missing, nil
}
