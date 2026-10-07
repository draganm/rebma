package packstore

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/draganm/rebma/key"
)

// TestCompactDuringReads: reads stay correct while Compact unmaps
// and deletes their segments.
func TestCompactDuringReads(t *testing.T) {
	s, err := Open(t.TempDir(), WithSegmentSize(8<<10), WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var mu sync.Mutex
	byKey := map[key.Key][]byte{}
	var keys atomic.Pointer[[]key.Key]

	put := func(gen, count int) []Object {
		objs := make([]Object, count)
		for i := range objs {
			data := incompressible(4 << 10)
			data[0] = byte(i)
			data[1] = byte(gen)
			objs[i] = blobObj(t, data)
			if err := s.Put(objs[i].Key, objs[i].Data); err != nil {
				t.Fatal(err)
			}
		}
		mu.Lock()
		ks := make([]key.Key, 0, len(byKey)+len(objs))
		for _, o := range objs {
			byKey[o.Key] = o.Data
		}
		for k := range byKey {
			ks = append(ks, k)
		}
		mu.Unlock()
		keys.Store(&ks)
		return objs
	}
	put(0, 16)

	done := make(chan struct{})
	var readers sync.WaitGroup
	readErr := make(chan error, 4)
	for range 4 {
		readers.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				ks := *keys.Load()
				k := ks[i%len(ks)]
				if i%2 == 0 {
					data, err := s.Get(k)
					if errors.Is(err, ErrNotFound) {
						continue
					}
					if err != nil {
						readErr <- err
						return
					}
					mu.Lock()
					want := byKey[k]
					mu.Unlock()
					if !bytes.Equal(data, want) {
						readErr <- errors.New("Get returned wrong bytes")
						return
					}
					continue
				}
				rec, err := s.GetRecord(k)
				if errors.Is(err, ErrNotFound) {
					continue
				}
				if err != nil {
					readErr <- err
					return
				}
				var got int
				for _, b := range rec { // touch every byte
					got += int(b)
				}
				_ = got
			}
		})
	}

	for round := 1; round <= 8; round++ {
		objs := put(round, 16)
		live := map[key.Key]bool{}
		for _, o := range objs {
			live[o.Key] = true
		}
		if _, err := s.Compact(func(k key.Key) bool { return live[k] }, CompactOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	close(done)
	readers.Wait()
	select {
	case err := <-readErr:
		t.Fatal(err)
	default:
	}
}
