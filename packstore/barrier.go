package packstore

import "github.com/draganm/rebma/key"

// BeginBarrier starts grey capture: keys a write observes are live for
// the next Compact, letting ingests run concurrently with the caller's
// mark (specs/gc.qnt). Compact itself must still not overlap ingests.
func (s *Store) BeginBarrier() {
	s.greyMu.Lock()
	defer s.greyMu.Unlock()
	s.grey = make(map[key.Key]struct{})
	s.capturing.Store(true)
}

// AbortBarrier discards a capture without compacting.
func (s *Store) AbortBarrier() {
	s.greyMu.Lock()
	defer s.greyMu.Unlock()
	s.capturing.Store(false)
	s.grey = nil
}

func (s *Store) observe(k key.Key) {
	if !s.capturing.Load() {
		return
	}
	s.greyMu.Lock()
	defer s.greyMu.Unlock()
	if s.grey != nil {
		s.grey[k] = struct{}{}
	}
}

// ObserveKeys marks keys live for the next Compact, like the write paths'
// per-key observation. A reference PUT that lands while a mark is running
// calls it with the root's whole closure, so a reference committed during
// the cycle never dangles.
func (s *Store) ObserveKeys(keys []key.Key) {
	if !s.capturing.Load() {
		return
	}
	s.greyMu.Lock()
	defer s.greyMu.Unlock()
	if s.grey == nil {
		return
	}
	for _, k := range keys {
		s.grey[k] = struct{}{}
	}
}

// takeGrey ends the capture and hands the set to Compact.
func (s *Store) takeGrey() map[key.Key]struct{} {
	s.greyMu.Lock()
	defer s.greyMu.Unlock()
	s.capturing.Store(false)
	g := s.grey
	s.grey = nil
	return g
}
