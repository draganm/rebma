package refstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/draganm/rebma/refstore/internal/refsdb"
)

// driverConn returns the driver's connection behind conn, which identifies it
// across trips through the pool.
func driverConn(t *testing.T, conn *sql.Conn) (dc any) {
	t.Helper()
	if err := conn.Raw(func(c any) error { dc = c; return nil }); err != nil {
		t.Fatal(err)
	}
	return dc
}

// synchronousOfPool reads PRAGMA synchronous on every connection of s's pool,
// keyed by driverConn. A write may run on any of them, so there is no single
// write connection to ask; holding maxConns connections at once is holding
// all the pool may open, the one the last write ran on included.
func synchronousOfPool(t *testing.T, s *Store) map[any]string {
	t.Helper()
	// A connection that was never given back would make the last Conn wait
	// forever.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	levels := make(map[any]string, maxConns)
	for i := range maxConns {
		conn, err := s.db.Conn(ctx)
		if err != nil {
			t.Fatalf("taking connection %d of %d: %v", i+1, maxConns, err)
		}
		defer conn.Close()
		var level string
		if err := conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&level); err != nil {
			t.Fatalf("PRAGMA synchronous: %v", err)
		}
		levels[driverConn(t, conn)] = level
	}
	return levels
}

// wantSynchronous fails the test unless every connection of s's pool has
// PRAGMA synchronous at want ("2" is FULL, "1" is NORMAL).
func wantSynchronous(t *testing.T, s *Store, want string) {
	t.Helper()
	for _, got := range synchronousOfPool(t, s) {
		if got != want {
			t.Fatalf("a connection of the pool has synchronous = %s, want %s", got, want)
		}
	}
}

// The Rust test of the same name. Rust asks its one writer connection for
// the durability; here every connection of the pool is asked.
func TestVolatileWritesLandAndLeaveTheDurabilityAsConfigured(t *testing.T) {
	for _, durable := range []bool{true, false} {
		t.Run(fmt.Sprintf("sync=%v", durable), func(t *testing.T) {
			s := openT(t, t.TempDir(), durable)
			configured := pragma(t, s.db, "synchronous")
			if err := s.PutVolatile("pin", []byte("held")); err != nil {
				t.Fatal(err)
			}
			if got, err := s.Get("pin"); err != nil || string(got) != "held" {
				t.Fatalf("Get = %q, %v; want held", got, err)
			}
			if err := s.PutVolatile("pin", []byte("again")); err != nil {
				t.Fatal(err)
			}
			if got, err := s.Get("pin"); err != nil || string(got) != "again" {
				t.Fatalf("Get after overwrite = %q, %v; want again", got, err)
			}
			if err := s.DeleteVolatile("pin"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Get("pin"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get after delete = %v, want ErrNotFound", err)
			}
			if err := s.DeleteVolatile("pin"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("DeleteVolatile(absent) = %v, want ErrNotFound", err)
			}
			wantSynchronous(t, s, configured)
		})
	}
}

// With sync on, the write must really run at NORMAL — otherwise nothing is
// saved — and on a connection of its own: the pool's other connections keep
// FULL meanwhile, and the write's connection goes back to the pool at FULL.
// With sync off there is nothing to relax, and no connection to set aside.
func TestVolatileWriteRunsRelaxedOnItsOwnConnection(t *testing.T) {
	for _, tc := range []struct {
		sync                bool
		during, other, then string
	}{
		{true, "1", "2", "2"},  // NORMAL for the write only
		{false, "1", "1", "1"}, // NORMAL as configured
	} {
		t.Run(fmt.Sprintf("sync=%v", tc.sync), func(t *testing.T) {
			s := openT(t, t.TempDir(), tc.sync)
			var during, other string
			var own any // the write's connection, if it was given one
			err := s.volatile(func(ctx context.Context, db refsdb.DBTX) error {
				if conn, ok := db.(*sql.Conn); ok {
					own = driverConn(t, conn)
				}
				other = pragma(t, s.db, "synchronous")
				return db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&during)
			})
			if err != nil {
				t.Fatal(err)
			}
			if during != tc.during {
				t.Errorf("synchronous on the write's connection = %s, want %s", during, tc.during)
			}
			if other != tc.other {
				t.Errorf("synchronous on another connection during the write = %s, want %s", other, tc.other)
			}
			levels := synchronousOfPool(t, s)
			for _, got := range levels {
				if got != tc.then {
					t.Errorf("a connection of the pool has synchronous = %s, want %s", got, tc.then)
				}
			}
			if !tc.sync {
				return
			}
			// Not merely some connection at FULL: the one the write ran on,
			// restored rather than replaced.
			if got, pooled := levels[own]; !pooled || got != tc.then {
				t.Errorf("synchronous on the write's connection afterwards = %q (in the pool: %v), want %s", got, pooled, tc.then)
			}
		})
	}
}

// A write that fails — here on SQLite's busy error, another handle holding
// the write lock — must put FULL back all the same.
func TestFailedVolatileWriteLeavesTheDurabilityAsConfigured(t *testing.T) {
	shortBusyTimeout(t, 300*time.Millisecond)
	dir := t.TempDir()
	s, busy := openT(t, dir, true), openT(t, dir, true)
	release := holdWriteLock(t, busy)
	err := s.PutVolatile("pin", []byte("held"))
	release()
	if !isBusy(err) {
		t.Fatalf("PutVolatile while another connection held the write lock = %v, want SQLite's busy error", err)
	}
	wantSynchronous(t, s, "2")

	if err := s.PutVolatile("pin", []byte("held")); err != nil {
		t.Fatalf("PutVolatile after the writer finished: %v", err)
	}
	if got, err := s.Get("pin"); err != nil || string(got) != "held" {
		t.Fatalf("Get = %q, %v; want held", got, err)
	}
	wantSynchronous(t, s, "2")
}

// A write that panics must leave neither its connection relaxed nor the
// write mutex held.
func TestVolatileWriteThatPanicsLeavesTheDurabilityAsConfigured(t *testing.T) {
	s := openT(t, t.TempDir(), true)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the write did not panic")
			}
		}()
		s.volatile(func(context.Context, refsdb.DBTX) error { panic("boom") })
	}()
	wantSynchronous(t, s, "2")
	if !s.writeMu.TryLock() {
		t.Fatal("the write mutex is still held")
	}
	s.writeMu.Unlock()
	if err := s.Put("after", []byte("1")); err != nil {
		t.Fatalf("the handle that panicked cannot write any more: %v", err)
	}
}

// SQLite refuses to change synchronous inside a transaction, so a write that
// leaves one open makes the restore fail. A connection whose durability is
// unknown must then be closed, not given back to the pool, where the next
// durable write could pick it up. Closing it also ends the transaction, so
// the write lock is not left held.
func TestVolatileWriteThatCannotRestoreGivesUpItsConnection(t *testing.T) {
	shortBusyTimeout(t, 500*time.Millisecond)
	dir := t.TempDir()
	s, other := openT(t, dir, true), openT(t, dir, true)
	before := s.db.Stats().OpenConnections
	if before == 0 {
		t.Fatal("the pool holds no connection to take")
	}
	err := s.volatile(func(ctx context.Context, db refsdb.DBTX) error {
		_, err := db.ExecContext(ctx, "BEGIN IMMEDIATE")
		return err
	})
	if err == nil {
		t.Fatal("the write succeeded and the failed restore went unreported")
	}
	if got := s.db.Stats().OpenConnections; got != before-1 {
		t.Fatalf("%d connections open, want %d: the connection must be closed, not pooled", got, before-1)
	}
	wantSynchronous(t, s, "2")
	if err := other.Put("after", []byte("1")); err != nil {
		t.Fatalf("another handle cannot write any more: %v", err)
	}
	if err := s.PutVolatile("after-2", []byte("2")); err != nil {
		t.Fatalf("the handle that gave up a connection cannot write any more: %v", err)
	}
	wantSynchronous(t, s, "2")
}

// Volatile and durable writers share one store and one pool. Whatever
// connection the pool hands out while they run must be at FULL: a durable
// write may be the next to get it.
func TestConcurrentWritersNeverFindARelaxedConnection(t *testing.T) {
	s := openT(t, t.TempDir(), true)
	const rounds = 30
	var wg sync.WaitGroup
	defer wg.Wait()
	for worker := range 4 {
		wg.Go(func() {
			name := fmt.Sprintf("pin-%d", worker)
			for range rounds {
				if err := s.PutVolatile(name, []byte("held")); err != nil {
					t.Error(err)
					return
				}
				if err := s.DeleteVolatile(name); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for i := range rounds {
			if err := s.Put(fmt.Sprintf("durable-%d", i), []byte("kept")); err != nil {
				t.Error(err)
				return
			}
		}
	})
	for range rounds {
		wantSynchronous(t, s, "2")
	}
	wg.Wait()
	wantSynchronous(t, s, "2")
	all, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != rounds {
		t.Fatalf("%d records, want the %d durable ones and no pin", len(all), rounds)
	}
}
