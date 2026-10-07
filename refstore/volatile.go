package refstore

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/draganm/rebma/refstore/internal/refsdb"
)

// PutVolatile is Put without the commit fsync, for a record that need not
// survive a power loss because its owner discards it when it starts, such as
// a session's pin; with sync on, Put pays one fsync for each.
//
// The caller gives up durability and nothing else. The record is visible at
// once and survives a crash of the process, but a power loss or a crash of
// the operating system can take the most recent volatile writes with it. The
// database stays consistent either way, and no durable write is affected. On
// a store opened without sync no commit is fsynced, and PutVolatile is Put.
func (s *Store) PutVolatile(name string, record []byte) error {
	return s.volatile(func(ctx context.Context, db refsdb.DBTX) error {
		return refsdb.New(db).PutRecord(ctx, refsdb.PutRecordParams{Name: blob([]byte(name)), Record: blob(record)})
	})
}

// DeleteVolatile is Delete without the commit fsync, for a record written by
// PutVolatile, and on the same terms: a delete that is lost with the machine
// brings the record back, for its owner to discard. Like Delete it returns
// ErrNotFound if name is absent.
func (s *Store) DeleteVolatile(name string) error {
	return s.volatile(func(ctx context.Context, db refsdb.DBTX) error {
		n, err := refsdb.New(db).DeleteRecord(ctx, blob([]byte(name)))
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// volatile runs write, one autocommit statement, with synchronous=NORMAL,
// which in WAL mode skips the fsync of the commit.
//
// synchronous is a setting of one connection, and the pool hands a write
// whichever connection is free. So the switch, the write and the restore run
// on one connection that is held out of the pool for as long as it is
// relaxed, where nothing else can run on it, and under writeMu like every
// write. It is back at FULL before the pool sees it again, also after a write
// that failed or panicked: the next durable write may be handed it.
func (s *Store) volatile(write func(ctx context.Context, db refsdb.DBTX) error) (err error) {
	ctx := context.Background()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !s.syncWrites {
		// Every connection is at NORMAL already: nothing to relax, and
		// nothing to put back.
		return write(ctx, s.db)
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	// Deferred before the switch, so that a switch that failed is covered too.
	defer func() {
		_, restoreErr := conn.ExecContext(ctx, "PRAGMA synchronous=FULL")
		if restoreErr == nil {
			return
		}
		// A connection left at NORMAL would drop the fsync of every later
		// commit that runs on it, so it is closed instead of pooled: Raw
		// discards the connection when its function reports ErrBadConn. The
		// error wins over the write's own. Whatever kept the pragma from
		// running, an open transaction or a broken connection, leaves the
		// outcome of the write unknown as well.
		conn.Raw(func(any) error { return driver.ErrBadConn })
		err = fmt.Errorf("refstore: restoring synchronous=FULL after a volatile write: %w", restoreErr)
	}()
	if _, err := conn.ExecContext(ctx, "PRAGMA synchronous=NORMAL"); err != nil {
		return err
	}
	return write(ctx, conn)
}
