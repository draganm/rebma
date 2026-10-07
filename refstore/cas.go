package refstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/reference"
	"github.com/draganm/rebma/refstore/internal/refsdb"
)

// ErrConflict is returned by the optimistic writes when the reference is not
// in the state the caller expected: for Create it exists, for the compare
// forms it points at another key, for UpdateBatch some name is not where the
// caller saw it. Nothing was changed; the caller re-reads and decides.
var ErrConflict = errors.New("refstore: reference is not at the expected key")

// CompareAndSwap stores record under name only if the reference currently
// points at old — the move from one key to the next that must not overwrite
// somebody else's move. It returns ErrNotFound if the reference does not
// exist and ErrConflict if it points elsewhere. The expectation is the key,
// not the record bytes: a rewrite that kept the key does not invalidate it.
// record is stored verbatim, as by Put.
func (s *Store) CompareAndSwap(name string, old key.Key, record []byte) error {
	return s.ifAt(name, old, func(ctx context.Context, q *refsdb.Queries, current []byte) (int64, error) {
		return q.ReplaceRecord(ctx, refsdb.ReplaceRecordParams{Record: blob(record), Name: blob([]byte(name)), OldRecord: current})
	})
}

// CompareAndDelete removes name only if it currently points at old, with
// CompareAndSwap's errors.
func (s *Store) CompareAndDelete(name string, old key.Key) error {
	return s.ifAt(name, old, func(ctx context.Context, q *refsdb.Queries, current []byte) (int64, error) {
		return q.DeleteRecordIf(ctx, refsdb.DeleteRecordIfParams{Name: blob([]byte(name)), OldRecord: current})
	})
}

// Create stores record under name only if no such reference exists, and
// returns ErrConflict if one does.
func (s *Store) Create(name string, record []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	n, err := s.q.CreateRecord(context.Background(), refsdb.CreateRecordParams{Name: blob([]byte(name)), Record: blob(record)})
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrConflict
	}
	return nil
}

// Publication is one record an UpdateBatch stores, with the key the caller
// saw its name at: Old is the key the record replaces, or nil if there must
// be no such reference.
type Publication struct {
	Record Record
	Old    *key.Key
}

// Withdrawal is one name an UpdateBatch removes, with the key the caller saw
// it at.
type Withdrawal struct {
	Name string
	Old  key.Key
}

// UpdateBatch withdraws names and publishes records in one commit, each only
// if its reference is where the caller saw it, so a caller replacing one set
// of references with another is never seen half way and never undoes another
// process's move. A record's expectation is the key it replaces, or nil for
// no reference; a withdrawn name may also be gone already, as the caller
// asked for that state. Every expectation is checked before anything changes,
// and if one fails nothing does and the result is ErrConflict — never
// ErrNotFound, even for a record whose name should exist and does not.
// Withdrawals run first, so a name in both ends up published. It returns how
// many rows the withdrawals removed. An empty batch changes nothing.
func (s *Store) UpdateBatch(records []Publication, names []Withdrawal) (removed int, err error) {
	if len(records) == 0 && len(names) == 0 {
		return 0, nil
	}
	ctx := context.Background()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	// Unconditional, and a no-op once committed: a panic must not leave the
	// write lock held, which would wedge every writer in every process.
	defer tx.Rollback()
	q := s.q.WithTx(tx)
	for _, w := range names {
		_, ref, err := currentRecord(ctx, q, w.Name)
		switch {
		case errors.Is(err, ErrNotFound):
			// Already gone, which is the state the caller asked for.
		case err != nil:
			return 0, err
		case !pointsAt(ref, w.Old):
			return 0, ErrConflict
		}
	}
	for _, p := range records {
		_, ref, err := currentRecord(ctx, q, p.Record.Name)
		switch {
		case errors.Is(err, ErrNotFound):
			if p.Old != nil {
				return 0, ErrConflict
			}
		case err != nil:
			return 0, err
		case p.Old == nil || !pointsAt(ref, *p.Old):
			return 0, ErrConflict
		}
	}
	// The write lock has been held since the checks, so the plain statements
	// cannot undo another process's change.
	for _, w := range names {
		n, err := q.DeleteRecord(ctx, blob([]byte(w.Name)))
		if err != nil {
			return 0, err
		}
		removed += int(n)
	}
	for _, p := range records {
		if err := q.PutRecord(ctx, refsdb.PutRecordParams{Name: blob([]byte(p.Record.Name)), Record: blob(p.Record.Data)}); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return removed, nil
}

// currentRecord returns the record stored under name, raw and decoded, or
// ErrNotFound if there is none. A record that does not decode is an error.
func currentRecord(ctx context.Context, q *refsdb.Queries, name string) ([]byte, reference.Reference, error) {
	raw, err := q.GetRecord(ctx, blob([]byte(name)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, reference.Reference{}, ErrNotFound
	}
	if err != nil {
		return nil, reference.Reference{}, err
	}
	ref, err := reference.Decode(raw)
	if err != nil {
		return nil, reference.Reference{}, fmt.Errorf("refstore: current record of %q: %w", name, err)
	}
	return raw, ref, nil
}

func pointsAt(ref reference.Reference, k key.Key) bool {
	return bytes.Equal(ref.Key, k[:])
}

// ifAt runs change inside one write transaction after checking that name
// exists and points at old. The key lives inside the record, so the current
// record is decoded; one that does not decode is an error, never a match.
// change is guarded by the bytes just read and reports the rows it touched.
func (s *Store) ifAt(name string, old key.Key, change func(ctx context.Context, q *refsdb.Queries, current []byte) (int64, error)) (err error) {
	ctx := context.Background()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// Unconditional, and a no-op once committed: a panic must not leave the
	// write lock held, which would wedge every writer in every process.
	defer tx.Rollback()
	q := s.q.WithTx(tx)
	current, ref, err := currentRecord(ctx, q, name)
	if err != nil {
		return err
	}
	if !pointsAt(ref, old) {
		return ErrConflict
	}
	n, err := change(ctx, q, blob(current))
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrConflict
	}
	return tx.Commit()
}
