package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/draganm/rebma/gc"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/reference"
	"github.com/draganm/rebma/refstore"
	"github.com/urfave/cli/v2"
)

func refCommand() *cli.Command {
	return &cli.Command{
		Name:  "ref",
		Usage: "manage references: named pointers to root keys",
		Subcommands: []*cli.Command{
			{
				Name:   "list",
				Usage:  "list every reference: name, key, creation time, creator",
				Action: runRefList,
			},
			{
				Name:      "get",
				Usage:     "print the key a reference points at",
				ArgsUsage: "NAME",
				Action:    runRefGet,
			},
			{
				Name:      "set",
				Usage:     "create or overwrite reference NAME pointing at KEY",
				ArgsUsage: "NAME KEY",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "expect", Usage: "only if NAME currently points at `OLD`; 'none': only if NAME does not exist"},
				},
				Action: runRefSet,
			},
			{
				Name:      "rm",
				Usage:     "delete reference NAME",
				ArgsUsage: "NAME",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "expect", Usage: "only if NAME currently points at `OLD`"},
				},
				Action: runRefRm,
			},
		},
	}
}

// expectation is the precondition of an optimistic reference write: the
// reference must not have moved since the caller last looked.
type expectation struct {
	conditional bool    // false: write unconditionally
	absent      bool    // the reference must not exist
	key         key.Key // otherwise it must point here
}

func parseExpect(c *cli.Context, allowNone bool) (expectation, error) {
	if !c.IsSet("expect") {
		return expectation{}, nil
	}
	s := c.String("expect")
	switch {
	case s == "":
		// A script's unset variable. It must not quietly turn the write
		// into an unconditional one.
		return expectation{}, errors.New("--expect is empty: it takes the key the reference must point at")
	case s == "none" && allowNone:
		return expectation{conditional: true, absent: true}, nil
	case s == "none":
		return expectation{}, errors.New("--expect none: a delete cannot expect the reference to be absent")
	}
	k, err := parseHexKey(s)
	if err != nil {
		return expectation{}, fmt.Errorf("--expect: %w", err)
	}
	return expectation{conditional: true, key: k}, nil
}

// explain turns the store's sentinel errors into what the user expected.
func (e expectation) explain(name string, err error) error {
	switch {
	case errors.Is(err, refstore.ErrConflict) && e.absent:
		return fmt.Errorf("reference %q already exists: %w", name, err)
	case errors.Is(err, refstore.ErrConflict):
		return fmt.Errorf("reference %q does not point at %s: %w", name, e.key, err)
	case errors.Is(err, refstore.ErrNotFound) && e.conditional:
		return fmt.Errorf("reference %q does not exist, expected it at %s: %w", name, e.key, err)
	}
	return err
}

func runRefList(c *cli.Context) error {
	if c.NArg() != 0 {
		return fmt.Errorf("ref list takes no arguments, got %d", c.NArg())
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)
	records, err := refs.All()
	if err != nil {
		return err
	}
	for _, r := range records {
		rec, err := reference.Decode(r.Data)
		if err != nil {
			return fmt.Errorf("reference %q: %w", r.Name, err)
		}
		k, err := key.Parse(rec.Key)
		if err != nil {
			return fmt.Errorf("reference %q: stored key: %w", r.Name, err)
		}
		line := fmt.Sprintf("%s %s %s", rec.Name, k, time.Unix(0, rec.CreatedAt).UTC().Format(time.RFC3339))
		if rec.User != "" {
			line += " " + rec.User
		}
		if _, err := fmt.Fprintln(c.App.Writer, line); err != nil {
			return err
		}
	}
	return nil
}

func runRefGet(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("ref get requires exactly one NAME argument, got %d", c.NArg())
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)
	k, _, err := resolveSpec(refs, "ref:"+c.Args().First())
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(c.App.Writer, k.String())
	return err
}

func runRefSet(c *cli.Context) error {
	if c.NArg() != 2 {
		return fmt.Errorf("ref set requires NAME KEY arguments, got %d", c.NArg())
	}
	name := c.Args().Get(0)
	k, err := parseHexKey(c.Args().Get(1))
	if err != nil {
		return err
	}
	exp, err := parseExpect(c, true)
	if err != nil {
		return err
	}
	rec := reference.Reference{
		Name:      name,
		Key:       k[:],
		CreatedAt: time.Now().UnixNano(),
	}
	raw, err := rec.Encode()
	if err != nil {
		return err
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	coll, err := openCollector(c, objects, refs, gc.Options{})
	if err != nil {
		closeStore(objects, refs)
		return err
	}
	err = putRef(coll, refs, name, k, raw, exp)
	return errors.Join(err, coll.Close(), closeStore(objects, refs))
}

func runRefRm(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("ref rm requires exactly one NAME argument, got %d", c.NArg())
	}
	exp, err := parseExpect(c, false)
	if err != nil {
		return err
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	coll, err := openCollector(c, objects, refs, gc.Options{})
	if err != nil {
		closeStore(objects, refs)
		return err
	}
	err = rmRef(coll, refs, c.Args().First(), exp)
	return errors.Join(err, coll.Close(), closeStore(objects, refs))
}

// putRef writes a reference under the collector's removal lock: the closure
// is reused or walked — a missing object fails the write, naming it — the
// record is stored, and an overwritten root is released. This is the
// optimistic reference PUT: on a 404 the caller re-sends the missing
// objects and retries.
//
// Unconditional calls for one name must be serialized by the caller (the
// one-shot CLI is); the read-old → prepare → put → release sequence is not
// atomic against a concurrent writer of the same name. With an expectation
// the store itself refuses the write if the reference moved meanwhile.
// refGate is what a reference put needs from the collector: the completeness
// walk under the reference lock. A *gc.Collector opens a span for the one
// put; a *gc.Span is a span already open around the writes the reference
// names.
type refGate interface {
	PrepareRef(root key.Key) (commit, abort func(), err error)
	ReleaseRef(root key.Key) error
}

func putRef(coll refGate, refs *refstore.Store, name string, root key.Key, raw []byte, exp expectation) error {
	var old *key.Key
	if prev, err := refs.Get(name); err == nil {
		prevRef, err := reference.Decode(prev)
		if err != nil {
			return fmt.Errorf("existing reference %q: %w", name, err)
		}
		k, err := key.Parse(prevRef.Key)
		if err != nil {
			return fmt.Errorf("existing reference %q: %w", name, err)
		}
		old = &k
	} else if !errors.Is(err, refstore.ErrNotFound) {
		return err
	}
	commit, abort, err := coll.PrepareRef(root)
	if err != nil {
		return err
	}
	var putErr error
	switch {
	case !exp.conditional:
		putErr = refs.Put(name, raw)
	case exp.absent:
		putErr = refs.Create(name, raw)
	default:
		putErr = refs.CompareAndSwap(name, exp.key, raw)
	}
	if putErr != nil {
		abort()
		return exp.explain(name, putErr)
	}
	commit()
	if exp.conditional {
		// What was overwritten is what the store compared against, whatever
		// the read above saw: the expected key, or nothing.
		old = nil
		if !exp.absent {
			old = &exp.key
		}
	}
	if old != nil {
		return coll.ReleaseRef(*old)
	}
	return nil
}

// rmRef deletes a reference and releases its root: the tails leave the
// union; the closure file goes if no other name shares the root. No walk.
func rmRef(coll *gc.Collector, refs *refstore.Store, name string, exp expectation) error {
	prev, err := refs.Get(name)
	if err != nil {
		return exp.explain(name, err)
	}
	ref, err := reference.Decode(prev)
	if err != nil {
		return fmt.Errorf("reference %q: %w", name, err)
	}
	root, err := key.Parse(ref.Key)
	if err != nil {
		return fmt.Errorf("reference %q: %w", name, err)
	}
	if exp.conditional {
		// What was deleted pointed at the expected key, whatever prev said.
		err, root = refs.CompareAndDelete(name, exp.key), exp.key
	} else {
		err = refs.Delete(name)
	}
	if err != nil {
		return exp.explain(name, err)
	}
	return coll.ReleaseRef(root)
}
