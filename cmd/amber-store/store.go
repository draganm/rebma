package main

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/draganm/rebma/gc"
	"github.com/draganm/rebma/packstore"
	"github.com/draganm/rebma/refstore"
	"github.com/urfave/cli/v2"
)

// openStore opens (creating as needed) the store directory named by the
// --store flag or $AMBER_STORE: <dir>/packstore holds the objects,
// <dir>/refs the references DB. Any number of processes may have one store
// open at once (architecture/packstore.md, architecture/references.md).
func openStore(c *cli.Context) (*packstore.Store, *refstore.Store, error) {
	dir := c.String("store")
	if dir == "" {
		return nil, nil, fmt.Errorf("no store directory: set --store or $AMBER_STORE")
	}
	objects, err := packstore.Open(filepath.Join(dir, "packstore"),
		packstore.WithSync(true), packstore.WithSegmentSize(c.Int64("segment-size")))
	if err != nil {
		return nil, nil, err
	}
	refs, err := refstore.Open(filepath.Join(dir, "refs"), true)
	if err != nil {
		objects.Close()
		return nil, nil, err
	}
	return objects, refs, nil
}

// closeStore closes both halves, keeping every error.
func closeStore(objects *packstore.Store, refs *refstore.Store) error {
	return errors.Join(refs.Close(), objects.Close())
}

// openCollector opens the collector next to an already-open store pair;
// <dir>/closures holds the closure files. Close it before closeStore.
// cliSpan is one write span over a command's object writes and, when the
// command names them, the reference put.
type cliSpan struct {
	refs refGate // nil without a reference to put
	end  func() error
}

// openSpan opens the span. With a reference to put it is the collector's
// (gc.Collector.BeginSpan), which takes the reference lock before the store's
// gate, the order a cycle takes them in; without one, the store's own.
func openSpan(c *cli.Context, objects *packstore.Store, refs *refstore.Store, withRef bool) (cliSpan, error) {
	if !withRef {
		end, err := objects.BeginWrite()
		if err != nil {
			return cliSpan{}, err
		}
		return cliSpan{end: func() error { end(); return nil }}, nil
	}
	coll, err := openCollector(c, objects, refs, gc.Options{})
	if err != nil {
		return cliSpan{}, err
	}
	span, err := coll.BeginSpan()
	if err != nil {
		return cliSpan{}, errors.Join(err, coll.Close())
	}
	return cliSpan{refs: span, end: func() error { span.End(); return coll.Close() }}, nil
}

func openCollector(c *cli.Context, objects *packstore.Store, refs *refstore.Store, opts gc.Options) (*gc.Collector, error) {
	return gc.Open(filepath.Join(c.String("store"), "closures"), objects, refs, opts)
}
