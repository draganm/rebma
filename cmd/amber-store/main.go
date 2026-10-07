package main

import (
	"fmt"
	"os"

	"github.com/draganm/rebma/chunkers"
	"github.com/draganm/rebma/ingest"
	"github.com/draganm/rebma/packstore"
	"github.com/urfave/cli/v2"
)

// Entry point of the application.
func main() {
	if err := newApp().Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "amber-store: %v\n", err)
		os.Exit(1)
	}
}

// newApp creates and configures the CLI application. Every command operates
// directly on the store directory named by --store / $AMBER_STORE.
func newApp() *cli.App {
	return &cli.App{
		Name:  "amber-store",
		Usage: "local content-addressed filesystem tree store",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "store",
				Usage:   "store directory (layout: <dir>/packstore, <dir>/refs)",
				EnvVars: []string{"AMBER_STORE"},
			},
			&cli.Int64Flag{
				Name:  "segment-size",
				Usage: "pack segment size in bytes; the reaping granularity",
				Value: packstore.DefaultSegmentSize,
			},
		},
		Commands: []*cli.Command{
			ingestCommand(),
			lsCommand(),
			exportCommand(),
			restoreCommand(),
			refCommand(),
			commitCommand(),
			gcCommand(),
		},
	}
}

// chunkConfig holds the content-defined-chunking parameters used by the ingest
// command when building the tree.
type chunkConfig struct {
	min            int
	avg            int
	max            int
	itemBits       int
	xattrInlineMax int
}

// chunkOpts maps the CLI chunking flags onto the library options. min/avg/max
// must all be set together or all left unset (the library defaults).
func (cc *chunkConfig) chunkOpts() (ingest.ChunkOpts, error) {
	opts := ingest.ChunkOpts{ItemBits: cc.itemBits, XattrInlineMax: cc.xattrInlineMax}
	if cc.min == 0 && cc.avg == 0 && cc.max == 0 {
		return opts, nil
	}
	if cc.min <= 0 || cc.avg <= 0 || cc.max <= 0 {
		return ingest.ChunkOpts{}, fmt.Errorf("--min, --avg and --max must all be set together")
	}
	opts.Byte = &chunkers.ByteOpts{MinSize: cc.min, NormalSize: cc.avg, MaxSize: cc.max}
	return opts, nil
}

// chunkFlags returns the CLI flags that fill cc.
func chunkFlags(cc *chunkConfig) []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{
			Name:        "min",
			Usage:       "ultracdc minimum chunk size in bytes",
			Destination: &cc.min,
			Value:       chunkers.DefaultMinSize,
		},
		&cli.IntFlag{
			Name:        "avg",
			Usage:       "ultracdc average (normal) chunk size in bytes",
			Destination: &cc.avg,
			Value:       chunkers.DefaultNormalSize,
		},
		&cli.IntFlag{
			Name:        "max",
			Usage:       "ultracdc maximum chunk size in bytes",
			Destination: &cc.max,
			Value:       chunkers.DefaultMaxSize,
		},
		&cli.IntFlag{
			Name:        "item-bits",
			Value:       ingest.DefaultItemBits,
			Usage:       "item chunker average run = 2^bits",
			Destination: &cc.itemBits,
		},
		&cli.IntFlag{
			Name:        "xattr-inline-max",
			Value:       ingest.DefaultXattrInlineMax,
			Usage:       "xattrs larger than this many bytes spill to an XattrSet",
			Destination: &cc.xattrInlineMax,
		},
	}
}
