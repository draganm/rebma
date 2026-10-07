package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/draganm/rebma/ingest"
	"github.com/draganm/rebma/reference"
	"github.com/urfave/cli/v2"
)

type ingestConfig struct {
	chunk      chunkConfig
	ref        string
	jobs       int
	noIgnore   bool
	noProgress bool
}

func ingestCommand() *cli.Command {
	cfg := &ingestConfig{}
	flags := append(chunkFlags(&cfg.chunk),
		&cli.StringFlag{
			Name:        "ref",
			Usage:       "record the resolved root under reference NAME",
			Destination: &cfg.ref,
		},
		&cli.IntFlag{
			Name:        "jobs",
			Aliases:     []string{"j"},
			Value:       runtime.GOMAXPROCS(0),
			Usage:       "concurrent workers building the tree (default: number of CPUs)",
			Destination: &cfg.jobs,
		},
		&cli.BoolFlag{
			Name:        "no-ignore",
			Usage:       "do not honor .amberignore files",
			Destination: &cfg.noIgnore,
		},
		&cli.BoolFlag{
			Name:        "no-progress",
			Usage:       "disable the progress bar",
			Destination: &cfg.noProgress,
		},
	)
	return &cli.Command{
		Name:      "ingest",
		Usage:     "build the content-addressed tree for PATH (a directory or a single file), store it, and print the root key",
		ArgsUsage: "PATH",
		Flags:     flags,
		Action:    func(c *cli.Context) error { return runIngest(c, cfg) },
	}
}

// Handles the 'ingest' command.
func runIngest(c *cli.Context, cfg *ingestConfig) error {
	if c.NArg() != 1 {
		return fmt.Errorf("ingest requires exactly one PATH argument, got %d", c.NArg())
	}
	path := c.Args().First()
	if cfg.ref != "" {
		if err := reference.ValidateName(cfg.ref); err != nil {
			return err
		}
	}
	chunkOpts, err := cfg.chunk.chunkOpts()
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("PATH argument: %w", err)
	}

	// Progress is sized by a cheap pre-scan, unless disabled.
	var prog *Progress
	var pwg sync.WaitGroup
	ctx, cancel := context.WithCancel(c.Context)
	// LIFO: cancel() runs first to stop the progress goroutine, then pwg.Wait()
	// drains it — so every return path (including early errors) tears it down.
	defer pwg.Wait()
	defer cancel()
	if !cfg.noProgress {
		totalFiles, totalBytes := int64(1), info.Size()
		if info.IsDir() {
			totalFiles, totalBytes, err = ingest.Scan(path, cfg.noIgnore, cfg.jobs)
			if err != nil {
				return err
			}
		}
		prog = NewProgress(totalFiles, totalBytes)
		isTTY := isTerminal(os.Stderr)
		start := time.Now()
		pwg.Go(func() { prog.Run(ctx, os.Stderr, start, isTTY) })
	}

	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	opts := ingest.Opts{Jobs: cfg.jobs, Chunk: chunkOpts, NoIgnore: cfg.noIgnore}
	if prog != nil {
		opts.Progress = prog
	}
	// One write span over the ingest and the reference that names it: a GC
	// cycle in another process cannot fall between the two and find objects
	// whose reference is still to come, which only the grace period would
	// protect.
	span, err := openSpan(c, objects, refs, cfg.ref != "")
	if err != nil {
		closeStore(objects, refs)
		return err
	}
	root, _, err := ingest.Dir(objects, path, opts)
	if err != nil {
		span.end()
		closeStore(objects, refs)
		return err
	}
	if cfg.ref != "" {
		rec := reference.Reference{
			Name:      cfg.ref,
			Key:       root[:],
			CreatedAt: time.Now().UnixNano(),
		}
		raw, err := rec.Encode()
		if err == nil {
			err = putRef(span.refs, refs, cfg.ref, root, raw, expectation{})
		}
		if err != nil {
			span.end()
			closeStore(objects, refs)
			return fmt.Errorf("tree stored (root %s) but creating reference %q failed: %w\nretry with: amber-store ref set %q %s",
				root, cfg.ref, err, cfg.ref, root)
		}
	}
	if err := span.end(); err != nil {
		closeStore(objects, refs)
		return err
	}
	if err := closeStore(objects, refs); err != nil {
		return err
	}

	// Tear down progress before printing, so the root is not repainted over
	// by the progress goroutine's \r\033[K frames.
	cancel()
	pwg.Wait()
	fmt.Fprintf(c.App.Writer, "%s\n", root.String())
	return nil
}
