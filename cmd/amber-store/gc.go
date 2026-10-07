package main

import (
	"fmt"
	"time"

	"github.com/draganm/rebma/gc"
	"github.com/urfave/cli/v2"
)

type gcRunConfig struct {
	garbage float64
	grace   time.Duration
	rate    int64
	minFree uint64
}

func gcCommand() *cli.Command {
	cfg := &gcRunConfig{}
	return &cli.Command{
		Name:  "gc",
		Usage: "garbage collection: score packs, reap the mostly-dead ones",
		Subcommands: []*cli.Command{
			{
				Name:   "status",
				Usage:  "packs: id, sealed, bytes, garbage, eligible; totals; closures; union; last cycle",
				Action: runGCStatus,
			},
			{
				Name:  "run",
				Usage: "score now, reap packs above the garbage line",
				Flags: []cli.Flag{
					&cli.Float64Flag{
						Name:        "garbage",
						Usage:       "force the selection line (fraction; default: 0.5, or 0.1 under min-free pressure)",
						Destination: &cfg.garbage,
						Value:       -1,
					},
					&cli.DurationFlag{
						Name:        "grace",
						Usage:       "minimum age of a sealed pack before it can be reaped",
						Destination: &cfg.grace,
						Value:       time.Hour,
					},
					&cli.Int64Flag{
						Name:        "rate",
						Usage:       "copier bandwidth cap in bytes/s (0 = unlimited)",
						Destination: &cfg.rate,
					},
					&cli.Uint64Flag{
						Name:        "min-free",
						Usage:       "free-space floor in bytes (0 = 5% of the filesystem)",
						Destination: &cfg.minFree,
					},
				},
				Action: cfg.runGCRun,
			},
			{
				Name:      "why",
				Usage:     "references whose closure holds KEY's tail",
				ArgsUsage: "KEY",
				Action:    runGCWhy,
			},
		},
	}
}

func runGCStatus(c *cli.Context) error {
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)
	coll, err := openCollector(c, objects, refs, gc.Options{})
	if err != nil {
		return err
	}
	defer coll.Close()
	st, err := coll.Status(c.Context)
	if err != nil {
		return err
	}
	w := c.App.Writer
	fmt.Fprintf(w, "%-16s  %-20s  %10s  %7s  %s\n", "PACK", "SEALED", "BYTES", "GARBAGE", "ELIGIBLE")
	for _, p := range st.Packs {
		fmt.Fprintf(w, "%016x  %-20s  %10s  %6.1f%%  %v\n",
			p.ID, p.Sealed.Format(time.RFC3339), humanBytes(uint64(p.Body)), 100*p.Garbage, p.Eligible)
	}
	fmt.Fprintf(w, "live %s, garbage %s; %d refs, %d live objects marked\n",
		humanBytes(uint64(st.LiveBytes)), humanBytes(uint64(max(st.GarbageBytes, 0))),
		st.Refs, st.Marked)
	if st.Last != nil {
		fmt.Fprintf(w, "last cycle: %s, %d packs scored, %d reaped, %s copied, %s freed\n",
			st.Last.Start.Format(time.RFC3339), st.Last.Scored, len(st.Last.Reaped),
			humanBytes(uint64(st.Last.CopiedBytes)), humanBytes(uint64(max(st.Last.FreedBytes, 0))))
	}
	if st.LastError != "" {
		fmt.Fprintf(w, "last cycle error: %s\n", st.LastError)
	}
	return nil
}

func (cfg *gcRunConfig) runGCRun(c *cli.Context) error {
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)
	coll, err := openCollector(c, objects, refs, gc.Options{
		Grace:   cfg.grace,
		MinFree: cfg.minFree,
		Rate:    cfg.rate,
	})
	if err != nil {
		return err
	}
	defer coll.Close()
	stats, err := coll.Run(c.Context, cfg.garbage)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "%d packs scored, %d reaped, %d records (%s) copied, %s freed in %s (mark %s, sweep %s; %d objects marked)\n",
		stats.Scored, len(stats.Reaped), stats.CopiedRecords,
		humanBytes(uint64(stats.CopiedBytes)), humanBytes(uint64(max(stats.FreedBytes, 0))), stats.Duration.Round(time.Millisecond),
		stats.MarkDuration.Round(time.Millisecond), stats.SweepDuration.Round(time.Millisecond), stats.Marked)
	return nil
}

func runGCWhy(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("gc why requires exactly one KEY argument, got %d", c.NArg())
	}
	k, err := parseHexKey(c.Args().First())
	if err != nil {
		return err
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)
	coll, err := openCollector(c, objects, refs, gc.Options{})
	if err != nil {
		return err
	}
	defer coll.Close()
	names, err := coll.Why(k)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		fmt.Fprintln(c.App.Writer, "unreferenced")
		return nil
	}
	for _, n := range names {
		fmt.Fprintln(c.App.Writer, n)
	}
	return nil
}
