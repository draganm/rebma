package main

import (
	"fmt"
	"io"
	"os"

	"github.com/draganm/rebma/tarexport"
	"github.com/urfave/cli/v2"
)

type exportConfig struct {
	output string
}

func exportCommand() *cli.Command {
	cfg := &exportConfig{}
	return &cli.Command{
		Name:      "export",
		Usage:     "write the tree rooted at KEY, or at the subdirectory PATH within it, as a PAX tar to stdout; accepts a reference as ref:NAME[@PATH]",
		ArgsUsage: "KEY[/PATH] | ref:NAME[@PATH]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				Usage:       "write the tar to FILE instead of stdout",
				Destination: &cfg.output,
			},
		},
		Action: func(c *cli.Context) error { return runExport(c, cfg) },
	}
}

func runExport(c *cli.Context, cfg *exportConfig) error {
	if c.NArg() != 1 {
		return fmt.Errorf("export requires exactly one KEY[/PATH] argument, got %d", c.NArg())
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)
	k, path, err := resolveSpec(refs, c.Args().First())
	if err != nil {
		return err
	}
	dir, err := descend(objects, k, path)
	if err != nil {
		return err
	}
	var w io.Writer = c.App.Writer
	if cfg.output != "" {
		f, err := os.Create(cfg.output)
		if err != nil {
			return err
		}
		if err := tarexport.Write(f, dir, objects.Get); err != nil {
			f.Close()
			return err
		}
		return f.Close() // surface a flush/close error on the happy path
	}
	return tarexport.Write(w, dir, objects.Get)
}
