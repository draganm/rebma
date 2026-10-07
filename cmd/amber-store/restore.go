package main

import (
	"fmt"
	"io"

	"github.com/draganm/rebma/tarexport"
	"github.com/draganm/rebma/tarextract"
	"github.com/urfave/cli/v2"
)

func restoreCommand() *cli.Command {
	return &cli.Command{
		Name:      "restore",
		Usage:     "restore the filesystem tree rooted at KEY, or at the subdirectory PATH within it, into DIR; accepts a reference as ref:NAME[@PATH]",
		ArgsUsage: "(KEY[/PATH] | ref:NAME[@PATH]) DIR",
		Action:    runRestore,
	}
}

func runRestore(c *cli.Context) error {
	if c.NArg() != 2 {
		return fmt.Errorf("restore requires exactly two arguments KEY[/PATH] and DIR, got %d", c.NArg())
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)
	k, path, err := resolveSpec(refs, c.Args().Get(0))
	if err != nil {
		return err
	}
	dir, err := descend(objects, k, path)
	if err != nil {
		return err
	}

	// Stream the export straight into the extractor. A Write error closes the
	// pipe with its cause, so Extract surfaces it.
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(tarexport.Write(pw, dir, objects.Get))
	}()
	if err := tarextract.Extract(pr, c.Args().Get(1)); err != nil {
		return err
	}
	return pr.Close()
}
