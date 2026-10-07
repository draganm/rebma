package main

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/urfave/cli/v2"
	"golang.org/x/sys/unix"
)

// lsPageSize bounds one ListEntries page; runLs loops until the listing is
// drained, so it only caps memory per fetch, not the output.
const lsPageSize = 4096

type lsConfig struct {
	keys bool
}

func lsCommand() *cli.Command {
	cfg := &lsConfig{}
	return &cli.Command{
		Name:      "ls",
		Usage:     "list the entries of the directory object KEY, or of the subdirectory PATH within it; accepts a reference as ref:NAME[@PATH]",
		ArgsUsage: "KEY[/PATH] | ref:NAME[@PATH]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "keys",
				Usage:       "append each entry's content key (usable as KEY for ls/export/restore)",
				Destination: &cfg.keys,
			},
		},
		Action: func(c *cli.Context) error { return runLs(c, cfg) },
	}
}

func runLs(c *cli.Context, cfg *lsConfig) error {
	if c.NArg() != 1 {
		return fmt.Errorf("ls requires exactly one KEY[/PATH] argument, got %d", c.NArg())
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
	var entries []fstree.Entry
	var after []byte
	for {
		page, more, err := fstree.ListEntries(dir, after, lsPageSize, objects.Get)
		if err != nil {
			return err
		}
		entries = append(entries, page...)
		if !more {
			break
		}
		after = page[len(page)-1].Name
	}
	return renderLs(c.App.Writer, entries, time.Now(), cfg.keys)
}

// renderLs writes one ls -l style line per entry: mode, uid, gid, size, mtime
// and name (with the symlink target after "->"). Numeric columns are
// right-aligned to the widest value. When showKeys is set, each entry's
// content key (if any) is appended after the name.
func renderLs(w io.Writer, entries []fstree.Entry, now time.Time, showKeys bool) error {
	uidW, gidW, sizeW := 0, 0, 0
	for _, e := range entries {
		uidW = max(uidW, len(strconv.FormatUint(e.UID, 10)))
		gidW = max(gidW, len(strconv.FormatUint(e.GID, 10)))
		sizeW = max(sizeW, len(sizeString(e)))
	}
	for _, e := range entries {
		name := string(e.Name)
		if len(e.LinkTarget) > 0 {
			name += " -> " + string(e.LinkTarget)
		}
		if showKeys && len(e.ContentKey) > 0 {
			if ck, err := key.Parse(e.ContentKey); err == nil {
				name += " " + ck.String()
			}
		}
		_, err := fmt.Fprintf(w, "%s %*d %*d %*s %s %s\n",
			modeString(e.Mode),
			uidW, e.UID,
			gidW, e.GID,
			sizeW, sizeString(e),
			formatMtime(time.Unix(0, e.Mtime), now),
			name,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// sizeString renders the size column: "major,minor" for device entries, the
// content length carried by the entry's key otherwise (a directory key's
// length counts its entries). Symlinks show their target length.
func sizeString(e fstree.Entry) string {
	if len(e.Rdev) == 2 {
		return fmt.Sprintf("%d,%d", e.Rdev[0], e.Rdev[1])
	}
	if len(e.ContentKey) > 0 {
		if ck, err := key.Parse(e.ContentKey); err == nil {
			return strconv.FormatUint(ck.Length(), 10)
		}
	}
	return strconv.Itoa(len(e.LinkTarget))
}

// modeString renders a raw POSIX st_mode the way ls -l does: a type character
// followed by nine permission characters, with setuid/setgid/sticky folded
// into the corresponding execute slots.
func modeString(mode uint64) string {
	var b [10]byte
	switch mode & unix.S_IFMT {
	case unix.S_IFDIR:
		b[0] = 'd'
	case unix.S_IFLNK:
		b[0] = 'l'
	case unix.S_IFCHR:
		b[0] = 'c'
	case unix.S_IFBLK:
		b[0] = 'b'
	case unix.S_IFIFO:
		b[0] = 'p'
	case unix.S_IFSOCK:
		b[0] = 's'
	case unix.S_IFREG:
		b[0] = '-'
	default:
		b[0] = '?'
	}
	rwx := "rwxrwxrwx"
	for i, c := range []byte(rwx) {
		if mode&(1<<uint(8-i)) != 0 {
			b[1+i] = c
		} else {
			b[1+i] = '-'
		}
	}
	setBit := func(pos int, bit uint64, withX, withoutX byte) {
		if mode&bit == 0 {
			return
		}
		if b[pos] == 'x' {
			b[pos] = withX
		} else {
			b[pos] = withoutX
		}
	}
	setBit(3, unix.S_ISUID, 's', 'S')
	setBit(6, unix.S_ISGID, 's', 'S')
	setBit(9, unix.S_ISVTX, 't', 'T')
	return string(b[:])
}

// formatMtime renders an mtime the way ls -l does: "Jan _2 15:04" for times
// within the last six months, "Jan _2  2006" for older or future times.
func formatMtime(t, now time.Time) string {
	if t.After(now) || t.Before(now.AddDate(0, -6, 0)) {
		return t.Format("Jan _2  2006")
	}
	return t.Format("Jan _2 15:04")
}
