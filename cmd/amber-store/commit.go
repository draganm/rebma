package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/draganm/rebma/commit"
	"github.com/draganm/rebma/fstree"
	"github.com/draganm/rebma/key"
	"github.com/draganm/rebma/packstore"
	"github.com/draganm/rebma/reference"
	"github.com/draganm/rebma/refstore"
	"github.com/urfave/cli/v2"
)

func commitCommand() *cli.Command {
	return &cli.Command{
		Name:  "commit",
		Usage: "create and inspect commits: a tree with its parent commits, author, committer and message",
		Subcommands: []*cli.Command{
			{
				Name:      "create",
				Usage:     "record the directory at TREE as a commit and print the commit key",
				ArgsUsage: "KEY[/PATH] | ref:NAME[@PATH]",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "message", Aliases: []string{"m"}, Usage: "commit message"},
					&cli.StringFlag{Name: "author", Usage: "author as 'Name <email>'", Required: true},
					&cli.StringFlag{Name: "committer", Usage: "committer as 'Name <email>' (default: the author)"},
					&cli.StringFlag{Name: "date", Usage: "author and committer time, RFC 3339 (default: now, local zone)"},
					&cli.StringSliceFlag{Name: "parent", Usage: "parent commit, KEY or ref:NAME; repeat for a merge, mainline first"},
					&cli.StringFlag{Name: "change-id", Usage: "change id as `HEX`, 1-64 bytes: an identity that follows the change when the commit is rewritten"},
					&cli.StringFlag{Name: "ref", Usage: "point reference NAME at the new commit"},
				},
				Action: runCommitCreate,
			},
			{
				Name:      "show",
				Usage:     "print the commit at KEY or ref:NAME",
				ArgsUsage: "KEY | ref:NAME",
				Action:    runCommitShow,
			},
		},
	}
}

// parseIdentity splits "Name <email>" into its parts. A string with no
// trailing "<...>" is all name; the name must not be empty.
func parseIdentity(s string) (commit.Identity, error) {
	s = strings.TrimSpace(s)
	name, email := s, ""
	if strings.HasSuffix(s, ">") {
		if i := strings.LastIndex(s, "<"); i >= 0 {
			name, email = strings.TrimSpace(s[:i]), s[i+1:len(s)-1]
		}
	}
	if name == "" {
		return commit.Identity{}, fmt.Errorf("identity %q has no name; want 'Name <email>'", s)
	}
	return commit.Identity{Name: name, Email: email}, nil
}

func runCommitCreate(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("commit create requires exactly one TREE argument, got %d", c.NArg())
	}
	author, err := parseIdentity(c.String("author"))
	if err != nil {
		return fmt.Errorf("--author: %w", err)
	}
	committer := author
	if s := c.String("committer"); s != "" {
		if committer, err = parseIdentity(s); err != nil {
			return fmt.Errorf("--committer: %w", err)
		}
	}
	when := time.Now()
	if s := c.String("date"); s != "" {
		if when, err = time.Parse(time.RFC3339, s); err != nil {
			return fmt.Errorf("--date: %w", err)
		}
	}
	_, offset := when.Zone()
	author.When, author.TZOffset = when.UnixNano(), offset/60
	committer.When, committer.TZOffset = when.UnixNano(), offset/60
	var changeID []byte
	if c.IsSet("change-id") {
		if changeID, err = hex.DecodeString(c.String("change-id")); err != nil {
			return fmt.Errorf("--change-id: %w", err)
		}
		if len(changeID) == 0 {
			return errors.New("--change-id: empty; leave the flag out for a commit without a change id")
		}
	}
	refName := c.String("ref")
	if refName != "" {
		if err := reference.ValidateName(refName); err != nil {
			return err
		}
	}

	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	k, err := createCommit(c, objects, refs, commit.Commit{
		Author:    author,
		Committer: committer,
		Message:   c.String("message"),
		ChangeID:  changeID,
	}, refName)
	if err := errors.Join(err, closeStore(objects, refs)); err != nil {
		return err
	}
	_, err = fmt.Fprintln(c.App.Writer, k.String())
	return err
}

// createCommit resolves the tree and parent specs into rec, stores the commit
// and, when refName is set, points that reference at it.
func createCommit(c *cli.Context, objects *packstore.Store, refs *refstore.Store, rec commit.Commit, refName string) (key.Key, error) {
	root, path, err := resolveSpec(refs, c.Args().First())
	if err != nil {
		return key.Key{}, err
	}
	target, err := descend(objects, root, path)
	if err != nil {
		return key.Key{}, err
	}
	// TREE may name a commit, as the root or through a directory entry that
	// holds one: the new commit records that commit's tree.
	if rec.Tree, err = fstree.DirOf(target, objects.Get); err != nil {
		return key.Key{}, err
	}
	for _, spec := range c.StringSlice("parent") {
		pk, ppath, err := resolveSpec(refs, spec)
		if err != nil {
			return key.Key{}, fmt.Errorf("--parent %s: %w", spec, err)
		}
		if ppath != "" {
			return key.Key{}, fmt.Errorf("--parent %s: a parent is a commit, not a path within one", spec)
		}
		rec.Parents = append(rec.Parents, pk)
	}
	k, raw, err := rec.Object()
	if err != nil {
		return key.Key{}, err
	}
	// One write span from the check that the children are there to the
	// reference that names the commit: no sweep in another process falls in
	// between.
	span, err := openSpan(c, objects, refs, refName != "")
	if err != nil {
		return key.Key{}, err
	}
	defer span.end()
	for _, child := range append([]key.Key{rec.Tree}, rec.Parents...) {
		ok, err := objects.Has(child)
		if err != nil {
			return key.Key{}, err
		}
		if !ok {
			return key.Key{}, fmt.Errorf("%s is not in the store", child)
		}
	}
	// A parent has to be a commit the graph walks accept. Bytes under a key of
	// the first release's rule still decode, but no reference could ever be
	// put on what is built on them.
	for _, p := range rec.Parents {
		data, err := objects.Get(p)
		if err != nil {
			return key.Key{}, err
		}
		if _, err := fstree.ChildKeys(p, data); err != nil {
			return key.Key{}, fmt.Errorf("parent %s: %w", p, err)
		}
	}
	if err := objects.Put(k, raw); err != nil {
		return key.Key{}, err
	}
	if refName == "" {
		return k, nil
	}
	ref := reference.Reference{Name: refName, Key: k[:], CreatedAt: time.Now().UnixNano()}
	refRaw, err := ref.Encode()
	if err == nil {
		err = putRef(span.refs, refs, refName, k, refRaw, expectation{})
	}
	if err != nil {
		return key.Key{}, fmt.Errorf("commit stored (%s) but setting reference %q failed: %w\nretry with: amber-store ref set %q %s",
			k, refName, err, refName, k)
	}
	return k, nil
}

func runCommitShow(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("commit show requires exactly one KEY or ref:NAME argument, got %d", c.NArg())
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
	if path != "" {
		return fmt.Errorf("commit show takes a commit, not a path within one")
	}
	if k.Type() != key.Commit {
		return fmt.Errorf("%s is not a commit (type %v)", k, k.Type())
	}
	data, err := objects.Get(k)
	if err != nil {
		return err
	}
	rec, err := commit.Decode(data)
	if err != nil {
		return fmt.Errorf("commit %s: %w", k, err)
	}
	return renderCommit(c.App.Writer, k, rec)
}

// renderCommit prints a commit in git's cat-file layout: headers, a blank
// line, then the message indented by four spaces. A conflicted tree shows its
// further terms after the tree, in recorded order (remove, add, remove, …),
// and the labels that are not empty, numbered from 0, the tree.
func renderCommit(w io.Writer, k key.Key, c commit.Commit) error {
	var b strings.Builder
	fmt.Fprintf(&b, "commit %s\ntree %s\n", k, c.Tree)
	for i, term := range c.ConflictTerms {
		side := "remove"
		if i%2 == 1 {
			side = "add"
		}
		fmt.Fprintf(&b, "conflict-%s %s\n", side, term)
	}
	for i, label := range c.ConflictLabels {
		if label != "" {
			fmt.Fprintf(&b, "conflict-label %d %s\n", i, label)
		}
	}
	for _, p := range c.Parents {
		fmt.Fprintf(&b, "parent %s\n", p)
	}
	if len(c.ChangeID) > 0 {
		fmt.Fprintf(&b, "change-id %x\n", c.ChangeID)
	}
	fmt.Fprintf(&b, "author %s\ncommitter %s\n", identityLine(c.Author), identityLine(c.Committer))
	if len(c.Signature) > 0 {
		fmt.Fprintf(&b, "signature %d bytes\n", len(c.Signature))
	}
	if msg := strings.TrimRight(c.Message, "\n"); msg != "" {
		b.WriteString("\n")
		for line := range strings.SplitSeq(msg, "\n") {
			fmt.Fprintf(&b, "    %s\n", line)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// identityLine renders "Name <email> time", the time in the identity's zone.
func identityLine(id commit.Identity) string {
	who := id.Name
	if id.Email != "" {
		if who != "" {
			who += " "
		}
		who += "<" + id.Email + ">"
	}
	zone := time.FixedZone("", id.TZOffset*60)
	when := time.Unix(0, id.When).In(zone).Format(time.RFC3339)
	if who == "" { // an identity may name nobody at all
		return when
	}
	return who + " " + when
}
