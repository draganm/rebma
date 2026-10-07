# rebma

**rebma** is [Amber-Store Core](https://github.com/amber-store/core) with the
bytes of every key **reversed**: a key leads with its truncated hash and ends
with the type/length header ([`architecture/keys.md`](architecture/keys.md)).
The Go API, the CLI and the object encodings are otherwise Amber-Store Core's;
only the module path (`github.com/draganm/rebma`) differs. Because the keys
differ, so does everything derived from them — root keys, pack streams and
store directories are **not interchangeable** with Amber-Store's, and a store
directory must only ever be opened by one of the two.

A content-addressable store for **filesystem trees** — arbitrarily deep
directories and files, where file content is split by content-defined chunking and
every object is identified by a fixed 32-byte key derived from a hash of its
content.

> **Status:** working implementation. This is the **local core**: a Go library
> (plus a minimal CLI) that owns a store in a local directory — no sockets, no
> networking, no authentication. It is the foundation the amber server is
> built on, and it embeds directly into other projects (e.g. the JOBS engine).
> The design is specified in [`architecture/`](architecture/).

## What it is

Amber-Store models a POSIX-style filesystem as an immutable, deduplicated
[Merkle](https://en.wikipedia.org/wiki/Merkle_tree) structure in a content-addressed
store:

- **Files** are split into chunks by content-defined chunking (CDC); each chunk is a
  `Blob`. Large files get a multi-level `FileNode` index for O(log n) random-access
  seek.
- **Directories** are [prolly trees](https://docs.dolthub.com/architecture/storage-engine/prolly-tree)
  — sorted maps from name to entry, chunked at entry boundaries — so a directory
  with **>100K entries can be looked up, iterated, and mutated with sub-O(n)
  memory**.
- **Metadata** (type, mode, uid, gid, mtime, optional xattrs) lives in the parent
  directory entry. Symlinks and special files are stored inline; regular files and
  subdirectories reference content by key.
- **Deduplication is automatic**: identical content yields an identical key, and
  editing one entry re-writes only the O(log n) objects on its path — the rest is
  structurally shared with the previous tree.

## Design goals

- Very large directories (>100K entries) processed with **less than O(n)** memory.
- Directory entries carry all filesystem metadata; file content is reached through
  the store by key.
- Deterministic, implementation-independent encoding so any reader recomputes the
  same key for the same content.

## Object types

| Type       | Role                                                        |
|------------|-------------------------------------------------------------|
| `Blob`     | Raw file-content byte chunk (a CDC leaf).                   |
| `FileNode` | File chunk-index node (file content tree).                  |
| `DirLeaf`  | A run of complete directory entries (prolly-tree leaf).     |
| `DirNode`  | Directory index node (directory tree).                      |
| `XattrSet` | Spilled extended attributes, when too large to inline.      |
| `Commit`   | Snapshot record: a tree (or the sides of a conflict), parent commits, author, committer, message, change id. |

## Encoding

- **Keys** are 32 bytes: a truncated
  [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) hash of the content, then a
  length and a type/length header — an Amber-Store key, byte-reversed.
- **Structured objects** use **deterministic CBOR**
  ([RFC 8949 §4.2](https://www.rfc-editor.org/rfc/rfc8949#name-deterministically-encoded-c));
  `Blob`s are raw bytes. Deterministic encoding is required because the key's hash
  is taken over the serialized bytes.

## The library

The packages compose loosely; consumers wire them together and own their
store-directory layout. The conventional layout (which the CLI uses) is
`<dir>/packstore` for objects and `<dir>/refs` for references. A store
directory may be open in **any number of processes at once**: readers take no
lock, each writer owns an active segment of its own, and a GC cycle keeps the
writers of other processes waiting while it runs
([architecture/packstore.md](architecture/packstore.md)). They must all run on
one host, on a local filesystem.

| Package | Role |
|---------|------|
| `key` | The 32-byte content key: type, length, truncated BLAKE3 hash. |
| `fstree` | Tree objects (encode/decode), bottom-up builders, and the read paths: entry lookup, ordered listing, content streaming, reachable-set walks, completeness checks. A commit key reads as its tree, as a root and as the content key of a directory entry. |
| `commit` | The commit record: canonical CBOR encoding and validation; a change id and conflicted trees for jj; signature fields carried opaquely. |
| `chunkers` | Content-defined byte chunking (ultracdc) and item chunking for tree nodes. |
| `ingest` | Build a tree from a local directory (or single file): `Objects` streams every built object plus the resolved root; `Dir` writes straight into a packstore; `Scan` sizes progress displays. Honors `.amberignore`; `Opts.Exclude` skips names at the root (a working copy's metadata directory). |
| `amberignore` | `.gitignore`-semantics exclusion for ingestion. |
| `packstore` | The local object store: append-only pack segments with parallel, deduplicating, verifying writers, shared by any number of processes. |
| `refstore` | SQLite-backed (WAL, multi-process) name → record map for references, with optimistic updates. |
| `reference` | The reference record: canonical CBOR encoding and validation; signature fields carried opaquely. |
| `amberpack` | The flat pack stream format (`key + payload` records, no root) used for transfer and storage. |
| `inbox` | Durable pack receiving: persist incoming packs, then drain them into a packstore. |
| `tarexport` | Stream a stored tree as a PAX tar. |
| `tarextract` | Materialize such a tar onto the filesystem, restoring metadata. |
| `cborx` | Shared deterministic-CBOR helpers. |

A minimal embedding looks like:

```go
objects, _ := packstore.Open(filepath.Join(dir, "packstore"))
defer objects.Close()

root, stats, _ := ingest.Dir(objects, "./some/dir", ingest.Opts{})
_ = tarexport.Write(w, root, objects.Get)
```

## The CLI

Every command operates directly on the store directory given by `--store` or
`$AMBER_STORE`, creating it as needed. Ingest a directory or a single file —
the root key (hex) is printed to stdout; a directory root is a `DirNode`, a
single-file root is the file's content key:

```sh
amber-store --store ./store ingest ./some/dir           # print the root key
amber-store --store ./store ingest --ref backups/home ./some/dir  # also name it
```

Inspect and export by key or reference, optionally addressing a subdirectory
with `KEY/PATH` or `ref:NAME[@PATH]`:

```sh
amber-store --store ./store ls KEY[/PATH]               # list entries, ls -l style (--keys adds content keys)
amber-store --store ./store ls ref:backups/home@sub/dir # ref:NAME[@PATH] works wherever KEY[/PATH] does
amber-store --store ./store export ref:backups/home -o tree.tar  # PAX tar (default: stdout)
amber-store --store ./store restore ref:backups/home ./dest      # recreate the tree on disk
amber-store --store ./store ref list                    # references: name, key, created, user
amber-store --store ./store ref set NAME KEY            # name an existing key
amber-store --store ./store ref set --expect OLD NAME KEY  # move NAME only if it still points at OLD ('none': only create)
amber-store --store ./store ref get NAME                # print the key a name points at
amber-store --store ./store ref rm NAME                 # delete the name; objects stay
amber-store --store ./store commit create --ref main --author 'Ann <ann@example.com>' -m 'first' KEY  # record a tree; --parent KEY|ref:NAME links history; --change-id HEX carries a change id
amber-store --store ./store commit show ref:main        # print a commit, git cat-file style
```

A commit key, or a reference to one, works wherever a directory `KEY` does — it
stands for the commit's tree. So does a directory entry that holds a commit:
`ls`, `export` and `restore` skip the commit object and continue with its tree.

Ingest parallelism is set with `--jobs` (default: number of CPUs). Chunking is
tunable with `--min/--avg/--max` (ultracdc byte chunking) and `--item-bits`
(index/entry chunking); `--xattr-inline-max` controls when extended attributes
spill to an `XattrSet` object.

`.amberignore` files exclude entries from ingestion, with `.gitignore`
semantics: negation (`!pattern`), `**` globs, directory-only (`name/`) and
anchored (`/name`) patterns; a file in any subdirectory applies to that
subtree and composes with inherited patterns (last match wins). Ignored
directories are pruned without being read. The `.amberignore` files
themselves are always stored, so a restored tree re-ingests to the same
root. `--no-ignore` disables all ignore processing.

## Architecture

| Document | Contents |
|----------|----------|
| [`architecture/keys.md`](architecture/keys.md)   | The 32-byte lookup key: truncated hash, payload length, header byte. |
| [`architecture/types.md`](architecture/types.md) | The type model: object types, filesystem entry types, length-field semantics. |
| [`architecture/fstree.md`](architecture/fstree.md) | On-the-wire CBOR layout of every type, the chunkers, tree construction, and read paths. |
| [`architecture/amberpack.md`](architecture/amberpack.md) | The flat pack stream: record framing, CRCs, recovery. |
| [`architecture/packstore.md`](architecture/packstore.md) | The store directory, the sidecar index of active segments, and the rules for sharing a store between processes. |
| [`architecture/references.md`](architecture/references.md) | Named pointers to keys: record layout, name rules, storage. |
| [`architecture/commits.md`](architecture/commits.md) | The commit object: record layout, conflicts and the change id, the footprint length, signing convention, reachability, commits inside directories, golden vectors. |

## Development

The repository uses a [Nix](https://nixos.org/) flake (with
[direnv](https://direnv.net/)) to provide the Go toolchain.

```sh
direnv allow        # or: nix develop
go build ./...
go test ./...
```

- Module: `github.com/draganm/rebma`
- Go: 1.26+

## License

Licensed under the GNU Lesser General Public License, version 3 only
(`LGPL-3.0-only`). See [`LICENSE`](LICENSE) for the LGPL terms and
[`COPYING`](COPYING) for the GPL terms incorporated by the LGPL.

Third-party notices retain their stated licenses.
