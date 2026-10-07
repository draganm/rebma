# rebma — project notes

rebma is amber-store/core with reversed key bytes (`architecture/keys.md`):
hash first, header byte last. Package, command and file-format names are
kept from amber-store/core.

## Dev environment

The Nix dev shell (`flake.nix`) provides `go`, `nodejs` (builds the embedded
admin SPA via `go generate ./cmd/amber-store`), and `python3`.

## Benchmarks

`cmd/amber-bench` is the ingest → delete → gc benchmark; its doc comment
describes the workload and every flag. `go test ./cmd/amber-bench` runs the
whole pipeline at smoke scale (30 refs, 0.1× sizes, 4 MiB packs; ~8 s,
builds the CLI itself). Full scale needs ~100 GiB of disk and a few minutes:

```sh
go build -o /tmp/amber-store ./cmd/amber-store
go run ./cmd/amber-bench -data /tmp/bench/data -store /tmp/bench/store \
    -restore /tmp/bench/restore -bin /tmp/amber-store -out /tmp/bench/results.json
go run ./cmd/amber-bench -phase report -out /tmp/bench/results.json   # reprint
```

The dataset is seeded, so reruns get the identical files and pack layout;
delete the data, store and binary afterwards.

### What it measures

- **Workload.** 1000 references, ~50 GiB unique: 300 "kept" refs of 100 MiB
  fresh random data and 700 "deleted" refs of 30 MiB, interleaved (kept =
  `i%10 < 3`). Every ref also holds whole-file reflinks of ~⅓ of its fresh
  size from the previous ref, so ~25 % of the 66 GiB ingested is duplicate,
  and kept refs share data with deleted neighbours. Files 256 KiB–4 MiB.
- **Ingest rate** (in-process `ingest.Dir` + collector `PrepareRef`, as the
  CLI/daemon do), per-ref timings, dedup counts.
- **Delete** of the 700 refs through `ReleaseRef`.
- **GC**: `amber-store gc run --grace 1s` under policy (0.5 garbage line),
  then a forced `--garbage 0` pass; `gc status` and `du` around each step;
  nominal vs really reclaimed bytes.
- **Integrity**: `fstree.CheckComplete` on every survivor, and a sample of
  kept refs restored through the CLI and diffed against the source.

### Results, 2026-08-25 (branch `simple-gc`)

Apple-silicon Mac, 14 cores, 48 GiB RAM, APFS SSD; 256 MiB segments, fsync
on, default chunking. Dataset: 66.16 GiB ingested, 49.80 GiB unique, 24.7 %
duplicate, 35,128 files. 200 packs after ingest.

| step | sequential copier (f7821fc) | pipelined copier (f739c7d) |
| --- | --- | --- |
| ingest 1000 refs | 120.5 s, 562 MiB/s avg (900 MiB/s steady, then memory-pressure stalls) | 71.3 s, 950 MiB/s avg, 716 MiB/s new bytes |
| ref put (closure walk) | 15 → 24 ms/ref over the run | 15 → 18 ms/ref |
| delete 700 refs | 9.6 s (13.7 ms/ref) | 10.9 s (15.5 ms/ref) |
| `gc run` (0.5 line): 199 scored, 63–64 reaped, 5.6 GiB copied, 16 GiB freed | **18.0 s** | **8.6 s** |
| `gc run --garbage 0`: 106–107 reaped, 19.5 GiB copied, 26.6 GiB freed | **53.5 s** | **33.0 s** |
| copier throughput | ~330–370 MiB/s | ~600–680 MiB/s |
| packstore on disk: ingest → policy → forced | 49.96 → 39.69 → 32.52 GiB | 49.90 → 39.77 → 32.52 GiB |
| integrity | 300/300 complete, 10 restores identical | same |

What "really got deleted": the 700 refs nominally own 20.5 GiB, but 3.2 GiB
of it stays live through the kept refs' copies (the collector reports
garbage 17.3 GiB right after the deletes). The policy run frees ~10.2 GiB
on disk — it deletes 16 GiB of packs but re-appends 5.6 GiB of survivors —
and leaves 7.1 GiB of garbage in the 105 packs under the 50 % line; the
forced pass reclaims the rest, 17.4 GiB in total (85 % of nominal), copying
19.5 GiB to free 7.2 GiB net.

Where the time goes: scoring is parallel and negligible; a cycle is the
copy loop. With the copy pipelined across `Jobs` workers it sits at the
same ceiling as ingest's new-byte rate — appends and `syncActive`'s fsync
both serialize on the packstore's append lock — so the next lever is the
fsync under that lock, not more workers. Delete cost is the union rebuild
per `ReleaseRef`. The first run's ingest tail (stalls up to 3 s on single
refs from ref ~600 on) was macOS memory pressure — 67 GiB of fresh source
plus 50 GiB of mmapped packs against 48 GiB of RAM — not the store; the
rerun did not reproduce it.

### Results, 2026-08-25, Linux (branch `simple-gc`, same commit as pipelined)

i7-1280P laptop (14 cores / 20 threads), 62 GiB RAM, NVMe, ext4 on
`/tmp` (no reflinks, so gen writes the shared files as full copies —
identical bytes, dataset just costs the full 66 GiB on disk); same
defaults (256 MiB segments, fsync on). Seeding held: same 200 packs after
ingest and the same on-disk trajectory 49.90 → 39.74 → 32.52 GiB.

| step | Linux i7-1280P | Mac pipelined (f739c7d) |
| --- | --- | --- |
| ingest 1000 refs | 66.4 s, 1021 MiB/s logical, 769 MiB/s new bytes | 71.3 s, 950 / 716 MiB/s |
| ref put (closure walk) | 3.1 → 6.6 ms/ref over the run | 15 → 18 ms/ref |
| delete 700 refs | 2.5 s (3.6 ms/ref) | 10.9 s (15.5 ms/ref) |
| `gc run` (0.5 line): 63 reaped, 5.6 GiB copied, 15.8 GiB freed | **6.0 s** | **8.6 s** |
| `gc run --garbage 0`: 107 reaped, 19.5 GiB copied, 26.8 GiB freed | **20.2 s** | **33.0 s** |
| copier throughput | ~990–1000 MiB/s | ~600–680 MiB/s |
| integrity | 300/300 complete, 10 restores identical | same |

Notably the copier runs *above* ingest's new-byte rate here (~1000 vs
769 MiB/s) — on ext4 the fsync under the append lock is cheaper than on
APFS, so the Mac conclusion "next lever is the fsync" is
filesystem-dependent. Ref put and delete are 3–4× faster (Pebble writes
with `pebble.Sync`, so the same fsync story). No memory-pressure stalls:
ingest decayed gently
1095 → 965 MiB/s over the run with per-ref maxima ≤ 139 ms.

### Results, 2026-08-25, Linux arm64 (branch `simple-gc`, same commit)

ASUS Ascent GX10 (NVIDIA GB10: 10× Cortex-X925 + 10× Cortex-A725,
20 cores), 119 GiB RAM, NVMe, ext4 (no reflinks, full 66 GiB dataset on
disk); same defaults. Seeding held: same dataset and 200 packs after
ingest; the policy pass reaped 64 packs here (i7 reaped 63, Mac 63–64),
so the mid trajectory differs slightly: 49.90 → 39.62 → 32.52 GiB.

| step | GX10 (GB10 arm64) | Linux i7-1280P |
| --- | --- | --- |
| ingest 1000 refs | 114.7 s, 591 MiB/s logical, 445 MiB/s new bytes | 66.4 s, 1021 / 769 MiB/s |
| ref put (closure walk) | 14.4 → 17.2 ms/ref over the run | 3.1 → 6.6 ms/ref |
| delete 700 refs | 7.3 s (10.4 ms/ref) | 2.5 s (3.6 ms/ref) |
| `gc run` (0.5 line): 64 reaped, 5.7 GiB copied, 16.0 GiB freed | **11.6 s** | 6.0 s (63 reaped) |
| `gc run --garbage 0`: 106 reaped, 19.4 GiB copied, 26.5 GiB freed | **40.4 s** | 20.2 s (107 reaped) |
| copier throughput | ~490–510 MiB/s | ~990–1000 MiB/s |
| integrity | 300/300 complete, 10 restores identical | same |

Roughly half the i7's throughput across the board, with the same shape:
the copier again sits just above ingest's new-byte rate (~500 vs
445 MiB/s), consistent with both serializing on the packstore append
lock + fsync — the ceiling is per-core/fsync speed, which the GB10's
efficiency-heavy core mix and NVMe path deliver less of than the
i7-1280P despite 20 cores. Ref put and delete land near the Mac's
numbers, not the i7's, so the Pebble `pebble.Sync` write cost here is
APFS-like. Ingest decayed gently 615 → 562 MiB/s with per-ref maxima
≤ 249 ms; no stalls (dataset + packs fit the 119 GiB page cache).

### Results, 2026-08-25, Linux: simple-gc vs mark-sweep-gc (PR #2)

Same i7-1280P laptop as above; side-by-side of `main` (95b8542,
simple-gc) against branch `mark-sweep-gc` (1b2e0a4, the port of Mic92's
bitmap collector from draganm/amber-store#9), same seeded dataset. Both
arms ran back to back with the dataset regenerated in place, so their
page-cache/writeback conditions match each other (they are ~15 % slower
on ingest than the pristine-disk run recorded above — a first
simple-gc run on an empty `/tmp/bench` did ingest in 75.8 s). End state
is identical: both reach 32.52 GiB, 300/300 complete, 10 restores
identical.

| step | simple-gc (main) | mark-sweep (PR #2) |
| --- | --- | --- |
| ingest 1000 refs | 92.7 s, 731 MiB/s logical, 550 MiB/s new bytes | 90.5 s, 749 / 564 MiB/s |
| — of which ingest.Dir | 87.8 s | 88.9 s (collector-independent, as expected) |
| ref put | 4.8 ms/ref median, growing 3.3 → 6.4 over the run | **1.4 ms/ref, flat** |
| delete 700 refs | 2.63 s (3.8 ms/ref) | **0.37 s (0.5 ms/ref)** |
| `gc run` (0.5 line): 63 reaped, 5.6 GiB copied, 15.8 GiB freed | 9.69 s | **6.56 s** (mark 0.52 s, sweep 5.94 s) |
| `gc run --garbage 0`: 107 reaped, 19.4–19.5 GiB copied | 32.5 s | 29.4 s (mark 0.51 s, sweep 28.8 s) |
| packstore on disk: ingest → policy → forced | 49.90 → 39.75 → 32.52 GiB | 49.90 → 39.75 → 32.52 GiB |
| liveness state | closures 10.4 MiB (1000 refs) / 5.0 MiB (300) + union in RAM | none (empty dir) |
| integrity | 300/300 complete, 10 restores identical | same |

Where the differences come from: ref put loses the union merge and the
closure-file write, keeping only the completeness walk — so it stops
growing with the number of live refs. Delete becomes a pure refstore
write (simple-gc paid an O(union) rebuild per release). The full mark —
walking all 300 kept trees and marking 503,941 objects into the bitmaps
— costs ~0.5 s (~1 µs/object), which the policy cycle more than wins
back: its copy loop pipelines verification (CRC + payload rehash)
across GOMAXPROCS workers with one appender and batches fsyncs at the
end, vs simple-gc's per-8-MiB fsyncs. The forced pass is a wash — both
are bounded by the 19.5 GiB survivor copy at the append lock. The costs
that moved *into* the cycle: `gc status` now pays a fresh ~0.5–2 s mark
per invocation (no persistent liveness), and a crashed/aborted cycle
redoes its mark from scratch. First-run variance on the same machine:
simple-gc's gc passes also measured 8.3 s / 28.7 s (65/104 reaped) on a
pristine disk, so treat ±10 % on the copy-bound numbers as noise.

### Results, 2026-08-25, Linux, 150 GiB: simple-gc vs mark-sweep-gc (PR #2)

Same i7-1280P, same protocol as the 50 GiB side-by-side but `-scale 3.0`:
1000 refs, 198.98 GiB ingested, 149.41 GiB unique (24.9 % duplicate),
105k files, 599 packs, 2.32M objects. Deleting the 700 refs leaves
51.9 GiB nominal garbage. Dataset + store are ~3× RAM (62 GiB), so the
page cache no longer holds the working set. simple-gc numbers are from
the control rerun under conditions matched to the mark-sweep arm; its
first run (fresh dataset) measured the same within noise except gc
(policy 18.3 s, forced 61.3 s). End state is identical in all three
runs: 149.69 → 104.4 → 97.71 GiB, 300/300 complete, 10 restores
identical.

| step | simple-gc (main) | mark-sweep (PR #2) |
| --- | --- | --- |
| ingest 1000 refs | 366.1 s, 556 MiB/s logical, 418 MiB/s new bytes | **282.0 s, 723 / 543 MiB/s** |
| — of which ingest.Dir | 356.1 s | 278.9 s (see below) |
| ref put | 9.8 ms/ref median, growing 4.4 → 14.5 over the run | **2.3 ms/ref, flat** |
| delete 700 refs | 6.85 s (9.8 ms/ref) | **0.38 s (0.5 ms/ref)** — 17× |
| `gc run` (0.5 line): 210 reaped, 7.3 GiB copied, 52.5 GiB freed | **14.5 s** (18.3 s first run) | 21.5 s (mark 4.6 s, sweep 16.8 s) |
| `gc run --garbage 0`: 129–130 reaped, 25.4–25.8 GiB copied | 58.7 s (61.3 s first run) | **31.3 s** (mark 3.2 s, sweep 28.0 s) — 2× |
| both gc passes end to end | 73.2 s | **52.8 s** |
| packstore on disk: ingest → policy → forced | 149.69 → 104.43 → 97.71 GiB | identical |
| liveness state | closures 25.1 MiB (1000 refs) / 14.2 MiB (300) + union in RAM | none |
| integrity | 300/300 complete, 10 restores identical | same |

How the trade moves at 3× scale. simple-gc's churn costs grow with the
union (2.32M live tails): deletes 2.6 → 6.9 s and ref put 4.8 → 9.8 ms
vs the 50 GiB run, both still climbing within the run; mark-sweep stays
flat (0.4 s, 2.3 ms) — there is nothing that scales with ref count on
its churn path. The ingest gap is larger than the timed ref-put split
(10.0 vs 3.1 s) explains: main's ingest.Dir — identical packstore code —
runs 77 s slower, and the remaining suspect is the per-put union merge,
which allocates a fresh ~37 MB union per reference (~37 GB over the
phase) and pays its concurrent-GC cost inside the ingest windows; at
50 GiB the same comparison showed ingest.Dir dead even, so the effect
only bites once the union is millions of entries. In the other
direction, the mark went superlinear: 0.5 → 4.6 s for 3× objects,
because MarkSet.locate probes pack filters newest-first across all 599
segments — O(marked objects × packs); that flips the policy pass to
simple-gc (14.5 vs 21.5 s), whose union scoring stays cheap. A per-key
segment memo (or marking in disk order) is the obvious next lever if the
mark matters at larger scales. The forced pass flips the other way,
2× for mark-sweep (31.3 vs 58.7 s): with the cache saturated,
simple-gc's per-8-MiB copy fsyncs under the append lock stall the
copier, while Compact batches its fsync to the end of the sweep. One
more scale note: Compact deletes all victims after all copies (peak
disk = store + copied bytes, here +25 GiB transient), where simple-gc
deletes per victim as it goes.

The pack polarization at this scale is worth knowing for policy tuning:
with 300 MiB refs against 256 MiB packs, most packs end up nearly all
live or all dead, so the 0.5-line policy pass already frees 52.5 of the
51.9 GiB nominal (the store's whole-pipeline picture: policy leaves just
6.7 GiB of garbage vs 7.0–7.2 GiB at 50 GiB scale on a 3× store).

### Results, 2026-09-23: opening a store, and sharing it (branch `packstore-multi-process`)

Not `amber-bench`: a throwaway program (removed) on the development Mac
(Apple silicon, APFS), warm cache, random incompressible objects, best of
three for the opens. "Before" is the branch point (`refstore-sqlite`: one
exclusive directory lock, every open scans the active segment); "after" adds
the sidecar index of active segments and lets any number of processes share
a store (`architecture/packstore.md`).

| | before | after |
| --- | --- | --- |
| open, 64 MiB active segment (16k records of 4 KiB) | 13.1 ms | **1.6 ms** |
| open, 256 MiB (66k records) | 52.4 ms | **6.2 ms** |
| open, 1 GiB (262k records) | 197.1 ms | **28.9 ms** |
| open + first put, 64 MiB / 256 MiB / 1 GiB | 11.3 / 43.7 / 178.8 ms | 3.0 / 12.9 / 57.4 ms |
| open, 512 MiB in 8 / 64 / 516 sealed segments | 2.0 / 2.7 / 9.2 ms | 1.0 / 1.9 / 10.6 ms |
| `Put` of 2 KiB, sync on | 4.41 ms | 4.79 ms |
| `Put` of 2 KiB, sync off (20,000) | 5.7 us | 7.6 us |
| `Has` on an absent key, 8 / 64 / 516 sealed segments | 0.04 / 0.27 / 2.35 us | 2.5 / 2.8 / 5.6 us |
| `Missing` of 20,000 absent keys, same stores | 3.2 / 3.6 / 8.6 ms | 3.1 / 3.7 / 10.3 ms |

What the numbers say. An open no longer reads the data file: what is left is
loading the sidecar's entries into a map, about 0.1 us a record, so it still
grows with the records in active segments. A process-per-command client (jj)
wants small segments: at 64 MiB an open costs 1.6 ms. With a cold cache the
gap is wider than shown, since the old open read the whole file. Open plus
first put costs about twice an open because adoption recovers the segment a
second time; taking over the index the view already holds is the known lever.
The gate between writers and a sweep costs about 2 us a write (take `gc.lock`
shared, read the generation, let go): invisible next to an fsync, where the
difference above is noise, visible only with sync off. A lookup that finds
nothing pays a stat of the directory and an fstat per foreign active segment,
2-3 us, to learn that nothing changed; before that check existed every miss
listed the directory, 33 / 113 / 760 us at 8 / 64 / 516 segments. The write
path's duplicate check never pays it, and `Missing` pays it once.
