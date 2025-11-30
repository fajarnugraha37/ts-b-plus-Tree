# Project Redwood (a.k.a. “yet another home-grown B+Tree”) 🌲

> ⚠️ **Learning sandbox only.** This repository is a notebook for exploring B+Tree internals, IO limits, WAL durability, and general storage nerdery. Do not point production traffic at it unless you enjoy page faults for breakfast.

## What is this?
A TypeScript implementation of a disk-backed B+Tree with:
- Fixed-size pages, buffer pool, WAL, background vacuum, multi-file growth, and restore points.
- Integration tests that shovel millions of keys while tracking RSS/heap usage.
- Enough instrumentation to learn *why* databases do what they do (and to appreciate why seasoned DB engineers drink so much coffee).

## Why this lib?
Because reading SQLite’s pager.c explains *what*; writing your own pager explains *why*. Redwood is intentionally tiny, TypeScript-native, and verbose, so you can single-step through splits, merges, WAL replay, and spillover IO without spelunking through C macros.

## Quick start
```bash
bun install
bun x tsc --noEmit
bun test              # full unit suite
bun test tests/integration # includes million-row stress tests
```
Minimal runtime example:
```ts
import { BPlusTree } from "./index.ts";

const tree = await BPlusTree.open({ filePath: "./data.db" });
await tree.set(42, Buffer.from("hello"));
console.log((await tree.get(42))?.toString());
await tree.close();
```

## Core concepts
### Architecture
- **Page Manager**: hands out 4–8KB pages, tracks free lists, truncates unused segments.
- **Buffer Pool**: LRU/clock eviction, optional group commit, per-page latching.
- **Leaf/Internal Nodes**: variable-length payloads with overflow chains for jumbo values.
- **WAL**: begin/page/commit records, optional Brotli compression, replay + checkpoints.

### How it works
1. `set()` normalizes keys/values, acquires the global write lock, then walks the tree under page latches.
2. When leaves overflow, we split, update parent separators, and propagate upward.
3. Deletes borrow/merge siblings, adjust separators, and update free lists.
4. WAL entries are flushed before dirty pages reach disk; checkpoints truncate WAL to 32 bytes.

### How it flows
```
app -> BPlusTree API -> latch manager -> buffer pool -> page manager -> file manager
                                   ^                                      |
                                   |--- WAL (mirror writes) ---------------|
```
Reads grab shared latches, hop to right siblings (B-link) if a high key was stale, and never touch the global lock.

### Memory, disk, IO interaction
- Buffer pool caches hot pages (default 25). Misses pull pages from `PageManager`.
- Overflow values spill into linked overflow pages; defrag + vacuum reclaim the chain.
- WAL sits beside the DB file(s); checkpoints/fsync happen according to `walOptions`.
- Background vacuum batches 32–512 pages per interval to keep fragmentation below 30%.

### Concurrency model
- Global `AsyncRWLock` serializes writers; readers use per-page latches.
- B-link right siblings let readers follow splits without re-fetching the root.
- Buffer pool group commit batches WAL writes for lower fsync pressure.

## Usage catalog
| Scenario | Snippet |
| --- | --- |
| Basic CRUD | `examples/basicCrud.ts` |
| Range scans | `examples/rangeScan.ts` |
| Persistence + reopen | `examples/persistence.ts` |
| Advanced IO tuning + vacuum | `examples/advancedOps.ts` |
| Restore points | `examples/restorePoint.ts` |
| Multi-file segmented pages | `tests/integration/multiFile.test.ts` |

## Advanced features
- **Segmented storage**: `io.segmentPages` spills pages into `*.segN` files automatically.
- **Background vacuum**: `maintenance.backgroundVacuum` keeps fragmentation in check.
- **Checkpoint cadence**: `walOptions.checkpointIntervalOps` & `checkpointIntervalMs`.
- **Restore points (PITR)**: copy DB+WAL into a labeled directory and rewind anytime.
- **WAL compression**: `walOptions.compressFrames` shrinks log size ~40% in tests.

## Cookbook
1. **Enable segmented storage & WAL compression**:
   ```ts
   await BPlusTree.open({
     filePath: "./seg.db",
     io: { segmentPages: 512, walDirectory: "./wal" },
     walOptions: { compressFrames: true, checkpointIntervalMs: 1000 },
   });
   ```
2. **Create a restore point before dangerous migrations**:
   ```ts
   await createRestorePoint(tree, "./restorePoints/pre-migration", "before-alter");
   ```
3. **Restore**:
   ```ts
   await restoreFromPoint("./restorePoints/pre-migration", "./db-rolled-back.db");
   ```
4. **Watch background vacuum in action**: run `examples/advancedOps.ts` and tail the logs; you’ll see vacuum batches trimming free pages.

## Performance benchmarks (bun test / local NVMe)
| Workload | 100K records | 1M records |
| --- | --- | --- |
| Insert throughput | ~120k ops/s, WAL 28 MB, RSS 60 MB | ~95k ops/s, WAL 220 MB, RSS 92 MB |
| Range scan (1M keys) | 220 MB/s streaming, heap 45 MB | 215 MB/s, heap 70 MB |
| Delete + vacuum | 80k ops/s, fragmentation <20% | 65k ops/s, fragmentation <25% |
*(numbers from integration tests on a MacBook Pro; your mileage will vary more than my sarcasm).* 

## API reference (TL;DR)
```ts
interface BPlusTreeOptions {
  filePath: string;
  walPath?: string;
  bufferPages?: number;
  io?: { pageSize?: number; readAheadPages?: number; walDirectory?: string; segmentPages?: number; };
  walOptions?: { compressFrames?: boolean; checkpointIntervalOps?: number; checkpointIntervalMs?: number; groupCommit?: boolean; };
  maintenance?: { backgroundVacuum?: boolean; vacuumOptions?: { intervalMs?: number; batchSize?: number; } };
}
```
Main methods: `get`, `set`, `delete`, `range`, `keys`, `values`, `defragment`, `vacuum`, `consistencyCheck`, `createRestorePoint`, `restoreFromPoint`.

## Performance & reliability status
- ✔️ Multi-level splits/merges, segmented storage, WAL replay, background vacuum.
- ✔️ PITR restore points + integration tests for checkpoint cadences and crash loops.
- ⚠️ No production hardening (no crash recovery across process boundaries yet, no checksums, no page-level security)—remember the warning up top.

Happy hacking! If you discover anything horrifying, please open an issue (or at least jot it down in your own storage diary).
