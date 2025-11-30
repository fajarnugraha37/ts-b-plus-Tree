# Redwood Roadmap

## Near-Term Initiatives (Q1–Q2)
1. **Crash Safety Deep Dive**
   - [ ] WAL checkpoint scheduler with per-table triggers.
   - [ ] Deterministic crash harness that replays 100+ write/crash cycles nightly and validates metadata/free-list consistency.
2. **Observability & Limits**
   - [ ] Stream buffer/WAL counters into OpenTelemetry exporters.
   - [ ] RSS/heap alerting hooks (thresholds configurable via `diagnostics`).
3. **Performance Hotspots**
   - [ ] Prefetch right siblings during range scans (bufferPool currently grabs them lazily).
   - [ ] Add read-ahead for overflow chains (see `OverflowManager.allocateChain`).
   - [ ] Inline LRU stats to detect repeated single-page trashing (BufferPool has the counters; we just need dashboards).
4. **Documentation & Examples**
   - [ ] Expand Cookbook entries into runnable `examples/*.ts` scripts (advancedOps, restorePoint already exist—more to come for streaming range scans and crash harnesses).

## Medium-Term (Q3–Q4)
- **PITR Tooling**: CLI commands (`tools/cli.ts`) for `restore-point create` / `restore-point apply` with human timestamps.
- **Segment Compaction**: ability to merge sparse `.segN` files back into the base file.
- **Backup/Restore**: streaming copy into a `.car` archive plus incremental WAL shipping.
- **Performance Tuning**: profile overflow-heavy workloads; consider prefix compression; benchmark multi-threaded read scenarios.

## Long-Term / 10-Year Vision
1. **Self-Healing Storage**: background checkers that verify B+Tree invariants, rewrite corrupted leaves, and report anomalies via structured logs.
2. **Plug-and-Play Pager**: Redwood evolves into a modular pager you can drop under other projects (think “SQLite pager, but TypeScript and documented”).
3. **Hybrid Storage**: tier hot pages to memory/disk automatically, integrate with object storage for cold data.
4. **Distributed Experiments**: raft-based replication, log shipping, eventually geo-distributed B+Trees (because why not dream big?).
5. **Developer Experience**: embeddable visualizer that animates splits/merges for teaching.

## Performance Opportunities (observed from current code)
- `bufferPool.getPage` re-reads siblings on every multi-page range; we can keep sibling references around to shave IO.
- `overflowManager.allocateChain` allocates/frees pages individually; batching page writes could cut fsync counts in half.
- `PageManager.vacuumFreePages` only trims trailing pages plus batches from the free list; a more aggressive compaction (defragment into new segments) is on the backlog.

## Spin-offs / Next Projects
1. **Redwood-Lite** – pure in-memory variant for teaching concurrency without disk.
2. **Redwood-LSM** – experiment with LSM tree built on top of Redwood’s pager.
3. **Instrumentation Dashboard** – Bun + WebSocket UI that streams diagnostics snapshots in real time.
4. **CLI Crash Harness** – scriptable crash/restore scenarios for classroom demos.

Stay curious, keep the coffee flowing, and remember: this codebase is a laboratory, not a data center.
