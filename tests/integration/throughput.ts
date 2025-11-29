import { test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";

const DEFAULT_COUNT = 1_000_000;
const recordCount = Number(process.env.BPTREE_BENCH_COUNT ?? DEFAULT_COUNT);
const checkpointStride = Math.max(1, Math.floor(recordCount / 100));
const VALUE_BYTES = 128;

function makeValue(n: number): Buffer {
  const buffer = Buffer.alloc(VALUE_BYTES);
  buffer.writeUInt32LE(n, 0);
  return buffer;
}

async function withTree<T>(fn: (tree: BPlusTree) => Promise<T>): Promise<T> {
  const tempDir = await mkdtemp(join(tmpdir(), "ts-btree-bench-"));
  const filePath = join(tempDir, "bench.db");
  const tree = await BPlusTree.open({ filePath });
  try {
    return await fn(tree);
  } finally {
    await tree.close();
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function loadSequential(tree: BPlusTree): Promise<void> {
  for (let i = 0; i < recordCount; i += 1) {
    await tree.set(i, makeValue(i));
  }
}

function logDuration(label: string, startMs: number): void {
  const elapsed = performance.now() - startMs;
  console.log(`${label}: ${(elapsed / 1000).toFixed(2)}s`);
}

test(
  `bench insert ${recordCount.toLocaleString()} sequential keys`,
  async () => {
    await withTree(async (tree) => {
      const start = performance.now();
      await loadSequential(tree);
      logDuration("insert", start);
      const stats = tree.bufferPool.getStats();
      console.log(
        `loads=${stats.pageLoads} flushes=${stats.pageFlushes} peakPages=${stats.maxResidentPages}`,
      );
    });
  },
  { timeout: Math.max(900_000, recordCount * 5) },
);

test(
  `bench point reads every ${checkpointStride}th key`,
  async () => {
    await withTree(async (tree) => {
      await loadSequential(tree);
      const start = performance.now();
      for (let i = 0; i < recordCount; i += checkpointStride) {
        const value = await tree.get(i);
        if (!value || value.readUInt32LE(0) !== i) {
          throw new Error(`read mismatch at ${i}`);
        }
      }
      logDuration("point reads", start);
    });
  },
  { timeout: Math.max(600_000, recordCount * 3) },
);

test(
  `bench range scan ${recordCount.toLocaleString()} keys`,
  async () => {
    await withTree(async (tree) => {
      await loadSequential(tree);
      const start = performance.now();
      let count = 0;
      for await (const row of tree.range(0, recordCount - 1)) {
        if (row.value.readUInt32LE(0) !== count) {
          throw new Error(`scan mismatch at ${count}`);
        }
        count += 1;
      }
      if (count !== recordCount) {
        throw new Error(`scan stopped early at ${count}`);
      }
      logDuration("range scan", start);
    });
  },
  { timeout: Math.max(600_000, recordCount * 3) },
);
