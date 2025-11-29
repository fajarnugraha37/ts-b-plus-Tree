import { test, expect } from "bun:test";
import { tmpdir } from "os";
import { join } from "path";
import { mkdtemp, rm } from "fs/promises";
import { BPlusTree } from "../../index.ts";

const DEFAULT_COUNT = 1_000_000;
const recordCount = Number(process.env.BPTREE_STRESS_COUNT ?? DEFAULT_COUNT);
const VALUE_BYTES = 128;

function makeValue(key: number): Buffer {
  const buffer = Buffer.alloc(VALUE_BYTES);
  buffer.writeUInt32LE(key, 0);
  return buffer;
}

test(
  `insert/read ${recordCount.toLocaleString()} records within buffer limits`,
  async () => {
    const tempDir = await mkdtemp(join(tmpdir(), "ts-btree-"));
    const filePath = join(tempDir, "million.db");
    const tree = await BPlusTree.open({ filePath });

    try {
      for (let i = 0; i < recordCount; i += 1) {
        await tree.set(i, makeValue(i));
      }

      expect(tree.meta.keyCount).toBe(BigInt(recordCount));

      const checkpoints = Math.max(1, Math.floor(recordCount / 10));
      for (let i = 0; i < recordCount; i += checkpoints) {
        const value = await tree.get(i);
        expect(value?.readUInt32LE(0)).toBe(i);
      }

      let scanned = 0;
      for await (const entry of tree.range(0, recordCount - 1)) {
        expect(entry.value.readUInt32LE(0)).toBe(scanned);
        scanned += 1;
      }
      expect(scanned).toBe(recordCount);

      const stats = tree.bufferPool.getStats();
      expect(stats.maxResidentPages).toBeLessThanOrEqual(tree.bufferPool.capacity);
      expect(stats.pageLoads).toBeGreaterThan(0);
      expect(stats.pageFlushes).toBeGreaterThan(0);
    } finally {
      await tree.close();
      await rm(tempDir, { recursive: true, force: true });
    }
  },
  {
    timeout: Math.max(600_000, recordCount * 10),
  },
);
