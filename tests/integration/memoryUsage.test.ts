import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";

const COUNT = Number(process.env.BPTREE_MEMORY_COUNT ?? 20_000);
const RSS_LIMIT = Number(process.env.BPTREE_MEMORY_LIMIT ?? 300 * 1024 * 1024); // 300MB

async function createTree() {
  const dir = await mkdtemp(join(tmpdir(), "ts-btree-mem-"));
  const filePath = join(dir, "memory.db");
  const tree = await BPlusTree.open({ filePath });
  return { dir, filePath, tree };
}

test(
  `heap/RSS stays within limits after ${COUNT.toLocaleString()} operations`,
  async () => {
    const { dir, tree } = await createTree();
    try {
      for (let i = 0; i < COUNT; i += 1) {
        const value = Buffer.alloc(128);
        value.writeUInt32LE(i, 0);
        await tree.set(i, value);
      }

      for (let i = 0; i < COUNT; i += 5000) {
        const value = await tree.get(i);
        expect(value?.readUInt32LE(0)).toBe(i);
      }

      const mem = process.memoryUsage();
      expect(mem.rss).toBeLessThan(RSS_LIMIT);
      expect(mem.heapUsed).toBeLessThan(RSS_LIMIT);
    } finally {
      await tree.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 120_000 },
);
