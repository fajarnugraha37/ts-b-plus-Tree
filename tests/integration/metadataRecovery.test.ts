import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";

async function crashTree(tree: BPlusTree): Promise<void> {
  await tree.bufferPool.flushAll();
  await tree.wal.close();
  await tree.pageManager.fileManager.close();
}

function valueFor(key: number): Buffer {
  const buffer = Buffer.alloc(64);
  buffer.writeUInt32LE(key, 0);
  return buffer;
}

test(
  "metadata and free list survive repeated crash loops",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-meta-"));
    const filePath = join(dir, "meta.db");
    const tracked = new Map<number, Buffer>();

    for (let round = 0; round < 4; round += 1) {
      const tree = await BPlusTree.open({
        filePath,
        walOptions: { checkpointIntervalOps: 0 },
      });
      try {
        const base = round * 200;
        for (let i = 0; i < 150; i += 1) {
          const key = base + i;
          const value = valueFor(key);
          await tree.set(key, value);
          tracked.set(key, value);
        }
        for (const key of Array.from(tracked.keys())) {
          if (key % 3 === 0) {
            await tree.delete(key);
            tracked.delete(key);
          }
        }
        await tree.defragment();
      } finally {
        await crashTree(tree);
      }

      const reopened = await BPlusTree.open({
        filePath,
        walOptions: { checkpointIntervalOps: 0 },
      });
      try {
        for (const [key, value] of tracked.entries()) {
          const row = await reopened.get(key);
          expect(row?.equals(value)).toBeTrue();
        }
        expect(reopened.meta.keyCount).toBe(BigInt(tracked.size));
        const freePages = await reopened.pageManager.collectFreePages();
        const set = new Set(freePages);
        expect(set.size).toBe(freePages.length);
        for (const page of freePages) {
          expect(page).toBeGreaterThanOrEqual(3);
        }
        expect(await reopened.consistencyCheck()).toBeTrue();
      } finally {
        await reopened.close();
      }
    }

    await rm(dir, { recursive: true, force: true });
  },
  { timeout: 180_000 },
);
