import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { setTimeout as delay } from "timers/promises";
import { BPlusTree } from "../../index.ts";

test(
  "background vacuum reduces fragmentation after heavy churn",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-vacuum-"));
    const filePath = join(dir, "vacuum.db");
    const tree = await BPlusTree.open({
      filePath,
      maintenance: {
        backgroundVacuum: true,
        vacuumOptions: { intervalMs: 50, batchSize: 64, maxIdleBatches: 20 },
      },
    });
    try {
      const total = 1200;
      for (let i = 0; i < total; i += 1) {
        const value = Buffer.alloc(512, i & 0xff);
        await tree.set(i, value);
      }
      for (let i = 0; i < total; i += 2) {
        await tree.delete(i);
      }
      const before = await tree.pageManager.fragmentationStats();
      await delay(500);
      const after = await tree.pageManager.fragmentationStats();
      expect(after.freePages).toBeLessThanOrEqual(before.freePages);
    } finally {
      await tree.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 120_000 },
);
