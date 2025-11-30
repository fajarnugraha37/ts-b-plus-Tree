import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";

test(
  "parallel range cursors scan disjoint partitions",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-parallel-"));
    const filePath = join(dir, "parallel.db");
    const tree = await BPlusTree.open({ filePath });
    const total = 1000;
    try {
      for (let i = 0; i < total; i += 1) {
        await tree.set(i, Buffer.from(`value-${i}`));
      }
      const partitions = [
        { start: 0, end: 249 },
        { start: 250, end: 499 },
        { start: 500, end: 749 },
        { start: 750, end: 999 },
      ];
      const cursors = partitions.map((p) =>
        tree.createRangeCursor(p.start, p.end),
      );
      const collected = await Promise.all(
        cursors.map(async (cursor) => {
          const keys: number[] = [];
          while (true) {
            const row = await cursor.next();
            if (!row) {
              break;
            }
            keys.push(Number(row.key.readBigUInt64BE()));
          }
          await cursor.close();
          return keys;
        }),
      );
      const flattened = collected.flat();
      flattened.sort((a, b) => a - b);
      expect(flattened.length).toBe(total);
      expect(flattened[0]).toBe(0);
      expect(flattened[flattened.length - 1]).toBe(total - 1);
    } finally {
      await tree.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 60_000 },
);
