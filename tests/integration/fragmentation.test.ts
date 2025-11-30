import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";

function largeValue(i: number): Buffer {
  const payload = Buffer.alloc(2048);
  payload.writeUInt32LE(i, 0);
  return payload;
}

test(
  "defragment reduces fragmentation and preserves data",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-frag-"));
    const filePath = join(dir, "frag.db");
    const tree = await BPlusTree.open({ filePath });
    try {
      const total = 750;
      for (let i = 0; i < total; i += 1) {
        await tree.set(i, largeValue(i));
      }
      for (let i = 0; i < total; i += 3) {
        await tree.delete(i);
      }
      const before = await tree.pageManager.fragmentationStats();
      await tree.defragment();
      const after = await tree.pageManager.fragmentationStats();
      expect(after.fragmentation).toBeLessThan(before.fragmentation);
      for (let i = 0; i < total; i += 1) {
        const value = await tree.get(i);
        if (i % 3 === 0) {
          expect(value).toBeNull();
        } else {
          expect(value?.readUInt32LE(0)).toBe(i);
        }
      }
      await tree.close();
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 120_000 },
);
