import { expect, test } from "bun:test";
import { mkdtemp, rm, stat } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";

test(
  "segmenting pages across multiple files works",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-seg-"));
    const filePath = join(dir, "segmented.db");
    const tree = await BPlusTree.open({
      filePath,
      io: {
        pageSize: 1024,
        segmentPages: 8,
      },
    });
    try {
      const total = 400;
      for (let i = 0; i < total; i += 1) {
        await tree.set(i, Buffer.from(`value-${i}`));
      }
      const segmentPath = `${filePath}.seg1`;
      const exists = await stat(segmentPath).then(
        () => true,
        () => false,
      );
      expect(exists).toBeTrue();
      for (let i = 0; i < total; i += 17) {
        const value = await tree.get(i);
        expect(value?.toString()).toBe(`value-${i}`);
      }
    } finally {
      await tree.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 120_000 },
);
