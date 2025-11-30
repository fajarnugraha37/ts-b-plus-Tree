import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree, exportSnapshot, importSnapshot } from "../../index.ts";

function largeValue(seed: number, multiplier = 2): Buffer {
  const buffer = Buffer.alloc(1024 * multiplier, seed & 0xff);
  buffer.writeUInt32LE(seed, 0);
  return buffer;
}

test(
  "snapshot export/import preserves overflow values",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-snapshot-"));
    const filePath = join(dir, "snapshot.db");
    const snapshotPath = join(dir, "snapshot.bin");
    const restoredPath = join(dir, "restored.db");
    const total = 256;

    const tree = await BPlusTree.open({ filePath });
    try {
      for (let i = 0; i < total; i += 1) {
        await tree.set(i, largeValue(i, 4));
      }
      await exportSnapshot(tree, snapshotPath);
    } finally {
      await tree.close();
    }

    const restored = await BPlusTree.open({ filePath: restoredPath });
    try {
      await importSnapshot(restored, snapshotPath);
      expect(restored.meta.keyCount).toBe(BigInt(total));
      for (let i = 0; i < total; i += 5) {
        const value = await restored.get(i);
        expect(value?.equals(largeValue(i, 4))).toBeTrue();
      }
    } finally {
      await restored.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 120_000 },
);
