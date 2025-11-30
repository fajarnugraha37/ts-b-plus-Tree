import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree, createRestorePoint, restoreFromPoint } from "../../index.ts";

function valueFor(n: number): Buffer {
  const buf = Buffer.alloc(64);
  buf.writeUInt32LE(n, 0);
  return buf;
}

test(
  "restore points rewind to earlier state",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-pitr-"));
    const dbPath = join(dir, "pitr.db");
    const tree = await BPlusTree.open({ filePath: dbPath });
    try {
      await tree.set(1, valueFor(1));
      await tree.set(2, valueFor(2));
      await createRestorePoint(tree, join(dir, "rp1"));

      await tree.set(3, valueFor(3));
      await createRestorePoint(tree, join(dir, "rp2"));

      await tree.set(4, valueFor(4));
    } finally {
      await tree.close();
    }

    const restore1Path = join(dir, "restore1.db");
    await restoreFromPoint(join(dir, "rp1"), restore1Path);
    const restored1 = await BPlusTree.open({ filePath: restore1Path });
    try {
      expect((await restored1.get(1))?.equals(valueFor(1))).toBeTrue();
      expect((await restored1.get(2))?.equals(valueFor(2))).toBeTrue();
      expect(await restored1.get(3)).toBeNull();
    } finally {
      await restored1.close();
    }

    const restore2Path = join(dir, "restore2.db");
    await restoreFromPoint(join(dir, "rp2"), restore2Path);
    const restored2 = await BPlusTree.open({ filePath: restore2Path });
    try {
      expect((await restored2.get(3))?.equals(valueFor(3))).toBeTrue();
      expect(await restored2.get(4)).toBeNull();
    } finally {
      await restored2.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 120_000 },
);
