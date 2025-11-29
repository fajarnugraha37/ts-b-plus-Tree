import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";

test(
  "tree recovers after simulated crash without checkpoint",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-walrecovery-"));
    const filePath = join(dir, "wal-crash.db");
    const tree = await BPlusTree.open({
      filePath,
      walOptions: {
        groupCommit: true,
        checkpointIntervalOps: 0,
      },
    });
    const total = 500;
    for (let i = 0; i < total; i += 1) {
      const value = Buffer.alloc(128);
      value.writeUInt32LE(i, 0);
      await tree.set(i, value);
    }
    // simulate crash: flush buffers but skip checkpoint/close
    await tree.bufferPool.flushAll();
    await tree.wal.close();
    await tree.pageManager.fileManager.close();

    const reopened = await BPlusTree.open({
      filePath,
      walOptions: {
        groupCommit: true,
        checkpointIntervalOps: 1000,
      },
    });
    for (let i = 0; i < total; i += 50) {
      const value = await reopened.get(i);
      expect(value?.readUInt32LE(0)).toBe(i);
    }
    await reopened.close();
    await rm(dir, { recursive: true, force: true });
  },
  { timeout: 60_000 },
);
