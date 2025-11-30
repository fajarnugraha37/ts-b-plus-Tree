import { expect, test } from "bun:test";
import { mkdtemp, rm, stat } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { setTimeout as delay } from "timers/promises";
import { BPlusTree } from "../../index.ts";

test(
  "time-based checkpoint truncates WAL",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-checkpoint-"));
    const filePath = join(dir, "checkpoint.db");
    const tree = await BPlusTree.open({
      filePath,
      walOptions: {
        checkpointIntervalOps: 0,
        checkpointIntervalMs: 50,
      },
    });
    try {
      await tree.set(1, Buffer.from("one"));
      await tree.set(2, Buffer.from("two"));
      await delay(60);
      await tree.set(3, Buffer.from("three"));
      const walStats = await stat(`${filePath}.wal`);
      expect(walStats.size).toBe(32);
    } finally {
      await tree.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 60_000 },
);
