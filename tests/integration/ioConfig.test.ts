import { expect, test } from "bun:test";
import { mkdtemp, rm, stat, mkdir } from "fs/promises";
import { join, basename } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";

test(
  "custom page size, read-ahead, and WAL directory work",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-io-"));
    const filePath = join(dir, "io.db");
    const walDir = join(dir, "wal");
    await mkdir(walDir);
    const tree = await BPlusTree.open({
      filePath,
      io: {
        pageSize: 8192,
        readAheadPages: 2,
        walDirectory: walDir,
      },
      walOptions: {
        compressFrames: true,
      },
      bufferPages: 10,
    });
    try {
      expect(tree.pageManager.pageSize).toBe(8192);
      for (let i = 0; i < 100; i += 1) {
        const value = Buffer.alloc(512);
        value.writeUInt32LE(i, 0);
        await tree.set(i, value);
      }
      for (let i = 0; i < 100; i += 10) {
        const value = await tree.get(i);
        expect(value?.readUInt32LE(0)).toBe(i);
      }
      const expectedWal = join(walDir, `${basename(filePath)}.wal`);
      const stats = await stat(expectedWal);
      expect(stats.size).toBeGreaterThan(0);
    } finally {
      await tree.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 120_000 },
);
