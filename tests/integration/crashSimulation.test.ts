import { expect, test } from "bun:test";
import { mkdtemp, rm, open } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";
import { PAGE_SIZE_BYTES } from "../../src/constants.ts";

const WAL_RECORD_HEADER_SIZE = 20;
const RECORD_TYPE_BEGIN = 0;
const RECORD_TYPE_PAGE = 1;

function valueFor(key: number): Buffer {
  const buffer = Buffer.alloc(64);
  buffer.writeUInt32LE(key, 0);
  buffer.write(`value-${key}`.slice(0, buffer.length - 4), 4, "utf8");
  return buffer;
}

async function crashTree(tree: BPlusTree): Promise<void> {
  await tree.bufferPool.flushAll();
  await tree.wal.close();
  await tree.pageManager.fileManager.close();
}

test(
  "replays WAL after crash without checkpoint",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-crash-"));
    const filePath = join(dir, "crash.db");
    const tree = await BPlusTree.open({
      filePath,
      walOptions: { checkpointIntervalOps: 0 },
    });
    const total = 200;
    for (let i = 0; i < total; i += 1) {
      await tree.set(i, valueFor(i));
    }
    await crashTree(tree);

    const reopened = await BPlusTree.open({
      filePath,
      walOptions: { checkpointIntervalOps: 0 },
    });
    try {
      for (let i = 0; i < total; i += 25) {
        const value = await reopened.get(i);
        expect(value?.equals(valueFor(i))).toBeTrue();
      }
      expect(await reopened.consistencyCheck()).toBeTrue();
    } finally {
      await reopened.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 120_000 },
);

async function appendTornWalRecord(walPath: string): Promise<void> {
  const handle = await open(walPath, "r+");
  try {
    const stats = await handle.stat();
    const beginRecord = Buffer.alloc(WAL_RECORD_HEADER_SIZE);
    beginRecord.writeUInt32LE(RECORD_TYPE_BEGIN, 0);
    beginRecord.writeUInt32LE(0xbeef, 4);
    await handle.write(beginRecord, 0, beginRecord.length, stats.size);

    const pageRecord = Buffer.alloc(WAL_RECORD_HEADER_SIZE);
    pageRecord.writeUInt32LE(RECORD_TYPE_PAGE, 0);
    pageRecord.writeUInt32LE(0xbeef, 4);
    pageRecord.writeUInt32LE(1234, 8);
    pageRecord.writeUInt32LE(PAGE_SIZE_BYTES, 12);
    pageRecord.writeUInt32LE(0, 16);
    await handle.write(
      pageRecord,
      0,
      pageRecord.length,
      stats.size + beginRecord.length,
    );
    // omit payload bytes to simulate a torn frame (crash mid-write)
  } finally {
    await handle.close();
  }
}

test(
  "ignores torn WAL tail after crash",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-crash-torn-"));
    const filePath = join(dir, "crash.db");
    const tree = await BPlusTree.open({ filePath });
    const walPath = `${filePath}.wal`;
    const total = 128;
    for (let i = 0; i < total; i += 1) {
      await tree.set(i, valueFor(i));
    }
    await crashTree(tree);
    await appendTornWalRecord(walPath);

    const reopened = await BPlusTree.open({ filePath });
    try {
      for (let i = 0; i < total; i += 16) {
        const value = await reopened.get(i);
        expect(value?.equals(valueFor(i))).toBeTrue();
      }
      expect(await reopened.consistencyCheck()).toBeTrue();
    } finally {
      await reopened.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 120_000 },
);

test(
  "overflow values survive crash and replay",
  async () => {
    const dir = await mkdtemp(join(tmpdir(), "ts-btree-crash-overflow-"));
    const filePath = join(dir, "overflow.db");
    const tree = await BPlusTree.open({ filePath });
    const bigValue = Buffer.alloc(tree.pageManager.pageSize * 4, 0xcd);
    await tree.set(5, bigValue);
    await crashTree(tree);

    const reopened = await BPlusTree.open({ filePath });
    try {
      const value = await reopened.get(5);
      expect(value?.equals(bigValue)).toBeTrue();
    } finally {
      await reopened.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
  { timeout: 120_000 },
);
