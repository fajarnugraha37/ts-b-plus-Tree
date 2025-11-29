import { expect, test } from "bun:test";
import { mkdtemp, rm, stat, open } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { PageManager } from "../../src/storage/pageManager.ts";
import { WriteAheadLog } from "../../src/storage/wal.ts";
import { PAGE_SIZE_BYTES } from "../../src/constants.ts";

async function setupEnv() {
  const dir = await mkdtemp(join(tmpdir(), "ts-btree-wal-"));
  const filePath = join(dir, "data.db");
  const pageManager = await PageManager.initialize(filePath);
  const walPath = `${filePath}.wal`;
  const wal = new WriteAheadLog(walPath, PAGE_SIZE_BYTES);
  await wal.open();
  return { dir, filePath, pageManager, wal, walPath };
}

test("WAL replays committed frames into base file", async () => {
  const { dir, pageManager, wal, walPath } = await setupEnv();
  try {
    const tx = await wal.beginTransaction();
    const payload = Buffer.alloc(PAGE_SIZE_BYTES, 0x11);
    await wal.writePage(tx, 5, payload);
    await wal.commitTransaction(tx);
    await wal.close();

    const recoveryWal = new WriteAheadLog(walPath, PAGE_SIZE_BYTES);
    await recoveryWal.open();
    await recoveryWal.replay(pageManager);

    const page = await pageManager.readPage(5);
    expect(page.equals(payload)).toBeTrue();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("WAL ignores uncommitted frames", async () => {
  const { dir, pageManager, wal, walPath } = await setupEnv();
  try {
    const tx = await wal.beginTransaction();
    const payload = Buffer.alloc(PAGE_SIZE_BYTES, 0x22);
    await wal.writePage(tx, 3, payload);
    // no commit
    await wal.close();

    const recoveryWal = new WriteAheadLog(walPath, PAGE_SIZE_BYTES);
    await recoveryWal.open();
    await recoveryWal.replay(pageManager);
    const page = await pageManager.readPage(3);
    expect(page.equals(Buffer.alloc(PAGE_SIZE_BYTES))).toBeTrue();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("checkpoint truncates WAL file", async () => {
  const { dir, pageManager, wal, walPath } = await setupEnv();
  try {
    const tx = await wal.beginTransaction();
    await wal.writePage(tx, 1, Buffer.alloc(PAGE_SIZE_BYTES, 0x33));
    await wal.commitTransaction(tx);
    await wal.checkpoint(pageManager);

    const fileStats = await stat(walPath);
    expect(fileStats.size).toBe(32);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("WAL recovers after simulated crash mid-commit", async () => {
  const { dir, pageManager, walPath } = await setupEnv();
  try {
    const wal = new WriteAheadLog(walPath, PAGE_SIZE_BYTES);
    await wal.open();
    const tx = await wal.beginTransaction();
    await wal.writePage(tx, 4, Buffer.alloc(PAGE_SIZE_BYTES, 0x44));
    // simulate crash by not committing and closing abruptly
    await wal.close();

    const crashWal = new WriteAheadLog(walPath, PAGE_SIZE_BYTES);
    await crashWal.open();
    const replayBefore = await pageManager.readPage(4);
    expect(replayBefore).toEqual(Buffer.alloc(PAGE_SIZE_BYTES));
    await crashWal.replay(pageManager);

    const afterReplay = await pageManager.readPage(4);
    expect(afterReplay.equals(Buffer.alloc(PAGE_SIZE_BYTES))).toBeTrue();

    const wal2 = new WriteAheadLog(walPath, PAGE_SIZE_BYTES);
    await wal2.open();
    const tx2 = await wal2.beginTransaction();
    const payload = Buffer.alloc(PAGE_SIZE_BYTES, 0x55);
    await wal2.writePage(tx2, 4, payload);
    await wal2.commitTransaction(tx2, true); // skip sync to simulate buffered commit
    await wal2.close();

    const recoveryWal = new WriteAheadLog(walPath, PAGE_SIZE_BYTES);
    await recoveryWal.open();
    await recoveryWal.replay(pageManager);

    const finalPage = await pageManager.readPage(4);
    expect(finalPage.equals(payload)).toBeTrue();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("WAL emits begin records before frames", async () => {
  const { dir, wal, walPath } = await setupEnv();
  try {
    const tx = await wal.beginTransaction();
    const payload = Buffer.alloc(PAGE_SIZE_BYTES, 0x77);
    await wal.writePage(tx, 9, payload);
    await wal.commitTransaction(tx);
    await wal.close();

    const handle = await open(walPath, "r");
    try {
      const header = Buffer.alloc(20);
      await handle.read(header, 0, 20, 32);
      expect(header.readUInt32LE(0)).toBe(0); // Begin record type
    } finally {
      await handle.close();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("WAL replay ignores torn frames at EOF", async () => {
  const { dir, pageManager, wal, walPath } = await setupEnv();
  try {
    const tx = await wal.beginTransaction();
    const payload = Buffer.alloc(PAGE_SIZE_BYTES, 0x88);
    await wal.writePage(tx, 11, payload);
    await wal.commitTransaction(tx);
    await wal.close();

    const handle = await open(walPath, "r+");
    try {
      const stats = await handle.stat();
      const beginRecord = Buffer.alloc(20);
      beginRecord.writeUInt32LE(0, 0); // Begin
      beginRecord.writeUInt32LE(0xdead, 4);
      await handle.write(beginRecord, 0, beginRecord.length, stats.size);
      const pageRecord = Buffer.alloc(20);
      pageRecord.writeUInt32LE(1, 0); // Page
      pageRecord.writeUInt32LE(0xdead, 4);
      pageRecord.writeUInt32LE(99, 8);
      pageRecord.writeUInt32LE(PAGE_SIZE_BYTES, 12);
      pageRecord.writeUInt32LE(0, 16);
      await handle.write(pageRecord, 0, pageRecord.length, stats.size + beginRecord.length);
      // omit payload bytes to simulate torn frame
    } finally {
      await handle.close();
    }

    const recoveryWal = new WriteAheadLog(walPath, PAGE_SIZE_BYTES);
    await recoveryWal.open();
    await recoveryWal.replay(pageManager);

    const page = await pageManager.readPage(11);
    expect(page.equals(payload)).toBeTrue();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
