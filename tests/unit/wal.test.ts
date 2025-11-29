import { expect, test } from "bun:test";
import { mkdtemp, rm, stat } from "fs/promises";
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
