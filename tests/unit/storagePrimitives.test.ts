import { expect, test } from "bun:test";
import { mkdtemp, rm, stat } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { FileManager } from "../../src/storage/fileManager.ts";
import { PageManager } from "../../src/storage/pageManager.ts";
import { BufferPool } from "../../src/storage/bufferPool.ts";
import { WriteAheadLog } from "../../src/storage/wal.ts";
import { PAGE_SIZE_BYTES, PageType } from "../../src/constants.ts";

async function tempPath(name: string): Promise<{ dir: string; path: string }> {
  const dir = await mkdtemp(join(tmpdir(), "ts-btree-storage-"));
  return { dir, path: join(dir, name) };
}

test("FileManager can grow, write, and read pages", async () => {
  const { dir, path } = await tempPath("fileManager.db");
  try {
    const fileManager = await FileManager.openOrCreate(path);
    const initialPages = await fileManager.pageCount();
    expect(initialPages).toBeGreaterThanOrEqual(3);

    const writeBuffer = Buffer.alloc(PAGE_SIZE_BYTES);
    writeBuffer.writeUInt32LE(0xdeadbeef, 0);
    await fileManager.writePage(5, writeBuffer);

    const readBuffer = await fileManager.readPage(5);
    expect(readBuffer.readUInt32LE(0)).toBe(0xdeadbeef >>> 0);
    expect(await fileManager.pageCount()).toBeGreaterThanOrEqual(6);
    await fileManager.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("PageManager initializes metadata and tracks free list", async () => {
  const { dir, path } = await tempPath("pageManager.db");
  try {
    const pageManager = await PageManager.initialize(path);
    const meta = await pageManager.readMeta();
    expect(meta.magic).toBe("BPTREE_V1");
    expect(meta.rootPage).toBe(2);
    expect(meta.treeDepth).toBe(1);

    const rootPage = await pageManager.readPage(meta.rootPage);
    expect(rootPage.readUInt8(0)).toBe(PageType.Leaf);

    const allocated = await pageManager.allocatePage();
    expect(allocated).toBeGreaterThan(meta.totalPages - 2);

    await pageManager.freePage(allocated);
    const afterFree = await pageManager.readMeta();
    expect(afterFree.freePageHead).toBe(allocated);
    await pageManager.fileManager.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("BufferPool enforces capacity and tracks stats", async () => {
  const { dir, path } = await tempPath("bufferPool.db");
  try {
    const pageManager = await PageManager.initialize(path);
    const bufferPool = new BufferPool(pageManager, { capacity: 2 });

    const pageNumbers: number[] = [];
    for (let i = 0; i < 3; i += 1) {
      const page = await pageManager.allocatePage();
      pageNumbers.push(page);
    }

    for (const pageNumber of pageNumbers) {
      const buffer = await bufferPool.getPage(pageNumber);
      buffer.writeUInt8(pageNumber, 0);
      bufferPool.unpin(pageNumber, true);
    }

    await bufferPool.flushAll();
    const stats = bufferPool.getStats();
    expect(stats.pageLoads).toBeGreaterThanOrEqual(3);
    expect(stats.pageFlushes).toBeGreaterThanOrEqual(3);
    expect(stats.maxResidentPages).toBeLessThanOrEqual(bufferPool.capacity);

    await pageManager.fileManager.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("WriteAheadLog appends frames", async () => {
  const { dir, path } = await tempPath("wal.db");
  const walPath = `${path}.wal`;
  try {
    const wal = new WriteAheadLog(walPath, PAGE_SIZE_BYTES);
    await wal.open();
    const tx = await wal.beginTransaction();
    const payload = Buffer.alloc(PAGE_SIZE_BYTES, 1);
    await wal.writePage(tx, 1, payload);
    await wal.commitTransaction(tx);
    await wal.close();

    const fileStats = await stat(walPath);
    expect(fileStats.size).toBeGreaterThan(32);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
