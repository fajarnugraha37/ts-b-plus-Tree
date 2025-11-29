import {
  MAGIC,
  PageType,
  PAGE_HEADER_SIZE,
  PAGE_SIZE_BYTES,
} from "../constants.ts";
import { FileManager } from "./fileManager.ts";

export interface MetaPage {
  magic: string;
  pageSize: number;
  rootPage: number;
  treeDepth: number;
  totalPages: number;
  keyCount: bigint;
  freePageHead: number;
}

export interface PageManagerOptions {
  pageSize?: number;
}

export class PageManager {
  readonly fileManager: FileManager;
  readonly pageSize: number;

  constructor(fileManager: FileManager, pageSize = PAGE_SIZE_BYTES) {
    this.fileManager = fileManager;
    this.pageSize = pageSize;
  }

  static async initialize(
    filePath: string,
    options: PageManagerOptions = {},
  ): Promise<PageManager> {
    const fileManager = await FileManager.openOrCreate(filePath, options);
    const manager = new PageManager(fileManager, fileManager.pageSize);
    const meta = await manager.readMeta();
    if (meta.magic !== MAGIC) {
      await manager.writeMeta({
        magic: MAGIC,
        pageSize: manager.pageSize,
        rootPage: 2,
        treeDepth: 1,
        totalPages: 3,
        keyCount: 0n,
        freePageHead: 0,
      });
      const root = Buffer.alloc(manager.pageSize);
      root.writeUInt8(PageType.Leaf, 0);
      await manager.fileManager.writePage(2, root);
    }
    return manager;
  }

  async readMeta(): Promise<MetaPage> {
    const buffer = await this.fileManager.readPage(0);
    const magic = buffer.subarray(0, 16).toString().replace(/\0+$/, "");
    if (!magic) {
      return {
        magic: "",
        pageSize: this.pageSize,
        rootPage: 2,
        treeDepth: 1,
        totalPages: 3,
        keyCount: 0n,
        freePageHead: 0,
      };
    }

    const pageSize = buffer.readUInt32LE(16);
    const rootPage = buffer.readUInt32LE(20);
    const treeDepth = buffer.readUInt32LE(24);
    const totalPages = buffer.readUInt32LE(28);
    const keyCount = buffer.readBigUInt64LE(32);
    const freePageHead = buffer.readUInt32LE(40);
    return {
      magic,
      pageSize,
      rootPage,
      treeDepth,
      totalPages,
      keyCount,
      freePageHead,
    };
  }

  async writeMeta(meta: MetaPage): Promise<void> {
    const buffer = Buffer.alloc(this.pageSize);
    buffer.write(meta.magic.padEnd(16, "\0"), 0, "utf-8");
    buffer.writeUInt32LE(meta.pageSize, 16);
    buffer.writeUInt32LE(meta.rootPage, 20);
    buffer.writeUInt32LE(meta.treeDepth, 24);
    buffer.writeUInt32LE(meta.totalPages, 28);
    buffer.writeBigUInt64LE(meta.keyCount, 32);
    buffer.writeUInt32LE(meta.freePageHead, 40);
    await this.fileManager.writePage(0, buffer);
  }

  async allocatePage(): Promise<number> {
    const meta = await this.readMeta();
    let pageNumber = meta.freePageHead;
    if (pageNumber > 0) {
      const freeBuffer = await this.fileManager.readPage(pageNumber);
      meta.freePageHead = freeBuffer.readUInt32LE(0);
      await this.writeMeta(meta);
      return pageNumber;
    }

    pageNumber = meta.totalPages;
    meta.totalPages += 1;
    await this.writeMeta(meta);
    await this.fileManager.writePage(pageNumber, Buffer.alloc(this.pageSize));
    return pageNumber;
  }

  async freePage(pageNumber: number): Promise<void> {
    const meta = await this.readMeta();
    const buffer = Buffer.alloc(this.pageSize);
    buffer.writeUInt32LE(meta.freePageHead, 0);
    await this.fileManager.writePage(pageNumber, buffer);
    meta.freePageHead = pageNumber;
    await this.writeMeta(meta);
  }

  async readPage(pageNumber: number): Promise<Buffer> {
    return this.fileManager.readPage(pageNumber);
  }

  async writePage(pageNumber: number, data: Buffer): Promise<void> {
    if (data.length !== this.pageSize) {
      throw new Error("Page write size mismatch");
    }
    await this.fileManager.writePage(pageNumber, data);
  }

  async resetPage(pageNumber: number, type: PageType): Promise<void> {
    const buffer = Buffer.alloc(this.pageSize);
    buffer.writeUInt8(type, 0);
    await this.writePage(pageNumber, buffer);
  }

  async fragmentationStats(): Promise<{
    totalPages: number;
    freePages: number;
    fragmentation: number;
  }> {
    const meta = await this.readMeta();
    const freePages = await this.#collectFreePages(meta);
    const ratio = meta.totalPages === 0 ? 0 : freePages.length / meta.totalPages;
    return {
      totalPages: meta.totalPages,
      freePages: freePages.length,
      fragmentation: ratio,
    };
  }

  async vacuumFreePages(): Promise<{
    reclaimed: number;
    totalPages: number;
    freePages: number;
    fragmentation: number;
  }> {
    const meta = await this.readMeta();
    const freePages = await this.#collectFreePages(meta);
    if (freePages.length === 0) {
      return {
        reclaimed: 0,
        totalPages: meta.totalPages,
        freePages: 0,
        fragmentation: 0,
      };
    }
    const freeSet = new Set(freePages);

    let reclaimed = 0;
    let newTotal = meta.totalPages;
    while (newTotal - 1 >= 3) {
      const candidate = newTotal - 1;
      if (!freeSet.has(candidate)) {
        break;
      }
      freeSet.delete(candidate);
      reclaimed += 1;
      newTotal -= 1;
    }

    if (reclaimed === 0) {
      await this.#rewriteFreeList(meta, freePages);
      return {
        reclaimed: 0,
        totalPages: meta.totalPages,
        freePages: freePages.length,
        fragmentation: freePages.length / meta.totalPages,
      };
    }

    const remainingFreePages = Array.from(freeSet).filter((page) => page >= 3);
    remainingFreePages.sort((a, b) => a - b);
    await this.#rewriteFreeList(meta, remainingFreePages);
    meta.totalPages = newTotal;
    await this.writeMeta(meta);
    await this.fileManager.truncatePages(newTotal);

    const fragmentation =
      newTotal === 0 ? 0 : remainingFreePages.length / newTotal;
    return {
      reclaimed,
      totalPages: newTotal,
      freePages: remainingFreePages.length,
      fragmentation,
    };
  }

  async #collectFreePages(meta?: MetaPage): Promise<number[]> {
    const targetMeta = meta ?? (await this.readMeta());
    const freePages: number[] = [];
    const seen = new Set<number>();
    let cursor = targetMeta.freePageHead;
    while (cursor !== 0 && !seen.has(cursor)) {
      seen.add(cursor);
      if (cursor >= 3) {
        freePages.push(cursor);
      }
      const buffer = await this.fileManager.readPage(cursor);
      cursor = buffer.readUInt32LE(0);
    }
    return freePages;
  }

  async #rewriteFreeList(meta: MetaPage, pages: number[]): Promise<void> {
    let head = 0;
    for (const page of pages) {
      const buffer = Buffer.alloc(this.pageSize);
      buffer.writeUInt32LE(head, 0);
      await this.fileManager.writePage(page, buffer);
      head = page;
    }
    meta.freePageHead = head;
    await this.writeMeta(meta);
  }
}
