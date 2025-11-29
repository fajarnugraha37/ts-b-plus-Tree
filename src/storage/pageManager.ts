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
}
