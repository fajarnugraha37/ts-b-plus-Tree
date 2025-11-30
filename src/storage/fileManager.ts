import { open } from "fs/promises";
import type { FileHandle } from "fs/promises";
import { PAGE_SIZE_BYTES } from "../constants.ts";

export interface FileManagerOptions {
  pageSize?: number;
  readAheadPages?: number;
}

export class FileManager {
  #handle: FileHandle | null = null;
  readonly pageSize: number;
  readonly readAheadPages: number;
  readonly filePath: string;

  private constructor(
    filePath: string,
    handle: FileHandle,
    pageSize: number,
    readAheadPages = 0,
  ) {
    this.#handle = handle;
    this.pageSize = pageSize;
    this.readAheadPages = readAheadPages;
    this.filePath = filePath;
  }

  static async openOrCreate(
    filePath: string,
    { pageSize = PAGE_SIZE_BYTES, readAheadPages = 0 }: FileManagerOptions = {},
  ): Promise<FileManager> {
    let handle: FileHandle;
    try {
      handle = await open(filePath, "r+");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        throw error;
      }
      handle = await open(filePath, "w+");
    }
    const manager = new FileManager(filePath, handle, pageSize, readAheadPages);
    await manager.#ensureMinimumPages(3);
    return manager;
  }

  async close(): Promise<void> {
    await this.#handle?.close();
    this.#handle = null;
  }

  async sync(): Promise<void> {
    await this.#handle?.sync();
  }

  async readPage(pageNumber: number): Promise<Buffer> {
    const buffer = Buffer.alloc(this.pageSize);
    await this.#ensureMinimumPages(pageNumber + 1);
    await this.#handle!.read(buffer, 0, this.pageSize, this.#offset(pageNumber));
    if (this.readAheadPages > 0) {
      for (let i = 1; i <= this.readAheadPages; i += 1) {
        const nextOffset = this.#offset(pageNumber + i);
        await this.#handle!.read(Buffer.alloc(0), 0, 0, nextOffset);
      }
    }
    return buffer;
  }

  async writePage(pageNumber: number, data: Buffer): Promise<void> {
    if (data.length !== this.pageSize) {
      throw new Error("Page writes must cover the entire page");
    }

    await this.#ensureMinimumPages(pageNumber + 1);
    await this.#handle!.write(data, 0, data.length, this.#offset(pageNumber));
  }

  async truncatePages(totalPages: number): Promise<void> {
    if (!this.#handle) {
      return;
    }
    await this.#handle.truncate(totalPages * this.pageSize);
  }

  async pageCount(): Promise<number> {
    const stats = await this.#handle!.stat();
    return Math.ceil(stats.size / this.pageSize);
  }

  async getFilePaths(): Promise<string[]> {
    return [this.filePath];
  }

  #offset(pageNumber: number): number {
    return pageNumber * this.pageSize;
  }

  async #ensureMinimumPages(pages: number): Promise<void> {
    const currentPages = await this.pageCount();
    if (currentPages >= pages) {
      return;
    }

    const missing = pages - currentPages;
    const padding = Buffer.alloc(missing * this.pageSize);
    await this.#handle!.write(padding, 0, padding.length, currentPages * this.pageSize);
  }
}
