import { readdir, stat } from "fs/promises";
import { basename, dirname, join } from "path";
import { FileManager, type FileManagerOptions } from "./fileManager.ts";
import { PageType } from "../constants.ts";

export interface SegmentedFileManagerOptions extends FileManagerOptions {
  segmentPages: number;
}

export class SegmentedFileManager {
  readonly basePath: string;
  readonly segmentPages: number;
  readonly pageSize: number;
  readonly readAheadPages: number;
  #segments = new Map<number, FileManager>();

  private constructor(
    basePath: string,
    segmentPages: number,
    first: FileManager,
    options: FileManagerOptions,
  ) {
    this.basePath = basePath;
    this.segmentPages = segmentPages;
    this.pageSize = first.pageSize;
    this.readAheadPages = options.readAheadPages ?? 0;
    this.#segments.set(0, first);
  }

  static async openOrCreate(
    basePath: string,
    options: SegmentedFileManagerOptions,
  ): Promise<SegmentedFileManager> {
    if (!Number.isInteger(options.segmentPages) || options.segmentPages <= 0) {
      throw new Error("segmentPages must be a positive integer");
    }
    const first = await FileManager.openOrCreate(basePath, options);
    const manager = new SegmentedFileManager(basePath, options.segmentPages, first, options);
    return manager;
  }

  async close(): Promise<void> {
    for (const segment of this.#segments.values()) {
      await segment.close();
    }
    this.#segments.clear();
  }

  async sync(): Promise<void> {
    for (const segment of this.#segments.values()) {
      await segment.sync();
    }
  }

  async readPage(pageNumber: number): Promise<Buffer> {
    const { segmentIndex, offset } = this.#locate(pageNumber);
    const segment = await this.#segment(segmentIndex);
    return segment.readPage(offset);
  }

  async writePage(pageNumber: number, data: Buffer): Promise<void> {
    const { segmentIndex, offset } = this.#locate(pageNumber);
    const segment = await this.#segment(segmentIndex);
    await segment.writePage(offset, data);
  }

  async resetPage(pageNumber: number, type: PageType): Promise<void> {
    const buffer = Buffer.alloc(this.pageSize);
    buffer.writeUInt8(type, 0);
    await this.writePage(pageNumber, buffer);
  }

  async pageCount(): Promise<number> {
    let max = 0;
    const seen = this.#segments.keys();
    for (const index of seen) {
      const manager = await this.#segment(index);
      const count = await manager.pageCount();
      const candidate = index * this.segmentPages + count;
      if (candidate > max) {
        max = candidate;
      }
    }
    if (max === 0) {
      const manager = await this.#segment(0);
      max = await manager.pageCount();
    }
    return max;
  }

  async truncatePages(totalPages: number): Promise<void> {
    const segmentsNeeded = Math.max(1, Math.ceil(totalPages / this.segmentPages));
    for (const [index, manager] of this.#segments) {
      const fullSegments = segmentsNeeded - 1;
      if (index < fullSegments) {
        await manager.truncatePages(this.segmentPages);
      } else if (index === fullSegments) {
        const pagesInSegment = totalPages - index * this.segmentPages;
        await manager.truncatePages(Math.max(0, pagesInSegment));
      } else {
        await manager.truncatePages(0);
      }
    }
  }

  async fileExists(index: number): Promise<boolean> {
    const path = this.#segmentPath(index);
    try {
      await stat(path);
      return true;
    } catch {
      return false;
    }
  }

  async getFilePaths(): Promise<string[]> {
    const dir = dirname(this.basePath);
    const base = basename(this.basePath);
    try {
      const entries = await readdir(dir);
      const matches = entries
        .filter((name) => name === base || name.startsWith(`${base}.seg`))
        .map((name) => join(dir, name));
      if (!matches.includes(this.basePath)) {
        matches.unshift(this.basePath);
      }
      return matches;
    } catch {
      return [this.basePath];
    }
  }

  async #segment(index: number): Promise<FileManager> {
    let manager = this.#segments.get(index);
    if (manager) {
      return manager;
    }
    const path = this.#segmentPath(index);
    manager = await FileManager.openOrCreate(path, {
      pageSize: this.pageSize,
      readAheadPages: this.readAheadPages,
    });
    this.#segments.set(index, manager);
    return manager;
  }

  #segmentPath(index: number): string {
    if (index === 0) {
      return this.basePath;
    }
    return `${this.basePath}.seg${index}`;
  }

  #locate(pageNumber: number): { segmentIndex: number; offset: number } {
    const segmentIndex = Math.floor(pageNumber / this.segmentPages);
    const offset = pageNumber - segmentIndex * this.segmentPages;
    return { segmentIndex, offset };
  }
}
