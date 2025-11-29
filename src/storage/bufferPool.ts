import { PageManager } from "./pageManager.ts";
import { WriteAheadLog } from "./wal.ts";

interface BufferFrame {
  pageNumber: number;
  buffer: Buffer;
  dirty: boolean;
  pinCount: number;
  lastAccess: number;
}

export interface BufferPoolOptions {
  capacity: number;
  wal?: WriteAheadLog;
}

export interface BufferPoolStats {
  pageLoads: number;
  pageFlushes: number;
  evictions: number;
  maxResidentPages: number;
}

export class BufferPool {
  readonly capacity: number;
  readonly pageManager: PageManager;
  readonly wal?: WriteAheadLog;
  readonly frames = new Map<number, BufferFrame>();
  #stats: BufferPoolStats = {
    pageLoads: 0,
    pageFlushes: 0,
    evictions: 0,
    maxResidentPages: 0,
  };

  constructor(pageManager: PageManager, options: BufferPoolOptions) {
    this.capacity = options.capacity;
    this.pageManager = pageManager;
    this.wal = options.wal;
  }

  async getPage(pageNumber: number): Promise<Buffer> {
    let frame = this.frames.get(pageNumber);
    if (!frame) {
      const buffer = await this.pageManager.readPage(pageNumber);
      frame = {
        pageNumber,
        buffer,
        dirty: false,
        pinCount: 0,
        lastAccess: Date.now(),
      };
      await this.#ensureCapacity();
      this.frames.set(pageNumber, frame);
      this.#stats.pageLoads += 1;
      this.#updateMaxResident();
    }

    frame.pinCount += 1;
    frame.lastAccess = Date.now();
    return frame.buffer;
  }

  unpin(pageNumber: number, dirty = false): void {
    const frame = this.frames.get(pageNumber);
    if (!frame) {
      throw new Error(`Page ${pageNumber} not found in buffer pool`);
    }
    if (frame.pinCount === 0) {
      throw new Error("Cannot unpin an unpinned page");
    }
    frame.pinCount -= 1;
    frame.dirty ||= dirty;
  }

  async flushPage(pageNumber: number): Promise<void> {
    const frame = this.frames.get(pageNumber);
    if (!frame || !frame.dirty) {
      return;
    }
    const walData = Buffer.from(frame.buffer);
    if (this.wal) {
      const txId = await this.wal.beginTransaction();
      await this.wal.writePage(txId, pageNumber, walData);
      await this.wal.commitTransaction(txId);
    }
    await this.pageManager.writePage(pageNumber, frame.buffer);
    frame.dirty = false;
    this.#stats.pageFlushes += 1;
  }

  async flushAll(): Promise<void> {
    for (const [pageNumber] of this.frames) {
      await this.flushPage(pageNumber);
    }
  }

  async evictPage(): Promise<number | null> {
    let candidate: BufferFrame | null = null;
    for (const frame of this.frames.values()) {
      if (frame.pinCount > 0) {
        continue;
      }
      if (!candidate || frame.lastAccess < candidate.lastAccess) {
        candidate = frame;
      }
    }

    if (!candidate) {
      return null;
    }

    await this.flushPage(candidate.pageNumber);
    this.frames.delete(candidate.pageNumber);
    this.#stats.evictions += 1;
    return candidate.pageNumber;
  }

  dropPage(pageNumber: number): void {
    const frame = this.frames.get(pageNumber);
    if (!frame) {
      return;
    }
    if (frame.pinCount > 0) {
      throw new Error("Cannot drop a pinned page");
    }
    this.frames.delete(pageNumber);
  }

  getStats(): BufferPoolStats {
    return { ...this.#stats };
  }

  async #ensureCapacity(): Promise<void> {
    if (this.frames.size < this.capacity) {
      return;
    }

    const evicted = await this.evictPage();
    if (evicted === null) {
      throw new Error("Buffer pool is full and all pages are pinned");
    }
  }

  #updateMaxResident(): void {
    if (this.frames.size > this.#stats.maxResidentPages) {
      this.#stats.maxResidentPages = this.frames.size;
    }
  }
}
