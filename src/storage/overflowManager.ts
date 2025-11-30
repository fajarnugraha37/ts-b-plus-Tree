import { OVERFLOW_HEADER_SIZE, PageType } from "../constants.ts";
import type { BufferPool } from "./bufferPool.ts";
import type { PageManager } from "./pageManager.ts";
import { deserializeOverflow, serializeOverflow } from "../tree/pages.ts";

export class OverflowManager {
  readonly pageManager: PageManager;
  readonly bufferPool: BufferPool;

  constructor(pageManager: PageManager, bufferPool: BufferPool) {
    this.pageManager = pageManager;
    this.bufferPool = bufferPool;
  }

  async allocateChain(data: Buffer): Promise<number> {
    if (data.length === 0) {
      return 0;
    }
    const payloadSize = this.pageManager.pageSize - OVERFLOW_HEADER_SIZE;
    if (payloadSize <= 0) {
      throw new Error("Overflow payload capacity must be positive");
    }
    let head = 0;
    let offset = 0;
    let pendingPageNumber = 0;
    let pendingBuffer: Buffer | null = null;
    while (offset < data.length) {
      const chunkLength = Math.min(payloadSize, data.length - offset);
      const pageNumber = await this.pageManager.allocatePage();
      const buffer = await this.bufferPool.getPage(pageNumber);
      const payload = data.subarray(offset, offset + chunkLength);
      serializeOverflow(
        {
          type: PageType.Overflow,
          next: 0,
          length: chunkLength,
          payload,
        },
        buffer,
      );

      if (pendingBuffer && pendingPageNumber) {
        pendingBuffer.writeUInt32LE(pageNumber, 4);
        this.bufferPool.unpin(pendingPageNumber, true);
        await this.bufferPool.flushPage(pendingPageNumber);
        this.bufferPool.dropPage(pendingPageNumber);
      }

      if (head === 0) {
        head = pageNumber;
      }

      offset += chunkLength;
      if (offset >= data.length) {
        this.bufferPool.unpin(pageNumber, true);
        await this.bufferPool.flushPage(pageNumber);
        this.bufferPool.dropPage(pageNumber);
        pendingBuffer = null;
        pendingPageNumber = 0;
      } else {
        pendingBuffer = buffer;
        pendingPageNumber = pageNumber;
      }
    }

    if (pendingBuffer && pendingPageNumber) {
      this.bufferPool.unpin(pendingPageNumber, true);
      await this.bufferPool.flushPage(pendingPageNumber);
      this.bufferPool.dropPage(pendingPageNumber);
    }

    return head;
  }

  async readChain(head: number, totalLength: number): Promise<Buffer> {
    if (head === 0) {
      return Buffer.alloc(0);
    }
    const target = Buffer.alloc(totalLength);
    let cursor = head;
    let offset = 0;
    while (cursor !== 0 && offset < totalLength) {
      const buffer = await this.bufferPool.getPage(cursor);
      const page = deserializeOverflow(buffer);
      const copyLength = Math.min(page.length, totalLength - offset);
      page.payload.copy(target, offset, 0, copyLength);
      offset += copyLength;
      const next = page.next;
      this.bufferPool.unpin(cursor, false);
      cursor = next;
    }
    if (offset !== totalLength) {
      throw new Error("Overflow chain truncated during read");
    }
    return target;
  }

  async freeChain(head: number): Promise<void> {
    let cursor = head;
    while (cursor !== 0) {
      const buffer = await this.bufferPool.getPage(cursor);
      const next = buffer.readUInt32LE(4);
      this.bufferPool.unpin(cursor, false);
      this.bufferPool.dropPage(cursor);
      await this.pageManager.freePage(cursor);
      cursor = next;
    }
  }
}
