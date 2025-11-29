import { open, stat } from "fs/promises";
import type { FileHandle } from "fs/promises";
import type { PageManager } from "./pageManager.ts";

const WAL_MAGIC = "TSWALV1";
const HEADER_SIZE = 32;
const RECORD_HEADER_SIZE = 20;

enum RecordType {
  Begin = 0,
  Page = 1,
  Commit = 2,
}

interface PendingFrame {
  pageNumber: number;
  data: Buffer;
}

export interface WalStats {
  framesWritten: number;
  commits: number;
  pendingTransactions: number;
}

export class WriteAheadLog {
  readonly walPath: string;
  readonly pageSize: number;
  #handle: FileHandle | null = null;
  #nextTxId = 1;
  #pending = new Map<number, PendingFrame[]>();
  #stats: WalStats = { framesWritten: 0, commits: 0, pendingTransactions: 0 };

  constructor(walPath: string, pageSize: number) {
    this.walPath = walPath;
    this.pageSize = pageSize;
  }

  async open(): Promise<void> {
    if (!this.#handle) {
      this.#handle = await open(this.walPath, "a+");
      await this.#ensureHeader();
    }
  }

  async close(): Promise<void> {
    await this.#handle?.close();
    this.#handle = null;
  }

  async beginTransaction(): Promise<number> {
    await this.open();
    const txId = this.#nextTxId++;
    this.#pending.set(txId, []);
    this.#stats.pendingTransactions = this.#pending.size;
    await this.#writeRecord(RecordType.Begin, txId, 0, Buffer.alloc(0));
    return txId;
  }

  async writePage(txId: number, pageNumber: number, data: Buffer): Promise<void> {
    if (data.length !== this.pageSize) {
      throw new Error(`WAL frames must match page size ${this.pageSize}`);
    }
    const frames = this.#pending.get(txId);
    if (!frames) {
      throw new Error(`Unknown WAL transaction ${txId}`);
    }
    frames.push({
      pageNumber,
      data: Buffer.from(data),
    });
  }

  async commitTransaction(txId: number, skipSync = false): Promise<void> {
    const frames = this.#pending.get(txId);
    if (!frames) {
      return;
    }
    await this.#ensureHeader();
    for (const frame of frames) {
      await this.#writeRecord(RecordType.Page, txId, frame.pageNumber, frame.data);
      this.#stats.framesWritten += 1;
    }
    await this.#writeRecord(RecordType.Commit, txId, 0, Buffer.alloc(0));
    if (!skipSync) {
      await this.#handle!.sync();
    }
    this.#pending.delete(txId);
    this.#stats.pendingTransactions = this.#pending.size;
    this.#stats.commits += 1;
  }

  rollbackTransaction(txId: number): void {
    this.#pending.delete(txId);
    this.#stats.pendingTransactions = this.#pending.size;
  }

  async replay(pageManager: PageManager): Promise<void> {
    await this.open();
    await this.#handle!.sync();
    const reader = await open(this.walPath, "r+");
    try {
      const stats = await reader.stat();
      if (stats.size <= HEADER_SIZE) {
        return;
      }
      await this.#validateHeader(reader);

      const pending = new Map<number, PendingFrame[]>();
      const committed = new Map<number, PendingFrame[]>();
      let offset = HEADER_SIZE;
      const headerBuf = Buffer.alloc(RECORD_HEADER_SIZE);

      while (offset + RECORD_HEADER_SIZE <= stats.size) {
        await reader.read(headerBuf, 0, RECORD_HEADER_SIZE, offset);
        const type = headerBuf.readUInt32LE(0);
        const txId = headerBuf.readUInt32LE(4);
        const pageNumber = headerBuf.readUInt32LE(8);
        const length = headerBuf.readUInt32LE(12);
        const checksum = headerBuf.readUInt32LE(16);
        offset += RECORD_HEADER_SIZE;

        if (type === RecordType.Begin) {
          if (!pending.has(txId)) {
            pending.set(txId, []);
          }
          continue;
        }

        if (type === RecordType.Page) {
          if (length !== this.pageSize || offset + length > stats.size) {
            break;
          }
          const data = Buffer.alloc(length);
          await reader.read(data, 0, length, offset);
          offset += length;
          if (checksum !== checksumBuffer(data)) {
            continue;
          }
          const frames = pending.get(txId) ?? [];
          frames.push({ pageNumber, data });
          pending.set(txId, frames);
        } else if (type === RecordType.Commit) {
          const frames = pending.get(txId);
          if (frames) {
            committed.set(txId, frames);
            pending.delete(txId);
          }
        } else {
          break;
        }
      }

      for (const frames of committed.values()) {
        for (const frame of frames) {
          await pageManager.writePage(frame.pageNumber, frame.data);
        }
      }

      await reader.truncate(HEADER_SIZE);
      await reader.sync();
    } finally {
      await reader.close();
    }
  }

  async checkpoint(pageManager: PageManager): Promise<void> {
    await this.replay(pageManager);
  }

  async reset(): Promise<void> {
    await this.#handle?.close();
    this.#handle = await open(this.walPath, "w+");
    this.#stats = { framesWritten: 0, commits: 0, pendingTransactions: 0 };
    await this.#ensureHeader();
  }

  getStats(): WalStats {
    return { ...this.#stats };
  }

  async #writeRecord(
    type: RecordType,
    txId: number,
    pageNumber: number,
    data: Buffer,
  ): Promise<void> {
    const header = Buffer.alloc(RECORD_HEADER_SIZE);
    header.writeUInt32LE(type, 0);
    header.writeUInt32LE(txId, 4);
    header.writeUInt32LE(pageNumber, 8);
    header.writeUInt32LE(data.length, 12);
    header.writeUInt32LE(checksumBuffer(data), 16);
    await this.#handle!.write(header);
    if (data.length > 0) {
      await this.#handle!.write(data);
    }
  }

  async #ensureHeader(): Promise<void> {
    if (!this.#handle) {
      return;
    }
    const stats = await this.#handle.stat();
    if (stats.size >= HEADER_SIZE) {
      return;
    }
    const header = Buffer.alloc(HEADER_SIZE);
    header.write(WAL_MAGIC, 0, "ascii");
    header.writeUInt32LE(this.pageSize, 16);
    await this.#handle.write(header, 0, HEADER_SIZE, 0);
    await this.#handle.sync();
  }

  async #validateHeader(handle: FileHandle): Promise<void> {
    const header = Buffer.alloc(HEADER_SIZE);
    await handle.read(header, 0, HEADER_SIZE, 0);
    const magic = header.subarray(0, WAL_MAGIC.length).toString("ascii");
    if (magic !== WAL_MAGIC) {
      header.fill(0);
      header.write(WAL_MAGIC, 0, "ascii");
      header.writeUInt32LE(this.pageSize, 16);
      await handle.write(header, 0, HEADER_SIZE, 0);
      await handle.sync();
    }
  }
}

function checksumBuffer(buffer: Buffer): number {
  let sum = 0;
  for (const byte of buffer.values()) {
    sum = (sum + byte) & 0xffffffff;
  }
  return sum >>> 0;
}
