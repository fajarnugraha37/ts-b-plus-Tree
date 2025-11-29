import { open } from "fs/promises";
import type { FileHandle } from "fs/promises";

interface WalFrame {
  pageNumber: number;
  checksum: number;
  data: Buffer;
}

export class WriteAheadLog {
  readonly walPath: string;
  #handle: FileHandle | null = null;

  constructor(walPath: string) {
    this.walPath = walPath;
  }

  async open(): Promise<void> {
    if (!this.#handle) {
      this.#handle = await open(this.walPath, "a+");
    }
  }

  async close(): Promise<void> {
    await this.#handle?.close();
    this.#handle = null;
  }

  async append(frame: WalFrame): Promise<void> {
    if (!this.#handle) {
      await this.open();
    }
    const header = Buffer.alloc(12);
    header.writeUInt32LE(frame.pageNumber, 0);
    header.writeUInt32LE(frame.checksum, 4);
    header.writeUInt32LE(frame.data.length, 8);
    await this.#handle!.write(header);
    await this.#handle!.write(frame.data);
  }

  async checkpoint(): Promise<void> {
    // Stub method to keep parity with AGENT.md specification.
  }
}
