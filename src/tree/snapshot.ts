import { createReadStream, createWriteStream } from "fs";
import { pipeline } from "stream/promises";
import { bufferToBigInt, normalizeValueInput, normalizeKeyInput } from "../utils/codec.ts";
import type { ValueSerializer } from "../utils/codec.ts";
import { BPlusTree } from "./bplusTree.ts";

const SNAPSHOT_MAGIC = "TSBTREE_SNAPSHOT_v1";

export interface SnapshotRecord {
  key: Buffer;
  value: Buffer;
}

export async function exportSnapshot(
  tree: BPlusTree,
  outputPath: string,
): Promise<void> {
  const writable = createWriteStream(outputPath);
  const header = Buffer.alloc(64);
  header.write(SNAPSHOT_MAGIC, 0, "ascii");
  header.writeBigUInt64LE(BigInt(tree.meta.keyCount), 32);
  writable.write(header);

  for await (const { key, value } of tree.range(
    normalizeKeyInput(0),
    normalizeKeyInput(Buffer.alloc(8, 0xff)),
  )) {
    const recordHeader = Buffer.alloc(8);
    recordHeader.writeUInt32LE(key.length, 0);
    recordHeader.writeUInt32LE(value.length, 4);
    writable.write(recordHeader);
    writable.write(key);
    writable.write(value);
  }

  writable.end();
}

export async function importSnapshot(
  tree: BPlusTree,
  inputPath: string,
): Promise<void> {
  const readable = createReadStream(inputPath);
  const chunks: Buffer[] = [];
  for await (const chunk of readable) {
    chunks.push(chunk as Buffer);
  }
  const buffer = Buffer.concat(chunks);
  const magic = buffer.subarray(0, SNAPSHOT_MAGIC.length).toString("ascii");
  if (magic !== SNAPSHOT_MAGIC) {
    throw new Error("invalid snapshot magic");
  }
  let offset = 64;
  while (offset < buffer.length) {
    const keyLength = buffer.readUInt32LE(offset);
    const valueLength = buffer.readUInt32LE(offset + 4);
    offset += 8;
    const key = buffer.subarray(offset, offset + keyLength);
    offset += keyLength;
    const value = buffer.subarray(offset, offset + valueLength);
    offset += valueLength;
    await tree.set(key, normalizeValueInput(value));
  }
}
