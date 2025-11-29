import {
  KEY_SIZE_BYTES,
  MAX_INTERNAL_KEYS,
  MAX_LEAF_KEYS,
  PageType,
  PAGE_HEADER_SIZE,
} from "../constants.ts";
import { bufferToBigInt, bigintToBuffer } from "../utils/codec.ts";

export interface LeafCell {
  key: bigint;
  inlineValue: Buffer;
  valueLength: number;
  overflowPage: number;
}

export interface LeafPage {
  type: PageType.Leaf;
  keyCount: number;
  rightSibling: number;
  cells: LeafCell[];
}

export interface InternalCell {
  key: bigint;
  child: number;
}

export interface InternalPage {
  type: PageType.Internal;
  keyCount: number;
  rightSibling: number;
  leftChild: number;
  cells: InternalCell[];
}

export type BTreePage = LeafPage | InternalPage;

function readHeader(buffer: Buffer): {
  type: PageType;
  keyCount: number;
  rightSibling: number;
  payloadOffset: number;
} {
  return {
    type: buffer.readUInt8(0),
    keyCount: buffer.readUInt16LE(2),
    rightSibling: buffer.readUInt32LE(4),
    payloadOffset: buffer.readUInt16LE(8),
  };
}

export function deserializeLeaf(buffer: Buffer): LeafPage {
  const header = readHeader(buffer);
  if (header.type !== PageType.Leaf) {
    throw new Error("Page is not a leaf");
  }
  const cells: LeafCell[] = [];
  const pointerStart = PAGE_HEADER_SIZE;
  const pointers =
    header.keyCount <= 0
      ? 0
      : Math.min(header.keyCount, MAX_LEAF_KEYS) * 2;
  for (let i = 0; i < header.keyCount && i < MAX_LEAF_KEYS; i += 1) {
    const pointerOffset = pointerStart + i * 2;
    const cellOffset = buffer.readUInt16LE(pointerOffset);
    if (cellOffset === 0) {
      continue;
    }
    const keyLength = buffer.readUInt16LE(cellOffset);
    const inlineLength = buffer.readUInt16LE(cellOffset + 2);
    const totalValueLength = buffer.readUInt32LE(cellOffset + 4);
    const overflowPage = buffer.readUInt32LE(cellOffset + 8);
    const keyBuffer = buffer.subarray(
      cellOffset + 12,
      cellOffset + 12 + keyLength,
    );
    const valueBuffer = buffer.subarray(
      cellOffset + 12 + keyLength,
      cellOffset + 12 + keyLength + inlineLength,
    );
    cells.push({
      key: bufferToBigInt(keyBuffer),
      inlineValue: Buffer.from(valueBuffer),
      valueLength: totalValueLength,
      overflowPage,
    });
  }

  return {
    type: PageType.Leaf,
    keyCount: cells.length,
    rightSibling: header.rightSibling,
    cells,
  };
}

export function serializeLeaf(page: LeafPage, target: Buffer): void {
  if (page.cells.length > MAX_LEAF_KEYS) {
    throw new Error("Leaf page overflow");
  }

  target.fill(0);
  target.writeUInt8(PageType.Leaf, 0);
  target.writeUInt16LE(page.cells.length, 2);
  target.writeUInt32LE(page.rightSibling, 4);
  let pointerOffset = PAGE_HEADER_SIZE;
  let payloadOffset = target.length;
  for (const cell of page.cells) {
    const keyBuffer = bigintToBuffer(cell.key);
    const inline = cell.inlineValue;
    const recordSize = 12 + keyBuffer.length + inline.length;
    payloadOffset -= recordSize;
    if (payloadOffset <= pointerOffset) {
      throw new Error("Leaf page overflow");
    }
    target.writeUInt16LE(keyBuffer.length, payloadOffset);
    target.writeUInt16LE(inline.length, payloadOffset + 2);
    target.writeUInt32LE(cell.valueLength, payloadOffset + 4);
    target.writeUInt32LE(cell.overflowPage, payloadOffset + 8);
    keyBuffer.copy(target, payloadOffset + 12);
    inline.copy(target, payloadOffset + 12 + keyBuffer.length);
    target.writeUInt16LE(payloadOffset, pointerOffset);
    pointerOffset += 2;
  }
  target.writeUInt16LE(payloadOffset, 8);
}

export function deserializeInternal(buffer: Buffer): InternalPage {
  const header = readHeader(buffer);
  if (header.type !== PageType.Internal) {
    throw new Error("Page is not an internal node");
  }

  let offset = PAGE_HEADER_SIZE;
  const leftChild = buffer.readUInt32LE(offset);
  offset += 4;

  const cells: InternalCell[] = [];
  for (let i = 0; i < header.keyCount && i < MAX_INTERNAL_KEYS; i += 1) {
    const keyBuffer = buffer.subarray(offset, offset + KEY_SIZE_BYTES);
    offset += KEY_SIZE_BYTES;
    const child = buffer.readUInt32LE(offset);
    offset += 4;
    cells.push({
      key: bufferToBigInt(keyBuffer),
      child,
    });
  }

  return {
    type: PageType.Internal,
    keyCount: cells.length,
    rightSibling: header.rightSibling,
    leftChild,
    cells,
  };
}

export function serializeInternal(page: InternalPage, target: Buffer): void {
  if (page.cells.length > MAX_INTERNAL_KEYS) {
    throw new Error("Internal page overflow");
  }

  target.fill(0);
  target.writeUInt8(PageType.Internal, 0);
  target.writeUInt16LE(page.cells.length, 2);
  target.writeUInt32LE(page.rightSibling, 4);
  target.writeUInt16LE(0, 8);

  let offset = PAGE_HEADER_SIZE;
  target.writeUInt32LE(page.leftChild, offset);
  offset += 4;

  for (const cell of page.cells) {
    const keyBuffer = bigintToBuffer(cell.key);
    keyBuffer.copy(target, offset);
    offset += KEY_SIZE_BYTES;
    target.writeUInt32LE(cell.child, offset);
    offset += 4;
  }
}
