import {
  KEY_SIZE_BYTES,
  MAX_INTERNAL_KEYS,
  MAX_LEAF_KEYS,
  PageType,
  VALUE_SIZE_BYTES,
  PAGE_HEADER_SIZE,
} from "../constants.ts";
import { bufferToBigInt, bigintToBuffer } from "../utils/codec.ts";

export interface LeafCell {
  key: bigint;
  value: Buffer;
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
} {
  return {
    type: buffer.readUInt8(0),
    keyCount: buffer.readUInt16LE(2),
    rightSibling: buffer.readUInt32LE(4),
  };
}

export function deserializeLeaf(buffer: Buffer): LeafPage {
  const header = readHeader(buffer);
  if (header.type !== PageType.Leaf) {
    throw new Error("Page is not a leaf");
  }

  const cells: LeafCell[] = [];
  let offset = PAGE_HEADER_SIZE;
  for (let i = 0; i < header.keyCount && i < MAX_LEAF_KEYS; i += 1) {
    const keyBuffer = buffer.subarray(offset, offset + KEY_SIZE_BYTES);
    offset += KEY_SIZE_BYTES;
    const valueBuffer = buffer.subarray(offset, offset + VALUE_SIZE_BYTES);
    offset += VALUE_SIZE_BYTES;
    cells.push({
      key: bufferToBigInt(keyBuffer),
      value: Buffer.from(valueBuffer),
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

  let offset = PAGE_HEADER_SIZE;
  for (const cell of page.cells) {
    const keyBuffer = bigintToBuffer(cell.key);
    keyBuffer.copy(target, offset);
    offset += KEY_SIZE_BYTES;
    cell.value.copy(target, offset);
    offset += VALUE_SIZE_BYTES;
  }
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
