import { expect, test } from "bun:test";
import {
  bufferToBigInt,
  bigintToBuffer,
  normalizeKeyInput,
  normalizeValueInput,
} from "../../src/utils/codec.ts";
import {
  deserializeLeaf,
  deserializeInternal,
  serializeLeaf,
  serializeInternal,
} from "../../src/tree/pages.ts";
import type { LeafPage } from "../../src/tree/pages.ts";
import { KEY_SIZE_BYTES, PageType } from "../../src/constants.ts";

test("codec converts between bigint and buffer", () => {
  const key = 0x1234_5678_9abcdn;
  const buffer = bigintToBuffer(key);
  expect(buffer.length).toBe(KEY_SIZE_BYTES);
  expect(bufferToBigInt(buffer)).toBe(key);
});

test("normalizeKeyInput accepts numbers, bigint, and buffers", () => {
  expect(bufferToBigInt(normalizeKeyInput(42))).toBe(42n);
  expect(bufferToBigInt(normalizeKeyInput(42n))).toBe(42n);

  const buf = Buffer.alloc(KEY_SIZE_BYTES);
  buf[KEY_SIZE_BYTES - 1] = 0xff;
  expect(bufferToBigInt(normalizeKeyInput(buf))).toBe(255n);
});

test("normalizeValueInput returns a copied buffer", () => {
  const original = Buffer.from("hello");
  const normalized = normalizeValueInput(original);
  expect(normalized.equals(original)).toBeTrue();
  expect(normalized).not.toBe(original);
});

test("leaf page serialization round-trip", () => {
  const leaf: LeafPage = {
    type: PageType.Leaf as const,
    keyCount: 2,
    rightSibling: 99,
    cells: [
      {
        key: 1n,
        inlineValue: Buffer.from("a"),
        valueLength: 1,
        overflowPage: 0,
      },
      {
        key: 2n,
        inlineValue: Buffer.from("b"),
        valueLength: 1,
        overflowPage: 0,
      },
    ],
  };
  const buffer = Buffer.alloc(4096);
  serializeLeaf(leaf, buffer);
  const parsed = deserializeLeaf(buffer);
  expect(parsed.keyCount).toBe(2);
  expect(parsed.rightSibling).toBe(99);
  expect(parsed.cells.map((c) => c.key)).toEqual([1n, 2n]);
  expect(parsed.cells[0]!.inlineValue.toString()).toBe("a");
  expect(parsed.cells[0]!.valueLength).toBe(1);
  expect(parsed.cells[0]!.overflowPage).toBe(0);
});

test("internal page serialization round-trip", () => {
  const internal = {
    type: PageType.Internal as const,
    keyCount: 2,
    rightSibling: 5,
    leftChild: 10,
    cells: [
      { key: 100n, child: 11 },
      { key: 200n, child: 12 },
    ],
  };
  const buffer = Buffer.alloc(4096);
  serializeInternal(internal, buffer);
  const parsed = deserializeInternal(buffer);
  expect(parsed.leftChild).toBe(10);
  expect(parsed.rightSibling).toBe(5);
  expect(parsed.cells.map((c) => c.key)).toEqual([100n, 200n]);
  expect(parsed.cells.map((c) => c.child)).toEqual([11, 12]);
});
