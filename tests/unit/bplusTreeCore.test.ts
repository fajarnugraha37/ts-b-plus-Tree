import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";
import { VALUE_SIZE_BYTES } from "../../src/constants.ts";

async function withTree<T>(fn: (tree: BPlusTree) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), "ts-btree-unit-"));
  const filePath = join(dir, "unit.db");
  const tree = await BPlusTree.open({ filePath });
  try {
    return await fn(tree);
  } finally {
    await tree.close();
    await rm(dir, { recursive: true, force: true });
  }
}

async function withTreePath<T>(fn: (filePath: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), "ts-btree-unit-"));
  const filePath = join(dir, "unit.db");
  try {
    return await fn(filePath);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

function bufferFromNumber(n: number): Buffer {
  const buffer = Buffer.alloc(VALUE_SIZE_BYTES);
  buffer.writeUInt32LE(n, 0);
  return buffer;
}

test("basic CRUD + range usage", async () => {
  await withTree(async (tree) => {
    await tree.set(1, bufferFromNumber(10));
    await tree.set(2, bufferFromNumber(20));
    await tree.set(3, bufferFromNumber(30));

    expect(await tree.get(1)).not.toBeNull();
    expect(await tree.get(2)).not.toBeNull();
    expect(await tree.get(3)).not.toBeNull();

    await tree.set(2, bufferFromNumber(22)); // overwrite
    expect((await tree.get(2))!.readUInt32LE(0)).toBe(22);

    const rangeValues: number[] = [];
    for await (const entry of tree.range(1, 3)) {
      rangeValues.push(entry.value.readUInt32LE(0));
    }
    expect(rangeValues).toEqual([10, 22, 30]);

    expect(await tree.delete(2)).toBeTrue();
    expect(await tree.get(2)).toBeNull();
    expect(tree.meta.keyCount).toBe(2n);
  });
});

test("set/get maintains ordering across larger range", async () => {
  await withTree(async (tree) => {
    for (let i = 0; i < 200; i += 1) {
      await tree.set(i, bufferFromNumber(i));
    }

    for (let i = 0; i < 200; i += 1) {
      const value = await tree.get(i);
      expect(value).not.toBeNull();
      expect(value!.readUInt32LE(0)).toBe(i);
    }

    let seen = 0;
    for await (const row of tree.range(0, 199)) {
      expect(row.value.readUInt32LE(0)).toBe(seen);
      seen += 1;
    }
    expect(seen).toBe(200);
  });
});

test("delete removes keys and triggers rebalancing", async () => {
  await withTree(async (tree) => {
    const total = 400;
    for (let i = 0; i < total; i += 1) {
      await tree.set(i, bufferFromNumber(i));
    }
    expect(tree.meta.keyCount).toBe(BigInt(total));

    for (let i = 0; i < total; i += 2) {
      const removed = await tree.delete(i);
      expect(removed).toBeTrue();
    }

    expect(tree.meta.keyCount).toBe(BigInt(total / 2));

    for (let i = 0; i < total; i += 1) {
      const value = await tree.get(i);
      if (i % 2 === 0) {
        expect(value).toBeNull();
      } else {
        expect(value).not.toBeNull();
        expect(value!.readUInt32LE(0)).toBe(i);
      }
    }

    let seen = 0;
    for await (const row of tree.range(1, total - 1)) {
      expect(row.value.readUInt32LE(0) % 2).toBe(1);
      seen += 1;
    }
    expect(seen).toBe(total / 2);

    expect(await tree.consistencyCheck()).toBeTrue();
  });
});

test("data persists across reopen and keeps stats sane", async () => {
  await withTreePath(async (filePath) => {
    const first = await BPlusTree.open({ filePath });
    for (let i = 0; i < 128; i += 1) {
      await first.set(i, bufferFromNumber(i * 2));
    }
    const firstStats = first.bufferPool.getStats();
    expect(firstStats.maxResidentPages).toBeLessThanOrEqual(first.bufferPool.capacity);
    await first.close();

    const reopened = await BPlusTree.open({ filePath });
    expect(reopened.meta.keyCount).toBe(128n);
    expect(reopened.meta.treeDepth).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < 128; i += 1) {
      const value = await reopened.get(i);
      expect(value).not.toBeNull();
      expect(value!.readUInt32LE(0)).toBe(i * 2);
    }

    await reopened.close();
  });
});
