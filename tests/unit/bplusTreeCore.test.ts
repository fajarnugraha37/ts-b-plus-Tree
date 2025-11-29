import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";
import { VALUE_SIZE_BYTES, PageType } from "../../src/constants.ts";
import { deserializeInternal, deserializeLeaf } from "../../src/tree/pages.ts";
import { utf8ValueSerializer, jsonValueSerializer } from "../../src/utils/codec.ts";
import type { DiagnosticsSnapshot } from "../../src/diagnostics.ts";

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

    const valueStrings: string[] = [];
    await tree.set(4, utf8ValueSerializer.serialize("ok"));
    for await (const item of tree.values(4, 4, utf8ValueSerializer)) {
      valueStrings.push(item);
    }
    expect(valueStrings).toEqual(["ok"]);

    const keys: number[] = [];
    for await (const key of tree.keys(1, 4)) {
      keys.push(Number(key.readBigUInt64BE()));
    }
    expect(keys).toContain(1);
    expect(keys).toContain(4);

    await tree.set(5, jsonValueSerializer.serialize({ foo: "bar" }));
    const jsonValues = [];
    for await (const item of tree.values(5, 5, jsonValueSerializer)) {
      jsonValues.push(item);
    }
    expect(jsonValues).toEqual([{ foo: "bar" }]);

    expect(await tree.delete(2)).toBeTrue();
    expect(await tree.get(2)).toBeNull();
    expect(tree.meta.keyCount).toBe(4n);
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

test("internal structure maintains utilization after deletes", async () => {
  await withTree(async (tree) => {
    let inserted = 0;
    while (tree.meta.treeDepth < 2) {
      for (let i = inserted; i < inserted + 256; i += 1) {
        await tree.set(i, bufferFromNumber(i));
      }
      inserted += 256;
      if (inserted > 4096) {
        throw new Error("failed to create multi-level tree");
      }
    }
    const total = inserted;

    const inspectLeaves = async () => {
      await tree.bufferPool.flushAll();
      const leaves: Array<{ page: number; keys: number[] }> = [];
      const pageCount = await tree.pageManager.fileManager.pageCount();
      for (let page = 2; page < pageCount; page += 1) {
        const buffer = await tree.pageManager.readPage(page);
        if (buffer.readUInt8(0) !== PageType.Leaf) {
          continue;
        }
        const leaf = deserializeLeaf(buffer);
        if (leaf.cells.length === 0) {
          continue;
        }
        leaves.push({ page, keys: leaf.cells.map((c) => Number(c.key)) });
      }
      return leaves;
    };

    const before = await inspectLeaves();
    expect(before.length).toBeGreaterThan(0);
    expect(before.every((leaf) => leaf.keys.length > 0)).toBeTrue();

    for (let i = 0; i < total; i += 3) {
      await tree.delete(i);
    }

    const after = await inspectLeaves();
    expect(after.length).toBeGreaterThan(0);
    expect(after.every((leaf) => leaf.keys.length >= 5)).toBeTrue();
    expect(await tree.consistencyCheck()).toBeTrue();
  });
});

test("multi-level structure survives reopen with sorted separators", async () => {
  await withTreePath(async (filePath) => {
    const builder = await BPlusTree.open({ filePath });
    const total = 1024;
    for (let i = 0; i < total; i += 1) {
      await builder.set(i, bufferFromNumber(i));
    }
    expect(builder.meta.treeDepth).toBeGreaterThanOrEqual(2);
    await builder.close();

    const reopened = await BPlusTree.open({ filePath });

    const validate = async (
      pageNumber: number,
      depth: number,
    ): Promise<{ min: bigint; max: bigint }> => {
      const buffer = await reopened.pageManager.readPage(pageNumber);
      const type = buffer.readUInt8(0);
      if (depth === 1) {
        expect(type).toBe(PageType.Leaf);
        const leaf = deserializeLeaf(buffer);
        expect(leaf.cells.length).toBeGreaterThan(0);
        for (let i = 1; i < leaf.cells.length; i += 1) {
          const current = leaf.cells[i]!;
          const prev = leaf.cells[i - 1]!;
          expect(current.key).toBeGreaterThanOrEqual(prev.key);
        }
        const first = leaf.cells[0];
        const last = leaf.cells[leaf.cells.length - 1];
        if (!first || !last) {
          throw new Error("Leaf missing boundary keys");
        }
        return {
          min: first.key,
          max: last.key,
        };
      }

      expect(type).toBe(PageType.Internal);
      const node = deserializeInternal(buffer);
      expect(node.cells.length).toBeGreaterThan(0);
      for (let i = 1; i < node.cells.length; i += 1) {
        const current = node.cells[i]!;
        const prev = node.cells[i - 1]!;
        expect(current.key).toBeGreaterThan(prev.key);
      }

      const ranges: Array<{ min: bigint; max: bigint }> = [];
      let childRange = await validate(node.leftChild, depth - 1);
      ranges.push(childRange);
      for (const cell of node.cells) {
        const nextRange = await validate(cell.child, depth - 1);
        expect(childRange.max).toBeLessThanOrEqual(cell.key);
        ranges.push(nextRange);
        childRange = nextRange;
      }

      const firstRange = ranges[0];
      const lastRange = ranges[ranges.length - 1];
      if (!firstRange || !lastRange) {
        throw new Error("Invalid range aggregation");
      }
      return {
        min: firstRange.min,
        max: lastRange.max,
      };
    };

    const range = await validate(reopened.meta.rootPage, reopened.meta.treeDepth);
    expect(range.min).toBe(0n);
    expect(range.max).toBe(BigInt(total - 1));
    await reopened.close();
  });
});

test("internal node redistribution keeps separators consistent", async () => {
  await withTree(async (tree) => {
    const total = 900;
    for (let i = 0; i < total; i += 1) {
      await tree.set(i, bufferFromNumber(i));
    }
    if (tree.meta.treeDepth < 2) {
      throw new Error("expected multi-level tree for redistribution test");
    }

    const scanInternalNodes = async () => {
      await tree.bufferPool.flushAll();
      const result: Array<{ page: number; keys: number[] }> = [];
      const stack: Array<{ page: number; depth: number }> = [
        { page: tree.meta.rootPage, depth: tree.meta.treeDepth },
      ];
      while (stack.length) {
        const current = stack.pop()!;
        if (current.depth === 1) {
          continue;
        }
        const buffer = await tree.pageManager.readPage(current.page);
        if (buffer.readUInt8(0) !== PageType.Internal) {
          continue;
        }
        const node = deserializeInternal(buffer);
        expect(node.cells.length).toBeGreaterThanOrEqual(1);
        for (let i = 1; i < node.cells.length; i += 1) {
          expect(node.cells[i]!.key).toBeGreaterThan(node.cells[i - 1]!.key);
        }
        result.push({
          page: current.page,
          keys: node.cells.map((c) => Number(c!.key)),
        });
        stack.push({ page: node.leftChild, depth: current.depth - 1 });
        for (const cell of node.cells) {
          stack.push({ page: cell.child, depth: current.depth - 1 });
        }
      }
      return result;
    };

    const before = await scanInternalNodes();
    expect(before.length).toBeGreaterThan(0);

    for (let i = 0; i < total; i += 2) {
      await tree.delete(i);
    }

    const after = await scanInternalNodes();
    expect(after.length).toBeGreaterThan(0);
    for (const node of after) {
      expect(node.keys.length).toBeGreaterThan(0);
    }
    expect(await tree.consistencyCheck()).toBeTrue();
  });
});

test("diagnostics sink receives snapshots and alerts", async () => {
  await withTreePath(async (filePath) => {
    const snapshots: DiagnosticsSnapshot[] = [];
    const alerts: string[] = [];
    const diagTree = await BPlusTree.open({
      filePath,
      diagnostics: {
        onSnapshot: (snap) => snapshots.push(snap),
        onAlert: (msg) => alerts.push(msg),
      },
      limits: { rssBytes: 1 }, // force alert
    });
    await diagTree.set(1, bufferFromNumber(1));
    await diagTree.delete(1);
    await diagTree.close();
    expect(snapshots.length).toBeGreaterThan(0);
    expect(alerts.length).toBeGreaterThanOrEqual(1);
  });
});

test("concurrent readers and writers", async () => {
  await withTree(async (tree) => {
    const writer = async (seed: number) => {
      for (let i = 0; i < 50; i += 1) {
        await tree.set(seed * 100 + i, bufferFromNumber(seed));
      }
    };
    const reader = async () => {
      for (let i = 0; i < 50; i += 1) {
        await tree.get(i);
      }
    };
    await Promise.all([
      writer(1),
      writer(2),
      writer(3),
      reader(),
      reader(),
    ]);
    expect(tree.meta.keyCount).toBeGreaterThan(0n);
  });
});
