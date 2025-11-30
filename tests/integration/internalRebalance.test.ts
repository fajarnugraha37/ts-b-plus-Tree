
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../../index.ts";
import { MIN_INTERNAL_KEYS, PageType } from "../../src/constants.ts";
import { deserializeInternal } from "../../src/tree/pages.ts";

function largeValue(n: number): Buffer {
  const buffer = Buffer.alloc(512);
  buffer.writeUInt32LE(n, 0);
  return buffer;
}

async function assertInternalOccupancy(tree: BPlusTree): Promise<void> {
  const { rootPage, treeDepth } = tree.meta;
  if (treeDepth <= 1) {
    return;
  }
  const stack = [{ page: rootPage, depth: treeDepth }];
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
    if (current.depth === treeDepth) {
      expect(node.cells.length).toBeGreaterThanOrEqual(1);
    } else {
      expect(node.cells.length).toBeGreaterThanOrEqual(MIN_INTERNAL_KEYS);
    }
    const nextDepth = current.depth - 1;
    stack.push({ page: node.leftChild, depth: nextDepth });
    for (const cell of node.cells) {
      stack.push({ page: cell.child, depth: nextDepth });
    }
  }
}

async function withTree<T>(
  fn: (tree: BPlusTree, filePath: string) => Promise<T>,
): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), "ts-btree-internal-"));
  const filePath = join(dir, "internal.db");
  const tree = await BPlusTree.open({ filePath });
  try {
    return await fn(tree, filePath);
  } finally {
    await tree.close();
    await rm(dir, { recursive: true, force: true });
  }
}

test(
  "internal nodes stay populated through delete-heavy workloads and reopen",
  async () => {
    await withTree(async (tree, filePath) => {
      const total = 3500;
      for (let i = 0; i < total; i += 1) {
        await tree.set(i, largeValue(i));
      }
      expect(tree.meta.treeDepth).toBeGreaterThanOrEqual(3);
      for (let i = 0; i < 1200; i += 1) {
        await tree.delete(i);
      }
      await assertInternalOccupancy(tree);
      await tree.close();

      const reopened = await BPlusTree.open({ filePath });
      try {
        await assertInternalOccupancy(reopened);
        for (let i = total - 1; i >= total - 800; i -= 1) {
          await reopened.delete(i);
        }
        await assertInternalOccupancy(reopened);
      } finally {
        await reopened.close();
      }
    });
  },
  { timeout: 180_000 },
);
