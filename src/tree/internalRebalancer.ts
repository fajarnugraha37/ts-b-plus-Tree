import { MIN_INTERNAL_KEYS } from "../constants.ts";
import type { BufferPool } from "../storage/bufferPool.ts";
import type { PageManager } from "../storage/pageManager.ts";
import type { MetaPage } from "../storage/pageManager.ts";
import type { InternalPage } from "./pages.ts";
import type { InternalPathEntry, LoadedPage } from "./types.ts";

export interface InternalRebalanceContext {
  bufferPool: BufferPool;
  pageManager: PageManager;
  getMeta(): MetaPage;
  mutateMeta(mutator: (meta: MetaPage) => void): Promise<void>;
  childPageNumber(page: InternalPage, slot: number): number | null;
  removeParentCell(parent: InternalPathEntry, index: number): void;
  loadInternal(pageNumber: number): Promise<LoadedPage<InternalPage>>;
  releaseInternal(page: LoadedPage<InternalPage>, dirty: boolean): void;
  dropPage(pageNumber: number): void;
}

export async function rebalanceInternalPath(
  context: InternalRebalanceContext,
  path: InternalPathEntry[],
): Promise<void> {
  for (let i = path.length - 1; i >= 0; i -= 1) {
    const entry = path[i];
    if (!entry) {
      continue;
    }
    const isRoot = i === 0;
    const minKeys = isRoot ? 1 : MIN_INTERNAL_KEYS;
    if (entry.page.cells.length >= minKeys) {
      continue;
    }
    if (isRoot) {
      const shrunk = await shrinkRootIfNeeded(context, path, entry);
      if (shrunk) {
        return;
      }
      continue;
    }
    await rebalanceInternalEntry(context, path, i);
  }
}

async function rebalanceInternalEntry(
  context: InternalRebalanceContext,
  path: InternalPathEntry[],
  index: number,
): Promise<void> {
  const entry = path[index];
  const parent = path[index - 1];
  if (!entry || !parent) {
    return;
  }
  const slot = parent.childIndex;
  if (await borrowFromLeft(context, entry, parent, slot)) {
    return;
  }
  if (await borrowFromRight(context, entry, parent, slot)) {
    return;
  }
  await mergeInternal(context, path, index, parent, slot);
}

async function borrowFromLeft(
  context: InternalRebalanceContext,
  entry: InternalPathEntry,
  parent: InternalPathEntry,
  slot: number,
): Promise<boolean> {
  if (slot < 0) {
    return false;
  }
  const leftSlot = slot - 1;
  const leftPageNumber = context.childPageNumber(parent.page, leftSlot);
  if (leftPageNumber === null) {
    return false;
  }
  const left = await context.loadInternal(leftPageNumber);
  if (left.page.cells.length <= MIN_INTERNAL_KEYS) {
    context.releaseInternal(left, false);
    return false;
  }
  const separator = parent.page.cells[slot];
  const borrowed = left.page.cells.pop();
  if (!separator || !borrowed) {
    context.releaseInternal(left, false);
    return false;
  }
  entry.page.cells.unshift({
    key: separator.key,
    child: entry.page.leftChild,
  });
  entry.page.leftChild = borrowed.child;
  separator.key = borrowed.key;
  entry.dirty = true;
  parent.dirty = true;
  context.releaseInternal(left, true);
  return true;
}

async function borrowFromRight(
  context: InternalRebalanceContext,
  entry: InternalPathEntry,
  parent: InternalPathEntry,
  slot: number,
): Promise<boolean> {
  const rightSlot = slot + 1;
  if (rightSlot > parent.page.cells.length - 1) {
    return false;
  }
  const rightPageNumber = context.childPageNumber(parent.page, rightSlot);
  if (rightPageNumber === null) {
    return false;
  }
  const right = await context.loadInternal(rightPageNumber);
  if (right.page.cells.length <= MIN_INTERNAL_KEYS) {
    context.releaseInternal(right, false);
    return false;
  }
  const separatorIndex = slot + 1;
  const separator = parent.page.cells[separatorIndex];
  const shifted = right.page.cells.shift();
  if (!separator || !shifted) {
    context.releaseInternal(right, false);
    return false;
  }
  const movedChild = right.page.leftChild;
  entry.page.cells.push({
    key: separator.key,
    child: movedChild,
  });
  separator.key = shifted.key;
  right.page.leftChild = shifted.child;
  entry.dirty = true;
  parent.dirty = true;
  context.releaseInternal(right, true);
  return true;
}

async function mergeInternal(
  context: InternalRebalanceContext,
  path: InternalPathEntry[],
  index: number,
  parent: InternalPathEntry,
  slot: number,
): Promise<void> {
  const entry = path[index];
  if (!entry) {
    return;
  }

  if (slot >= 0) {
    const leftPageNumber = context.childPageNumber(parent.page, slot - 1);
    if (leftPageNumber !== null) {
      await mergeWithLeft(context, path, index, parent, slot, leftPageNumber);
      return;
    }
  }

  const rightPageNumber = context.childPageNumber(parent.page, slot + 1);
  if (rightPageNumber === null) {
    throw new Error("Internal merge failed: no siblings available");
  }
  await mergeWithRight(context, path, index, parent, slot, rightPageNumber);
}

async function mergeWithLeft(
  context: InternalRebalanceContext,
  path: InternalPathEntry[],
  index: number,
  parent: InternalPathEntry,
  slot: number,
  leftPageNumber: number,
): Promise<void> {
  const entry = path[index];
  if (!entry) {
    return;
  }
  const left = await context.loadInternal(leftPageNumber);
  const parentKeyIndex = slot;
  const parentKey = parent.page.cells[parentKeyIndex];
  if (!parentKey) {
    context.releaseInternal(left, false);
    return;
  }

  left.page.cells.push({
    key: parentKey.key,
    child: entry.page.leftChild,
  });
  left.page.cells.push(...entry.page.cells);
  left.page.rightSibling = entry.page.rightSibling;
  context.removeParentCell(parent, parentKeyIndex);
  parent.childIndex = slot - 1;

  context.bufferPool.unpin(entry.pageNumber, false);
  context.dropPage(entry.pageNumber);
  await context.pageManager.freePage(entry.pageNumber);

  entry.pageNumber = left.pageNumber;
  entry.buffer = left.buffer;
  entry.page = left.page;
  entry.dirty = true;
  entry.childIndex = slot - 1;
}

async function mergeWithRight(
  context: InternalRebalanceContext,
  path: InternalPathEntry[],
  index: number,
  parent: InternalPathEntry,
  slot: number,
  rightPageNumber: number,
): Promise<void> {
  const entry = path[index];
  if (!entry) {
    return;
  }
  const right = await context.loadInternal(rightPageNumber);
  const parentKeyIndex = slot + 1;
  const parentKey = parent.page.cells[parentKeyIndex];
  if (!parentKey) {
    context.releaseInternal(right, false);
    return;
  }
  entry.page.cells.push({
    key: parentKey.key,
    child: right.page.leftChild,
  });
  entry.page.cells.push(...right.page.cells);
  entry.page.rightSibling = right.page.rightSibling;
  entry.dirty = true;
  context.removeParentCell(parent, parentKeyIndex);
  context.bufferPool.unpin(right.pageNumber, false);
  context.dropPage(right.pageNumber);
  await context.pageManager.freePage(right.pageNumber);
}

async function shrinkRootIfNeeded(
  context: InternalRebalanceContext,
  path: InternalPathEntry[],
  rootEntry: InternalPathEntry,
): Promise<boolean> {
  const meta = context.getMeta();
  if (meta.treeDepth <= 1) {
    return false;
  }
  if (rootEntry.page.cells.length > 0) {
    return false;
  }
  const newRootPageNumber = rootEntry.page.leftChild;
  if (!newRootPageNumber) {
    return false;
  }
  await context.mutateMeta((m) => {
    m.rootPage = newRootPageNumber;
    m.treeDepth -= 1;
  });
  context.bufferPool.unpin(rootEntry.pageNumber, false);
  context.dropPage(rootEntry.pageNumber);
  await context.pageManager.freePage(rootEntry.pageNumber);
  path.length = 0;

  return context.getMeta().treeDepth === 1;
}
