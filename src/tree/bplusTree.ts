import {
  BUFFER_POOL_PAGES,
  MAX_INTERNAL_KEYS,
  MAX_LEAF_KEYS,
  MIN_INTERNAL_KEYS,
  MIN_LEAF_KEYS,
  PageType,
} from "../constants.ts";
import { BufferPool } from "../storage/bufferPool.ts";
import { PageManager } from "../storage/pageManager.ts";
import { WriteAheadLog } from "../storage/wal.ts";
import {
  deserializeInternal,
  deserializeLeaf,
  serializeInternal,
  serializeLeaf,
} from "./pages.ts";
import {
  bufferToBigInt,
  normalizeKeyInput,
  normalizeValueInput,
} from "../utils/codec.ts";
import type { MetaPage } from "../storage/pageManager.ts";
import type { InternalPage, LeafPage } from "./pages.ts";
import type { KeyInput } from "../utils/codec.ts";

interface LoadedPage<T> {
  pageNumber: number;
  buffer: Buffer;
  page: T;
}

interface BPlusTreeOptions {
  filePath: string;
  walPath?: string;
  bufferPages?: number;
}

interface InternalPathEntry extends LoadedPage<InternalPage> {
  dirty: boolean;
  childIndex: number;
}

export class BPlusTree {
  readonly pageManager: PageManager;
  readonly bufferPool: BufferPool;
  readonly wal: WriteAheadLog;
  meta: MetaPage;

  private constructor(
    pageManager: PageManager,
    bufferPool: BufferPool,
    wal: WriteAheadLog,
    meta: MetaPage,
  ) {
    this.pageManager = pageManager;
    this.bufferPool = bufferPool;
    this.wal = wal;
    this.meta = meta;
  }

  static async open(options: BPlusTreeOptions): Promise<BPlusTree> {
    const pageManager = await PageManager.initialize(options.filePath);
    const wal = new WriteAheadLog(options.walPath ?? `${options.filePath}.wal`);
    await wal.open();
    const bufferPool = new BufferPool(pageManager, {
      capacity: options.bufferPages ?? BUFFER_POOL_PAGES,
      wal,
    });
    const meta = await pageManager.readMeta();
    return new BPlusTree(pageManager, bufferPool, wal, meta);
  }

  async close(): Promise<void> {
    await this.bufferPool.flushAll();
    await this.wal.close();
    await this.pageManager.fileManager.sync();
    await this.pageManager.fileManager.close();
  }

  async get(keyInput: KeyInput): Promise<Buffer | null> {
    const key = bufferToBigInt(normalizeKeyInput(keyInput));
    const { leaf } = await this.#traverseToLeaf(key);
    try {
      const index = leaf.page.cells.findIndex((cell) => cell.key === key);
      if (index < 0) {
        return null;
      }
      const cell = leaf.page.cells[index];
      return cell ? Buffer.from(cell.value) : null;
    } finally {
      this.#releaseLeaf(leaf, false);
    }
  }

  async set(keyInput: KeyInput, value: Buffer): Promise<void> {
    const keyBuffer = normalizeKeyInput(keyInput);
    const key = bufferToBigInt(keyBuffer);
    const normalizedValue = normalizeValueInput(value);

    const { path, leaf } = await this.#traverseToLeaf(key, true);

    try {
      const existingIndex = leaf.page.cells.findIndex((cell) => cell.key === key);
      if (existingIndex >= 0) {
        leaf.page.cells[existingIndex] = { key, value: normalizedValue };
        this.#releaseLeaf(leaf, true);
        return;
      }

      const insertIndex = this.#insertIntoLeaf(leaf.page, key, normalizedValue);
      if (insertIndex === 0) {
        const parent = path[path.length - 1];
        if (parent) {
          this.#updateParentKeyForLeaf(parent, parent.childIndex, leaf.page);
        }
      }
      while (leaf.page.cells.length > MAX_LEAF_KEYS) {
        const split = await this.#splitLeaf(leaf);
        await this.#propagateSplit(path, split.key, split.pageNumber);
      }
      this.#releaseLeaf(leaf, true);
      await this.#mutateMeta((meta) => {
        meta.keyCount += 1n;
      });
    } finally {
      for (const entry of path) {
        this.#releaseInternal(entry, entry.dirty);
      }
    }
  }

  async delete(keyInput: KeyInput): Promise<boolean> {
    const key = bufferToBigInt(normalizeKeyInput(keyInput));
    const { path, leaf } = await this.#traverseToLeaf(key, true);
    let deleted = false;
    let leafDirty = false;
    let currentLeaf: LoadedPage<LeafPage> | null = leaf;
    try {
      const idx = leaf.page.cells.findIndex((cell) => cell.key === key);
      if (idx >= 0) {
        leaf.page.cells.splice(idx, 1);
        deleted = true;
        leafDirty = true;
        const parent = path[path.length - 1];
        if (idx === 0 && leaf.page.cells.length > 0 && parent) {
          this.#updateParentKeyForLeaf(parent, parent.childIndex, leaf.page);
        }
        await this.#mutateMeta((meta) => {
          meta.keyCount -= 1n;
        });
        const rebalanced = await this.#rebalanceLeafAfterDelete(leaf, path, leafDirty);
        currentLeaf = rebalanced.leaf;
        leafDirty = rebalanced.dirty;
        await this.#rebalanceInternalPath(path);
      }
      if (currentLeaf) {
        this.#releaseLeaf(currentLeaf, leafDirty);
      }
      return deleted;
    } finally {
      for (const entry of path) {
        this.#releaseInternal(entry, entry.dirty);
      }
    }
  }

  async *range(startInput: KeyInput, endInput: KeyInput): AsyncGenerator<{
    key: Buffer;
    value: Buffer;
  }> {
    const startKey = bufferToBigInt(normalizeKeyInput(startInput));
    const endKey = bufferToBigInt(normalizeKeyInput(endInput));
    if (endKey < startKey) {
      return;
    }

    let { leaf } = await this.#traverseToLeaf(startKey);
    try {
      while (true) {
        for (const cell of leaf.page.cells) {
          if (cell.key < startKey) {
            continue;
          }
          if (cell.key > endKey) {
            return;
          }
          yield {
            key: Buffer.from(normalizeKeyInput(cell.key)),
            value: Buffer.from(cell.value),
          };
        }
        if (!leaf.page.rightSibling) {
          return;
        }
        const nextLeaf = await this.#loadLeaf(leaf.page.rightSibling);
        this.#releaseLeaf(leaf, false);
        leaf = nextLeaf;
      }
    } finally {
      this.#releaseLeaf(leaf, false);
    }
  }

  async defragment(): Promise<void> {
    await this.bufferPool.flushAll();
  }

  async vacuum(): Promise<void> {
    // Placeholder for page compaction strategy
  }

  async consistencyCheck(): Promise<boolean> {
    const visited = new Set<number>();
    await this.#validateNode(this.meta.rootPage, this.meta.treeDepth, visited);
    return true;
  }

  async #validateNode(
    pageNumber: number,
    depth: number,
    visited: Set<number>,
  ): Promise<void> {
    if (visited.has(pageNumber)) {
      throw new Error(`Cycle detected at page ${pageNumber}`);
    }
    visited.add(pageNumber);
    if (depth === 1) {
      const leaf = await this.#loadLeaf(pageNumber);
      this.#releaseLeaf(leaf, false);
      return;
    }

    const internal = await this.#loadInternal(pageNumber);
    for (const child of [internal.page.leftChild, ...internal.page.cells.map((c) => c.child)]) {
      await this.#validateNode(child, depth - 1, visited);
    }
    this.#releaseInternal(internal, false);
  }

  async #rebalanceLeafAfterDelete(
    leaf: LoadedPage<LeafPage>,
    path: InternalPathEntry[],
    dirty: boolean,
  ): Promise<{ leaf: LoadedPage<LeafPage>; dirty: boolean }> {
    if (this.meta.treeDepth === 1) {
      return { leaf, dirty };
    }
    const parent = path[path.length - 1];
    if (!parent) {
      return { leaf, dirty };
    }
    const slot = parent.childIndex;
    const minKeys = Math.max(1, MIN_LEAF_KEYS);
    if (leaf.page.cells.length >= minKeys) {
      this.#updateParentKeyForLeaf(parent, slot, leaf.page);
      return { leaf, dirty };
    }

    const leftSlot = slot - 1;
    const leftPageNumber = this.#childPageNumber(parent.page, leftSlot);
    if (leftPageNumber !== null) {
      const leftLeaf = await this.#loadLeaf(leftPageNumber);
      if (leftLeaf.page.cells.length > MIN_LEAF_KEYS) {
        const borrowed = leftLeaf.page.cells.pop();
        if (borrowed) {
          leaf.page.cells.unshift(borrowed);
          this.#updateParentKeyForLeaf(parent, slot, leaf.page);
          this.#releaseLeaf(leftLeaf, true);
          return { leaf, dirty: true };
        }
      }
      this.#releaseLeaf(leftLeaf, false);
    }

    const rightSlot = slot + 1;
    const rightPageNumber = this.#childPageNumber(parent.page, rightSlot);
    if (rightPageNumber !== null) {
      const rightLeaf = await this.#loadLeaf(rightPageNumber);
      if (rightLeaf.page.cells.length > MIN_LEAF_KEYS) {
        const borrowed = rightLeaf.page.cells.shift();
        if (borrowed) {
          leaf.page.cells.push(borrowed);
          this.#updateParentKeyForLeaf(parent, rightSlot, rightLeaf.page);
          this.#updateParentKeyForLeaf(parent, slot, leaf.page);
          this.#releaseLeaf(rightLeaf, true);
          return { leaf, dirty: true };
        }
      }
      this.#releaseLeaf(rightLeaf, false);
    }

    if (leftPageNumber !== null) {
      const leftLeaf = await this.#loadLeaf(leftPageNumber);
      leftLeaf.page.cells.push(...leaf.page.cells);
      leftLeaf.page.rightSibling = leaf.page.rightSibling;
      this.bufferPool.unpin(leaf.pageNumber, false);
      this.bufferPool.dropPage(leaf.pageNumber);
      await this.pageManager.freePage(leaf.pageNumber);
      this.#removeParentCell(parent, slot);
      parent.childIndex = leftSlot;
      this.#updateParentKeyForLeaf(parent, leftSlot, leftLeaf.page);
      return { leaf: leftLeaf, dirty: true };
    }

    if (rightPageNumber !== null) {
      const rightLeaf = await this.#loadLeaf(rightPageNumber);
      leaf.page.cells.push(...rightLeaf.page.cells);
      leaf.page.rightSibling = rightLeaf.page.rightSibling;
      this.bufferPool.unpin(rightLeaf.pageNumber, false);
      this.bufferPool.dropPage(rightLeaf.pageNumber);
      await this.pageManager.freePage(rightLeaf.pageNumber);
      this.#removeParentCell(parent, rightSlot);
      this.#updateParentKeyForLeaf(parent, slot, leaf.page);
      return { leaf, dirty: true };
    }

    return { leaf, dirty };
  }

  async #rebalanceInternalPath(path: InternalPathEntry[]): Promise<void> {
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
        const shrunk = await this.#shrinkRootIfNeeded(path, entry);
        if (shrunk) {
          return;
        }
        continue;
      }
      await this.#rebalanceInternalEntry(path, i);
    }
  }

  async #rebalanceInternalEntry(
    path: InternalPathEntry[],
    index: number,
  ): Promise<void> {
    if (index === 0) {
      return;
    }
    const entry = path[index];
    const parent = path[index - 1];
    if (!entry || !parent) {
      return;
    }
    const slot = parent.childIndex;
    if (await this.#borrowFromLeftInternal(entry, parent, slot)) {
      return;
    }
    if (await this.#borrowFromRightInternal(entry, parent, slot)) {
      return;
    }
    await this.#mergeInternal(path, index, parent, slot);
  }

  async #borrowFromLeftInternal(
    entry: InternalPathEntry,
    parent: InternalPathEntry,
    slot: number,
  ): Promise<boolean> {
    if (slot < 0) {
      return false;
    }
    const leftSlot = slot - 1;
    const leftPageNumber = this.#childPageNumber(parent.page, leftSlot);
    if (leftPageNumber === null) {
      return false;
    }
    const left = await this.#loadInternal(leftPageNumber);
    if (left.page.cells.length <= MIN_INTERNAL_KEYS) {
      this.#releaseInternal(left, false);
      return false;
    }
    const separator = parent.page.cells[slot];
    if (!separator) {
      this.#releaseInternal(left, false);
      return false;
    }
    const borrowed = left.page.cells.pop();
    if (!borrowed) {
      this.#releaseInternal(left, false);
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
    this.#releaseInternal(left, true);
    return true;
  }

  async #borrowFromRightInternal(
    entry: InternalPathEntry,
    parent: InternalPathEntry,
    slot: number,
  ): Promise<boolean> {
    const rightSlot = slot + 1;
    if (rightSlot > parent.page.cells.length - 1) {
      return false;
    }
    const rightPageNumber = this.#childPageNumber(parent.page, rightSlot);
    if (rightPageNumber === null) {
      return false;
    }
    const right = await this.#loadInternal(rightPageNumber);
    if (right.page.cells.length <= MIN_INTERNAL_KEYS) {
      this.#releaseInternal(right, false);
      return false;
    }
    const separatorIndex = slot + 1;
    const separator = parent.page.cells[separatorIndex];
    if (!separator) {
      this.#releaseInternal(right, false);
      return false;
    }
    const shifted = right.page.cells.shift();
    if (!shifted) {
      this.#releaseInternal(right, false);
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
    this.#releaseInternal(right, true);
    return true;
  }

  async #mergeInternal(
    path: InternalPathEntry[],
    index: number,
    parent: InternalPathEntry,
    slot: number,
  ): Promise<void> {
    const entry = path[index];
    const parentEntry = parent;
    if (!entry) {
      return;
    }
    if (slot >= 0) {
      const leftPageNumber = this.#childPageNumber(parent.page, slot - 1);
      if (leftPageNumber !== null) {
        await this.#mergeWithLeftInternal(path, index, parent, slot, leftPageNumber);
        return;
      }
    }
    const rightPageNumber = this.#childPageNumber(parent.page, slot + 1);
    if (rightPageNumber === null) {
      throw new Error("Internal node merge failed: no siblings available");
    }
    await this.#mergeWithRightInternal(path, index, parent, slot, rightPageNumber);
  }

  async #mergeWithLeftInternal(
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
    const left = await this.#loadInternal(leftPageNumber);
    const parentKeyIndex = slot;
    const parentKey = parent.page.cells[parentKeyIndex];
    if (!parentKey) {
      this.#releaseInternal(left, false);
      return;
    }
    left.page.cells.push({
      key: parentKey.key,
      child: entry.page.leftChild,
    });
    for (const cell of entry.page.cells) {
      left.page.cells.push(cell);
    }
    left.page.rightSibling = entry.page.rightSibling;
    this.#removeParentCell(parent, parentKeyIndex);
    parent.childIndex = slot - 1;
    this.bufferPool.unpin(entry.pageNumber, false);
    this.bufferPool.dropPage(entry.pageNumber);
    await this.pageManager.freePage(entry.pageNumber);
    entry.pageNumber = left.pageNumber;
    entry.buffer = left.buffer;
    entry.page = left.page;
    entry.dirty = true;
  }

  async #mergeWithRightInternal(
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
    const right = await this.#loadInternal(rightPageNumber);
    const parentKeyIndex = slot + 1;
    const parentKey = parent.page.cells[parentKeyIndex];
    if (!parentKey) {
      this.#releaseInternal(right, false);
      return;
    }
    entry.page.cells.push({
      key: parentKey.key,
      child: right.page.leftChild,
    });
    for (const cell of right.page.cells) {
      entry.page.cells.push(cell);
    }
    entry.page.rightSibling = right.page.rightSibling;
    entry.dirty = true;
    this.#removeParentCell(parent, parentKeyIndex);
    this.bufferPool.unpin(right.pageNumber, false);
    this.bufferPool.dropPage(right.pageNumber);
    await this.pageManager.freePage(right.pageNumber);
  }

  async #shrinkRootIfNeeded(
    path: InternalPathEntry[],
    rootEntry: InternalPathEntry,
  ): Promise<boolean> {
    if (this.meta.treeDepth <= 1) {
      return false;
    }
    if (rootEntry.page.cells.length > 0) {
      return false;
    }
    const newRootPageNumber = rootEntry.page.leftChild;
    if (!newRootPageNumber) {
      return false;
    }
    await this.#mutateMeta((meta) => {
      meta.rootPage = newRootPageNumber;
      meta.treeDepth -= 1;
    });
    this.bufferPool.unpin(rootEntry.pageNumber, false);
    this.bufferPool.dropPage(rootEntry.pageNumber);
    await this.pageManager.freePage(rootEntry.pageNumber);
    if (this.meta.treeDepth === 1) {
      path.length = 0;
      return true;
    }
    const newRoot = await this.#loadInternal(newRootPageNumber);
    rootEntry.pageNumber = newRoot.pageNumber;
    rootEntry.buffer = newRoot.buffer;
    rootEntry.page = newRoot.page;
    rootEntry.dirty = false;
    return false;
  }
  async #splitLeaf(
    leaf: LoadedPage<LeafPage>,
  ): Promise<{ key: bigint; pageNumber: number }> {
    const midpoint = Math.ceil(leaf.page.cells.length / 2);
    const siblingCells = leaf.page.cells.splice(midpoint);
    const siblingPageNumber = await this.pageManager.allocatePage();
    const siblingBuffer = await this.bufferPool.getPage(siblingPageNumber);
    const siblingPage: LeafPage = {
      type: PageType.Leaf,
      keyCount: siblingCells.length,
      rightSibling: leaf.page.rightSibling,
      cells: siblingCells,
    };
    leaf.page.rightSibling = siblingPageNumber;
    serializeLeaf(siblingPage, siblingBuffer);
    this.bufferPool.unpin(siblingPageNumber, true);
    const firstSiblingCell = siblingPage.cells[0];
    if (!firstSiblingCell) {
      throw new Error("Leaf split produced empty sibling");
    }
    return { key: firstSiblingCell.key, pageNumber: siblingPageNumber };
  }

  async #propagateSplit(
    path: InternalPathEntry[],
    key: bigint,
    rightPageNumber: number,
  ): Promise<void> {
    let promoteKey = key;
    let promoteChild = rightPageNumber;

    for (let i = path.length - 1; i >= 0; i -= 1) {
      const entry = path[i];
      if (!entry) {
        continue;
      }
      this.#insertIntoInternal(entry.page, promoteKey, promoteChild);
      entry.dirty = true;
      if (entry.page.cells.length <= MAX_INTERNAL_KEYS) {
        return;
      }

      const sibling = await this.#splitInternal(entry);
      promoteKey = sibling.key;
      promoteChild = sibling.pageNumber;
    }

    await this.#createNewRoot(promoteKey, promoteChild);
  }

  async #createNewRoot(key: bigint, rightChild: number): Promise<void> {
    const newRootPageNumber = await this.pageManager.allocatePage();
    const buffer = await this.bufferPool.getPage(newRootPageNumber);
    const rootPage: InternalPage = {
      type: PageType.Internal,
      keyCount: 1,
      rightSibling: 0,
      leftChild: this.meta.rootPage,
      cells: [{ key, child: rightChild }],
    };
    serializeInternal(rootPage, buffer);
    this.bufferPool.unpin(newRootPageNumber, true);
    await this.#mutateMeta((meta) => {
      meta.rootPage = newRootPageNumber;
      meta.treeDepth += 1;
    });
  }

  async #splitInternal(
    entry: InternalPathEntry,
  ): Promise<{ key: bigint; pageNumber: number }> {
    const midpoint = Math.ceil(entry.page.cells.length / 2) - 1;
    const promote = entry.page.cells[midpoint];
    if (!promote) {
      throw new Error("Internal page split failed: no promote key");
    }
    const rightCells = entry.page.cells.splice(midpoint + 1);
    const rightPageNumber = await this.pageManager.allocatePage();
    const buffer = await this.bufferPool.getPage(rightPageNumber);
    const rightPage: InternalPage = {
      type: PageType.Internal,
      keyCount: rightCells.length,
      rightSibling: entry.page.rightSibling,
      leftChild: promote.child,
      cells: rightCells,
    };
    entry.page.rightSibling = rightPageNumber;
    serializeInternal(rightPage, buffer);
    this.bufferPool.unpin(rightPageNumber, true);
    entry.page.cells.splice(midpoint, 1);
    entry.dirty = true;
    return { key: promote.key, pageNumber: rightPageNumber };
  }

  #insertIntoLeaf(page: LeafPage, key: bigint, value: Buffer): number {
    let idx = 0;
    while (idx < page.cells.length) {
      const cell = page.cells[idx];
      if (!cell || cell.key >= key) {
        break;
      }
      idx += 1;
    }
    page.cells.splice(idx, 0, { key, value });
    return idx;
  }

  #insertIntoInternal(page: InternalPage, key: bigint, child: number): void {
    let idx = 0;
    while (idx < page.cells.length) {
      const cell = page.cells[idx];
      if (!cell || cell.key >= key) {
        break;
      }
      idx += 1;
    }
    page.cells.splice(idx, 0, { key, child });
  }

  #childPageNumber(page: InternalPage, slot: number): number | null {
    if (slot < -1 || slot > page.cells.length - 1) {
      return null;
    }
    if (slot === -1) {
      return page.leftChild;
    }
    const cell = page.cells[slot];
    return cell ? cell.child : null;
  }

  #updateParentKeyForLeaf(
    parent: InternalPathEntry | undefined,
    slot: number,
    leaf: LeafPage,
  ): void {
    if (!parent || slot < 0) {
      return;
    }
    const first = leaf.cells[0];
    if (!first) {
      return;
    }
    const parentCell = parent.page.cells[slot];
    if (!parentCell) {
      return;
    }
    parentCell.key = first.key;
    parent.dirty = true;
  }

  #removeParentCell(parent: InternalPathEntry, index: number): void {
    if (index < 0 || index >= parent.page.cells.length) {
      return;
    }
    parent.page.cells.splice(index, 1);
    parent.dirty = true;
  }

  async #traverseToLeaf(
    key: bigint,
    keepPath = false,
  ): Promise<{ leaf: LoadedPage<LeafPage>; path: InternalPathEntry[] }> {
    const path: InternalPathEntry[] = [];
    let pageNumber = this.meta.rootPage;
    for (let depth = this.meta.treeDepth; depth > 1; depth -= 1) {
      const internal = await this.#loadInternal(pageNumber);
      const { child, slot } = this.#pickChildSlot(internal.page, key);
      const entry: InternalPathEntry = { ...internal, dirty: false, childIndex: slot };
      path.push(entry);
      if (!keepPath) {
        this.#releaseInternal(entry, false);
        path.pop();
      }
      pageNumber = child;
    }
    const leaf = await this.#loadLeaf(pageNumber);
    return { leaf, path };
  }

  #pickChildSlot(page: InternalPage, key: bigint): { child: number; slot: number } {
    let child = page.leftChild;
    if (page.cells.length === 0) {
      return { child, slot: -1 };
    }
    for (let i = 0; i < page.cells.length; i++) {
      const cell = page.cells[i];
      if (!cell) {
        continue;
      }
      if (key < cell.key) {
        return { child, slot: i - 1 };
      }
      child = cell.child;
    }
    return { child, slot: page.cells.length - 1 };
  }

  async #mutateMeta(mutator: (meta: MetaPage) => void): Promise<void> {
    const latest = await this.pageManager.readMeta();
    mutator(latest);
    await this.pageManager.writeMeta(latest);
    this.meta = latest;
  }

  async #loadLeaf(pageNumber: number): Promise<LoadedPage<LeafPage>> {
    const buffer = await this.bufferPool.getPage(pageNumber);
    const page = deserializeLeaf(buffer);
    return { pageNumber, buffer, page };
  }

  async #loadInternal(pageNumber: number): Promise<LoadedPage<InternalPage>> {
    const buffer = await this.bufferPool.getPage(pageNumber);
    const page = deserializeInternal(buffer);
    return { pageNumber, buffer, page };
  }

  #releaseLeaf(page: LoadedPage<LeafPage>, dirty: boolean): void {
    if (dirty) {
      serializeLeaf(page.page, page.buffer);
    }
    this.bufferPool.unpin(page.pageNumber, dirty);
  }

  #releaseInternal(page: LoadedPage<InternalPage>, dirty: boolean): void {
    if (dirty) {
      serializeInternal(page.page, page.buffer);
    }
    this.bufferPool.unpin(page.pageNumber, dirty);
  }
}
