import {
  BUFFER_POOL_PAGES,
  MAX_INTERNAL_KEYS,
  MAX_LEAF_KEYS,
  MIN_LEAF_KEYS,
  PageType,
  MAGIC,
  KEY_SIZE_BYTES,
  PAGE_HEADER_SIZE,
} from "../constants.ts";
import { join, basename } from "path";
import { BufferPool } from "../storage/bufferPool.ts";
import { PageManager } from "../storage/pageManager.ts";
import { OverflowManager } from "../storage/overflowManager.ts";
import { PageLatchManager } from "../storage/latchManager.ts";
import { SegmentedFileManager } from "../storage/segmentedFileManager.ts";
import { BackgroundVacuum, type VacuumOptions } from "../storage/backgroundVacuum.ts";
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
  type ValueSerializer,
} from "../utils/codec.ts";
import type { MetaPage } from "../storage/pageManager.ts";
import type { InternalPage, LeafPage, LeafCell } from "./pages.ts";
import type { KeyInput } from "../utils/codec.ts";
import type { InternalPathEntry, LoadedPage } from "./types.ts";
import type { InternalRebalanceContext } from "./internalRebalancer.ts";
import { rebalanceInternalPath } from "./internalRebalancer.ts";
import type { DiagnosticsSink } from "../diagnostics.ts";
import { AsyncRWLock } from "../utils/locks.ts";

export interface RangeCursor {
  next(): Promise<{ key: Buffer; value: Buffer } | null>;
  close(): Promise<void>;
}

type RangeCursorState = {
  startKey: bigint;
  endKey: bigint;
  leaf: LoadedPage<LeafPage> | null;
  release: (() => void) | null;
  index: number;
  done: boolean;
  firstLeaf: boolean;
};

interface BPlusTreeOptions {
  filePath: string;
  walPath?: string;
  bufferPages?: number;
  walOptions?: {
    groupCommit?: boolean;
    checkpointIntervalOps?: number;
    compressFrames?: boolean;
    checkpointIntervalMs?: number;
  };
  diagnostics?: DiagnosticsSink;
  limits?: {
    rssBytes?: number;
    bufferPages?: number;
  };
  fileOptions?: {
    pageSize?: number;
    readAheadPages?: number;
  };
  io?: {
    pageSize?: number;
    readAheadPages?: number;
    walDirectory?: string;
    segmentPages?: number;
  };
  maintenance?: {
    backgroundVacuum?: boolean;
    vacuumOptions?: VacuumOptions;
  };
}

export class BPlusTree {
  readonly pageManager: PageManager;
  readonly bufferPool: BufferPool;
  readonly wal: WriteAheadLog;
  readonly overflowManager: OverflowManager;
  readonly latchManager = new PageLatchManager();
  #backgroundVacuum?: BackgroundVacuum;
  #vacuumOptions?: VacuumOptions;
  #vacuumEnabled = false;
  meta: MetaPage;
  #checkpointIntervalOps: number;
  #opsSinceCheckpoint = 0;
  #checkpointIntervalMs: number;
  #lastCheckpointTime = Date.now();
  #diagnostics?: DiagnosticsSink;
  #rssLimit: number;
  #bufferPageLimit?: number;
  #rwLock = new AsyncRWLock();

  private constructor(
    pageManager: PageManager,
    bufferPool: BufferPool,
    wal: WriteAheadLog,
    meta: MetaPage,
    walOptions?: BPlusTreeOptions["walOptions"],
    diagnostics?: DiagnosticsSink,
    limits?: BPlusTreeOptions["limits"],
    maintenance?: BPlusTreeOptions["maintenance"],
  ) {
    this.pageManager = pageManager;
    this.bufferPool = bufferPool;
    this.wal = wal;
    this.overflowManager = new OverflowManager(pageManager, bufferPool);
    this.meta = meta;
    this.#checkpointIntervalOps = walOptions?.checkpointIntervalOps ?? 0;
    this.#checkpointIntervalMs = walOptions?.checkpointIntervalMs ?? 0;
    this.#diagnostics = diagnostics;
    this.#rssLimit = limits?.rssBytes ?? 100 * 1024 * 1024;
    this.#bufferPageLimit = limits?.bufferPages;
    this.#vacuumOptions = maintenance?.vacuumOptions ?? {};
    if (maintenance?.backgroundVacuum) {
      this.#vacuumEnabled = true;
    }
  }

  static async open(options: BPlusTreeOptions): Promise<BPlusTree> {
    const ioOptions = options.io ?? {};
    const requestedPageSize = ioOptions.pageSize ?? options.fileOptions?.pageSize;
    if (requestedPageSize !== undefined) {
      if (!Number.isInteger(requestedPageSize) || requestedPageSize < 512 || requestedPageSize % 512 !== 0) {
        throw new Error("pageSize must be a multiple of 512 bytes");
      }
    }
    const readAheadPages = ioOptions.readAheadPages ?? options.fileOptions?.readAheadPages;
    const segmentPages = ioOptions.segmentPages;
    const pageManager = await PageManager.initialize(options.filePath, {
      pageSize: requestedPageSize,
      readAheadPages,
      fileManagerFactory: segmentPages
        ? () =>
            SegmentedFileManager.openOrCreate(options.filePath, {
              pageSize: requestedPageSize,
              readAheadPages,
              segmentPages,
            })
        : undefined,
    });
    const derivedWalPath =
      options.walPath ??
      (ioOptions.walDirectory
        ? join(ioOptions.walDirectory, `${basename(options.filePath)}.wal`)
        : undefined);
    const wal = new WriteAheadLog(
      derivedWalPath ?? `${options.filePath}.wal`,
      pageManager.pageSize,
      { compressFrames: options.walOptions?.compressFrames },
    );
    await wal.open();
    await wal.replay(pageManager);
    const bufferPool = new BufferPool(pageManager, {
      capacity: options.bufferPages ?? BUFFER_POOL_PAGES,
      wal,
      groupCommit: options.walOptions?.groupCommit
        ? { enabled: true, maxBatchPages: 8 }
        : undefined,
    });
    const meta = await pageManager.readMeta();
    const tree = new BPlusTree(
      pageManager,
      bufferPool,
      wal,
      meta,
      options.walOptions,
      options.diagnostics,
      options.limits,
      options.maintenance,
    );
    if (tree.#vacuumEnabled) {
      tree.#startBackgroundVacuum();
    }
    return tree;
  }

  async close(): Promise<void> {
    const release = await this.#rwLock.acquireWrite();
    try {
      await this.#stopBackgroundVacuum();
      await this.bufferPool.flushAll();
      await this.wal.checkpoint(this.pageManager);
      await this.wal.close();
      await this.pageManager.fileManager.sync();
      await this.pageManager.fileManager.close();
      this.latchManager.reset();
      await this.#emitDiagnostics("close");
    } finally {
      release();
    }
  }

  async get(keyInput: KeyInput): Promise<Buffer | null> {
    const key = bufferToBigInt(normalizeKeyInput(keyInput));
    const { leaf, release } = await this.#findLeafForKey(key);
    try {
      const index = leaf.page.cells.findIndex((cell) => cell.key === key);
      if (index < 0) {
        return null;
      }
      return await this.#materializeCellValue(leaf.page.cells[index]!);
    } finally {
      this.#releaseLeaf(leaf, false);
      release();
    }
  }

  async set(keyInput: KeyInput, value: Buffer): Promise<void> {
    const release = await this.#rwLock.acquireWrite();
    try {
      const keyBuffer = normalizeKeyInput(keyInput);
      const key = bufferToBigInt(keyBuffer);
      const normalizedValue = normalizeValueInput(value);

      await this.#insertKeyValue(key, normalizedValue);
      await this.#maybeCheckpoint();
      await this.#emitDiagnostics("set");
    } finally {
      release();
    }
  }

  async delete(keyInput: KeyInput): Promise<boolean> {
    const release = await this.#rwLock.acquireWrite();
    try {
      const key = bufferToBigInt(normalizeKeyInput(keyInput));
      const { path, leaf } = await this.#traverseToLeaf(key, true);
      let deleted = false;
      let leafDirty = false;
      let currentLeaf: LoadedPage<LeafPage> | null = leaf;

      try {
        const idx = leaf.page.cells.findIndex((cell) => cell.key === key);
        if (idx >= 0) {
          const [removed] = leaf.page.cells.splice(idx, 1);
          if (removed) {
            await this.overflowManager.freeChain(removed.overflowPage);
          }
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
          await rebalanceInternalPath(this.#internalContext(), path);
          await this.#maybeCheckpoint();
          await this.#emitDiagnostics("delete");
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
    } finally {
      release();
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

    let { leaf, release } = await this.#findLeafForKey(startKey);
    try {
      while (true) {
        for (const cell of leaf.page.cells) {
          if (cell.key < startKey) {
            continue;
          }
          if (cell.key > endKey) {
            return;
          }
          const value = await this.#materializeCellValue(cell);
          yield {
            key: Buffer.from(normalizeKeyInput(cell.key)),
            value,
          };
        }
        if (!leaf.page.rightSibling) {
          return;
        }
        const nextPage = leaf.page.rightSibling;
        const nextRelease = await this.latchManager.acquireShared(nextPage);
        const nextLeaf = await this.#loadLeaf(nextPage);
        this.#releaseLeaf(leaf, false);
        release();
        leaf = nextLeaf;
        release = nextRelease;
      }
    } finally {
      this.#releaseLeaf(leaf, false);
      release();
    }
  }

  async *keys(startInput: KeyInput, endInput: KeyInput): AsyncGenerator<Buffer> {
    for await (const { key } of this.range(startInput, endInput)) {
      yield key;
    }
  }

  async *values<T = Buffer>(
    startInput: KeyInput,
    endInput: KeyInput,
    serializer?: ValueSerializer<T>,
  ): AsyncGenerator<T> {
    for await (const row of this.range(startInput, endInput)) {
      if (serializer) {
        yield serializer.deserialize(row.value);
      } else {
        yield row.value as unknown as T;
      }
    }
  }

  createRangeCursor(startInput: KeyInput, endInput: KeyInput): RangeCursor {
    const startKey = bufferToBigInt(normalizeKeyInput(startInput));
    const endKey = bufferToBigInt(normalizeKeyInput(endInput));
    if (endKey < startKey) {
      throw new Error("end key must be >= start key");
    }
    const state: RangeCursorState = {
      startKey,
      endKey,
      leaf: null,
      release: null,
      index: 0,
      done: false,
      firstLeaf: true,
    };
    return {
      next: async () => this.#cursorNext(state),
      close: async () => {
        await this.#cursorClose(state);
      },
    };
  }

  async defragment(): Promise<void> {
    const release = await this.#rwLock.acquireWrite();
    try {
      const restartVacuum = this.#vacuumEnabled;
      await this.#stopBackgroundVacuum();
      await this.bufferPool.flushAll();
      await this.wal.checkpoint(this.pageManager);
      const entries = await this.#collectAllEntries();
      this.bufferPool.reset();
      this.latchManager.reset();
      await this.wal.reset();
      const freshMeta: MetaPage = {
        magic: MAGIC,
        pageSize: this.pageManager.pageSize,
        rootPage: 2,
        treeDepth: 1,
        totalPages: 3,
        keyCount: 0n,
        freePageHead: 0,
      };
      await this.pageManager.writeMeta(freshMeta);
      await this.pageManager.resetPage(2, PageType.Leaf);
      await this.pageManager.fileManager.truncatePages(3);
      this.meta = freshMeta;
      for (const entry of entries) {
        await this.#insertKeyValue(
          bufferToBigInt(entry.key),
          normalizeValueInput(entry.value),
        );
      }
      await this.#emitDiagnostics("defragment");
      if (restartVacuum) {
        this.#startBackgroundVacuum();
      }
    } finally {
      release();
    }
  }

  async vacuum(): Promise<void> {
    const release = await this.#rwLock.acquireWrite();
    try {
      await this.bufferPool.flushAll();
      await this.pageManager.vacuumFreePages();
      if (!this.#vacuumEnabled) {
        await this.#getVacuumManager().runOnce();
      }
    } finally {
      release();
    }
  }

  async consistencyCheck(): Promise<boolean> {
    const release = await this.#rwLock.acquireRead();
    try {
      const visited = new Set<number>();
      await this.#validateNode(this.meta.rootPage, this.meta.treeDepth, visited);
      return true;
    } finally {
      release();
    }
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
    const minKeys = Math.max(1, MIN_LEAF_KEYS);
    if (leaf.page.cells.length >= minKeys) {
      this.#updateParentKeyForLeaf(parent, parent.childIndex, leaf.page);
      return { leaf, dirty };
    }

    const leftSlot = parent.childIndex - 1;
    const leftPageNumber = this.#childPageNumber(parent.page, leftSlot);
    if (leftPageNumber !== null) {
      const leftLeaf = await this.#loadLeaf(leftPageNumber);
      if (leftLeaf.page.cells.length > MIN_LEAF_KEYS) {
        const borrowed = leftLeaf.page.cells.pop();
        if (borrowed) {
          const candidateSize = this.#leafSerializedSize([
            borrowed,
            ...leaf.page.cells,
          ]);
          if (candidateSize <= this.pageManager.pageSize) {
            leaf.page.cells.unshift(borrowed);
            this.#updateParentKeyForLeaf(parent, parent.childIndex, leaf.page);
            this.#releaseLeaf(leftLeaf, true);
            return { leaf, dirty: true };
          }
          leftLeaf.page.cells.push(borrowed);
        }
      }
      this.#releaseLeaf(leftLeaf, false);
    }

    const rightSlot = parent.childIndex + 1;
    const rightPageNumber = this.#childPageNumber(parent.page, rightSlot);
    if (rightPageNumber !== null) {
      const rightLeaf = await this.#loadLeaf(rightPageNumber);
      if (rightLeaf.page.cells.length > MIN_LEAF_KEYS) {
        const borrowed = rightLeaf.page.cells.shift();
        if (borrowed) {
          const candidateSize = this.#leafSerializedSize([
            ...leaf.page.cells,
            borrowed,
          ]);
          if (candidateSize <= this.pageManager.pageSize) {
            leaf.page.cells.push(borrowed);
            this.#updateParentKeyForLeaf(parent, rightSlot, rightLeaf.page);
            this.#updateParentKeyForLeaf(parent, parent.childIndex, leaf.page);
            this.#releaseLeaf(rightLeaf, true);
            return { leaf, dirty: true };
          }
          rightLeaf.page.cells.unshift(borrowed);
        }
      }
      this.#releaseLeaf(rightLeaf, false);
    }

    if (leftPageNumber !== null) {
      const leftLeaf = await this.#loadLeaf(leftPageNumber);
      const mergedSize = this.#leafSerializedSize([
        ...leftLeaf.page.cells,
        ...leaf.page.cells,
      ]);
      if (mergedSize <= this.pageManager.pageSize) {
        leftLeaf.page.cells.push(...leaf.page.cells);
        leftLeaf.page.rightSibling = leaf.page.rightSibling;
        this.bufferPool.unpin(leaf.pageNumber, false);
        this.bufferPool.dropPage(leaf.pageNumber);
        await this.pageManager.freePage(leaf.pageNumber);
        this.#removeParentCell(parent, parent.childIndex);
        parent.childIndex = leftSlot;
        this.#updateParentKeyForLeaf(parent, leftSlot, leftLeaf.page);
        return { leaf: leftLeaf, dirty: true };
      }
      this.#releaseLeaf(leftLeaf, false);
    }

    if (rightPageNumber !== null) {
      const rightLeaf = await this.#loadLeaf(rightPageNumber);
      const mergedSize = this.#leafSerializedSize([
        ...leaf.page.cells,
        ...rightLeaf.page.cells,
      ]);
      if (mergedSize <= this.pageManager.pageSize) {
        leaf.page.cells.push(...rightLeaf.page.cells);
        leaf.page.rightSibling = rightLeaf.page.rightSibling;
        this.bufferPool.unpin(rightLeaf.pageNumber, false);
        this.bufferPool.dropPage(rightLeaf.pageNumber);
        await this.pageManager.freePage(rightLeaf.pageNumber);
        this.#removeParentCell(parent, rightSlot);
        this.#updateParentKeyForLeaf(parent, parent.childIndex, leaf.page);
        return { leaf, dirty: true };
      }
      this.#releaseLeaf(rightLeaf, false);
    }

    return { leaf, dirty };
  }

  async #splitLeaf(
    leaf: LoadedPage<LeafPage>,
  ): Promise<{ key: bigint; pageNumber: number }> {
    const totalSize = this.#leafSerializedSize(leaf.page.cells);
    const targetSize = totalSize / 2;
    let accumulated = PAGE_HEADER_SIZE;
    let splitIndex = 0;
    for (; splitIndex < leaf.page.cells.length - 1; splitIndex += 1) {
      const cell = leaf.page.cells[splitIndex];
      if (!cell) {
        break;
      }
      const cellSize = this.#leafCellSerializedSize(cell);
      if (accumulated + cellSize >= targetSize) {
        break;
      }
      accumulated += cellSize;
    }
    const siblingCells = leaf.page.cells.splice(splitIndex + 1);
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
    const first = siblingPage.cells[0];
    if (!first) {
      throw new Error("Leaf split produced empty sibling");
    }
    return { key: first.key, pageNumber: siblingPageNumber };
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
      throw new Error("Internal page split failed");
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

  #insertIntoLeaf(page: LeafPage, cell: LeafCell): number {
    let idx = 0;
    while (idx < page.cells.length) {
      const existing = page.cells[idx];
      if (!existing || existing.key >= cell.key) {
        break;
      }
      idx += 1;
    }
    page.cells.splice(idx, 0, cell);
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

  async #insertKeyValue(key: bigint, normalizedValue: Buffer): Promise<void> {
    const { path, leaf } = await this.#traverseToLeaf(key, true);
    try {
      const existingIndex = leaf.page.cells.findIndex((cell) => cell.key === key);
        if (existingIndex >= 0) {
          const previous = leaf.page.cells[existingIndex];
          if (previous) {
            await this.overflowManager.freeChain(previous.overflowPage);
          }
          leaf.page.cells[existingIndex] = await this.#prepareLeafCell(key, normalizedValue);
        this.#releaseLeaf(leaf, true);
        return;
      }

      const insertIndex = this.#insertIntoLeaf(
        leaf.page,
        await this.#prepareLeafCell(key, normalizedValue),
      );
      if (insertIndex === 0) {
        const parent = path[path.length - 1];
        if (parent) {
          this.#updateParentKeyForLeaf(parent, parent.childIndex, leaf.page);
        }
      }

      while (this.#leafSerializedSize(leaf.page.cells) > this.pageManager.pageSize) {
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
    for (let depth = this.meta.treeDepth; depth > 1; ) {
      const internal = await this.#loadInternal(pageNumber);
      const { child, slot, nextSibling } = this.#pickChildSlot(internal.page, key);
      if (nextSibling) {
        this.#releaseInternal(internal, false);
        pageNumber = nextSibling;
        continue;
      }
      const entry: InternalPathEntry = { ...internal, dirty: false, childIndex: slot };
      path.push(entry);
      if (!keepPath) {
        this.#releaseInternal(entry, false);
        path.pop();
      }
      pageNumber = child;
      depth -= 1;
    }
    const leaf = await this.#loadLeaf(pageNumber);
    return { leaf, path };
  }

  async #findLeafForKey(
    key: bigint,
  ): Promise<{ leaf: LoadedPage<LeafPage>; release: () => void }> {
    const snapshot = { rootPage: this.meta.rootPage, depth: this.meta.treeDepth };
    let pageNumber = snapshot.rootPage;
    let depth = snapshot.depth;
    let release = await this.latchManager.acquireShared(pageNumber);

    if (depth === 1) {
      const leaf = await this.#loadLeaf(pageNumber);
      return this.#maybeMoveRightForSearch(leaf, release, key);
    }

    for (; depth > 1; depth -= 1) {
      const internal = await this.#loadInternal(pageNumber);
      const { child } = this.#pickChildSlot(internal.page, key);
      this.#releaseInternal(internal, false);
      const nextRelease = await this.latchManager.acquireShared(child);
      release();
      pageNumber = child;
      release = nextRelease;
    }

    const leaf = await this.#loadLeaf(pageNumber);
    return this.#maybeMoveRightForSearch(leaf, release, key);
  }

  #pickChildSlot(
    page: InternalPage,
    key: bigint,
  ): { child: number; slot: number; nextSibling: number | null } {
    let child = page.leftChild;
    if (page.cells.length === 0) {
      return { child, slot: -1, nextSibling: null };
    }
    for (let i = 0; i < page.cells.length; i += 1) {
      const cell = page.cells[i];
      if (!cell) {
        continue;
      }
      if (key < cell.key) {
        return { child, slot: i - 1, nextSibling: null };
      }
      child = cell.child;
    }
    if (page.rightSibling) {
      return {
        child: page.rightSibling,
        slot: page.cells.length - 1,
        nextSibling: page.rightSibling,
      };
    }
    return { child, slot: page.cells.length - 1, nextSibling: null };
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

  async #collectAllEntries(): Promise<Array<{ key: Buffer; value: Buffer }>> {
    const entries: Array<{ key: Buffer; value: Buffer }> = [];
    let pageNumber = this.meta.rootPage;
    for (let depth = this.meta.treeDepth; depth > 1; depth -= 1) {
      const internal = await this.#loadInternal(pageNumber);
      pageNumber = internal.page.leftChild;
      this.#releaseInternal(internal, false);
    }
    let leaf = await this.#loadLeaf(pageNumber);
    try {
      while (true) {
        for (const cell of leaf.page.cells) {
          const value = await this.#materializeCellValue(cell);
          entries.push({
            key: Buffer.from(normalizeKeyInput(cell.key)),
            value,
          });
        }
        if (!leaf.page.rightSibling) {
          break;
        }
        const nextLeaf = await this.#loadLeaf(leaf.page.rightSibling);
        this.#releaseLeaf(leaf, false);
        leaf = nextLeaf;
      }
    } finally {
      this.#releaseLeaf(leaf, false);
    }
    return entries;
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

  #internalContext(): InternalRebalanceContext {
    return {
      bufferPool: this.bufferPool,
      pageManager: this.pageManager,
      getMeta: () => this.meta,
      mutateMeta: (mutator) => this.#mutateMeta(mutator),
      childPageNumber: (page, slot) => this.#childPageNumber(page, slot),
      removeParentCell: (parent, index) => this.#removeParentCell(parent, index),
      loadInternal: (pageNumber) => this.#loadInternal(pageNumber),
      releaseInternal: (page, dirty) => this.#releaseInternal(page, dirty),
      dropPage: (pageNumber) => this.bufferPool.dropPage(pageNumber),
    };
  }

  async #maybeCheckpoint(): Promise<void> {
    this.#opsSinceCheckpoint += 1;
    const opsDue =
      this.#checkpointIntervalOps > 0 &&
      this.#opsSinceCheckpoint >= this.#checkpointIntervalOps;
    const timeDue =
      this.#checkpointIntervalMs > 0 &&
      Date.now() - this.#lastCheckpointTime >= this.#checkpointIntervalMs;
    if (!opsDue && !timeDue) {
      return;
    }
    this.#opsSinceCheckpoint = 0;
    this.#lastCheckpointTime = Date.now();
    await this.bufferPool.flushAll();
    await this.wal.checkpoint(this.pageManager);
  }

  #leafCellSerializedSize(cell: LeafCell): number {
    return 12 + KEY_SIZE_BYTES + cell.inlineValue.length;
  }

  #leafSerializedSize(cells: LeafCell[]): number {
    let size = PAGE_HEADER_SIZE + cells.length * 2;
    for (const cell of cells) {
      size += 12 + KEY_SIZE_BYTES + cell.inlineValue.length;
    }
    return size;
  }

  async #prepareLeafCell(key: bigint, value: Buffer): Promise<LeafCell> {
    const inlineLimit = this.#maxInlineValueBytes();
    const inlineLength = Math.min(value.length, inlineLimit);
    const inlineValue = Buffer.from(value.subarray(0, inlineLength));
    const remainder = value.subarray(inlineLength);
    const overflowPage =
      remainder.length > 0 ? await this.overflowManager.allocateChain(remainder) : 0;
    return {
      key,
      inlineValue,
      valueLength: value.length,
      overflowPage,
    };
  }

  async #materializeCellValue(cell: LeafCell): Promise<Buffer> {
    if (cell.overflowPage === 0) {
      return Buffer.from(cell.inlineValue);
    }
    const inline = Buffer.from(cell.inlineValue);
    const remainder = await this.overflowManager.readChain(
      cell.overflowPage,
      cell.valueLength - inline.length,
    );
    return Buffer.concat([inline, remainder], cell.valueLength);
  }

  #maxInlineValueBytes(): number {
    const overhead = PAGE_HEADER_SIZE + 2 + KEY_SIZE_BYTES + 12;
    return Math.max(0, this.pageManager.pageSize - overhead);
  }

  #leafHighKey(page: LeafPage): bigint | null {
    if (page.cells.length === 0) {
      return null;
    }
    return page.cells[page.cells.length - 1]!.key;
  }

  async #maybeMoveRightForSearch(
    leaf: LoadedPage<LeafPage>,
    release: () => void,
    key: bigint,
  ): Promise<{ leaf: LoadedPage<LeafPage>; release: () => void }> {
    let currentLeaf = leaf;
    let currentRelease = release;
    while (currentLeaf.page.rightSibling) {
      const highKey = this.#leafHighKey(currentLeaf.page);
      if (highKey === null || key <= highKey) {
        break;
      }
      const siblingPage = currentLeaf.page.rightSibling;
      const siblingRelease = await this.latchManager.acquireShared(siblingPage);
      const siblingLeaf = await this.#loadLeaf(siblingPage);
      this.#releaseLeaf(currentLeaf, false);
      currentRelease();
      currentLeaf = siblingLeaf;
      currentRelease = siblingRelease;
    }
    return { leaf: currentLeaf, release: currentRelease };
  }

  async #emitDiagnostics(reason: string): Promise<void> {
    if (!this.#diagnostics) {
      return;
    }
    const bufferStats = this.bufferPool.getStats();
    const walStats = this.wal.getStats();
    const mem = process.memoryUsage();
    const snapshot = {
      bufferPool: bufferStats,
      wal: walStats,
      rssBytes: mem.rss,
      heapUsedBytes: mem.heapUsed,
      reason,
    };
    this.#diagnostics.onSnapshot?.(snapshot);
    if (mem.rss > this.#rssLimit) {
      this.#diagnostics.onAlert?.(
        `RSS ${mem.rss} exceeds limit ${this.#rssLimit}`,
        snapshot,
      );
    }
    if (
      this.#bufferPageLimit &&
      bufferStats.maxResidentPages > this.#bufferPageLimit
    ) {
      this.#diagnostics.onAlert?.(
        `Buffer pages ${bufferStats.maxResidentPages} exceeded limit ${this.#bufferPageLimit}`,
        snapshot,
      );
    }
  }

  async #cursorNext(
    state: RangeCursorState,
  ): Promise<{ key: Buffer; value: Buffer } | null> {
    if (state.done) {
      return null;
    }
    await this.#ensureCursorLeaf(state);
    while (!state.done && state.leaf) {
      if (state.index >= state.leaf.page.cells.length) {
        if (!state.leaf.page.rightSibling) {
          await this.#cursorClose(state);
          return null;
        }
        await this.#moveCursorToSibling(state, state.leaf.page.rightSibling);
        continue;
      }
      const cell = state.leaf.page.cells[state.index];
      if (!cell) {
        state.index += 1;
        continue;
      }
      state.index += 1;
      if (state.firstLeaf && cell.key < state.startKey) {
        continue;
      }
      state.firstLeaf = false;
      if (cell.key > state.endKey) {
        await this.#cursorClose(state);
        return null;
      }
      const value = await this.#materializeCellValue(cell);
      return {
        key: Buffer.from(normalizeKeyInput(cell.key)),
        value,
      };
    }
    return null;
  }

  async #cursorClose(state: RangeCursorState): Promise<void> {
    if (state.leaf) {
      this.#releaseLeaf(state.leaf, false);
      state.leaf = null;
    }
    if (state.release) {
      state.release();
      state.release = null;
    }
    state.done = true;
  }

  async #ensureCursorLeaf(state: RangeCursorState): Promise<void> {
    if (state.leaf || state.done) {
      return;
    }
    const { leaf, release } = await this.#findLeafForKey(state.startKey);
    state.leaf = leaf;
    state.release = release;
    state.index = 0;
    state.firstLeaf = true;
  }

  async #moveCursorToSibling(
    state: RangeCursorState,
    siblingPageNumber: number,
  ): Promise<void> {
    const nextRelease = await this.latchManager.acquireShared(siblingPageNumber);
    const nextLeaf = await this.#loadLeaf(siblingPageNumber);
    if (state.leaf) {
      this.#releaseLeaf(state.leaf, false);
      state.release?.();
    }
    state.leaf = nextLeaf;
    state.release = nextRelease;
    state.index = 0;
    state.firstLeaf = false;
  }

  #startBackgroundVacuum(): void {
    this.#getVacuumManager().start();
  }

  async #stopBackgroundVacuum(): Promise<void> {
    if (this.#backgroundVacuum) {
      await this.#backgroundVacuum.stop();
    }
  }

  #getVacuumManager(): BackgroundVacuum {
    if (!this.#backgroundVacuum) {
      this.#backgroundVacuum = new BackgroundVacuum(
        this.pageManager,
        this.#vacuumOptions ?? {},
      );
    }
    return this.#backgroundVacuum;
  }
}
