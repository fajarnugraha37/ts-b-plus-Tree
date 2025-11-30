import { setTimeout as delay } from "timers/promises";
import type { PageManager } from "./pageManager.ts";

export interface VacuumOptions {
  intervalMs?: number;
  batchSize?: number;
  maxIdleBatches?: number;
}

export class BackgroundVacuum {
  #pageManager: PageManager;
  #options: Required<VacuumOptions>;
  #running = false;
  #stopRequested = false;
  #idleBatches = 0;
  #loopPromise: Promise<void> | null = null;

  constructor(pageManager: PageManager, options: VacuumOptions = {}) {
    this.#pageManager = pageManager;
    this.#options = {
      intervalMs: options.intervalMs ?? 15_000,
      batchSize: options.batchSize ?? 512,
      maxIdleBatches: options.maxIdleBatches ?? 2,
    };
  }

  start(): void {
    if (this.#running) {
      return;
    }
    this.#stopRequested = false;
    this.#running = true;
    this.#loopPromise = this.#loop();
  }

  async stop(): Promise<void> {
    if (!this.#running) {
      return;
    }
    this.#stopRequested = true;
    await this.#loopPromise;
    this.#loopPromise = null;
  }

  async runOnce(): Promise<void> {
    await this.#runBatch();
  }

  async #loop(): Promise<void> {
    while (!this.#stopRequested) {
      await this.#runBatch();
      await delay(this.#options.intervalMs);
    }
    this.#running = false;
    this.#loopPromise = null;
  }

  async #runBatch(): Promise<void> {
    const meta = await this.#pageManager.readMeta();
    const freePages = await this.#pageManager.collectFreePages(meta);
    if (freePages.length === 0) {
      this.#idleBatches += 1;
      if (this.#idleBatches >= this.#options.maxIdleBatches) {
        this.#stopRequested = true;
      }
      return;
    }
    this.#idleBatches = 0;

    const remaining = freePages
      .filter((page) => page >= 3)
      .sort((a, b) => a - b);
    const candidates = remaining.splice(0, this.#options.batchSize);
    if (candidates.length === 0) {
      return;
    }
    await this.#pageManager.rewriteFreeList(meta, remaining);
    for (const page of candidates) {
      await this.#pageManager.freePage(page);
    }
  }
}
