import { AsyncRWLock } from "../utils/locks.ts";

export class PageLatchManager {
  #latches = new Map<number, AsyncRWLock>();

  #get(pageNumber: number): AsyncRWLock {
    if (!this.#latches.has(pageNumber)) {
      this.#latches.set(pageNumber, new AsyncRWLock());
    }
    return this.#latches.get(pageNumber)!;
  }

  async acquireShared(pageNumber: number): Promise<() => void> {
    return this.#get(pageNumber).acquireRead();
  }

  async acquireExclusive(pageNumber: number): Promise<() => void> {
    return this.#get(pageNumber).acquireWrite();
  }

  reset(): void {
    this.#latches.clear();
  }
}
