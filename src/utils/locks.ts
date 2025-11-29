export class AsyncRWLock {
  #readers = 0;
  #writer = false;
  #pendingReads: Array<() => void> = [];
  #pendingWrites: Array<() => void> = [];

  async acquireRead(): Promise<() => void> {
    if (!this.#writer && this.#pendingWrites.length === 0) {
      this.#readers += 1;
      return () => this.#releaseRead();
    }
    return new Promise<() => void>((resolve) => {
      this.#pendingReads.push(() => {
        this.#readers += 1;
        resolve(() => this.#releaseRead());
      });
    });
  }

  async acquireWrite(): Promise<() => void> {
    if (!this.#writer && this.#readers === 0) {
      this.#writer = true;
      return () => this.#releaseWrite();
    }
    return new Promise<() => void>((resolve) => {
      this.#pendingWrites.push(() => {
        this.#writer = true;
        resolve(() => this.#releaseWrite());
      });
    });
  }

  async withReadLock<T>(fn: () => Promise<T> | T): Promise<T> {
    const release = await this.acquireRead();
    try {
      return await fn();
    } finally {
      release();
    }
  }

  async withWriteLock<T>(fn: () => Promise<T> | T): Promise<T> {
    const release = await this.acquireWrite();
    try {
      return await fn();
    } finally {
      release();
    }
  }

  #releaseRead(): void {
    this.#readers -= 1;
    if (this.#readers === 0 && this.#pendingWrites.length > 0) {
      const next = this.#pendingWrites.shift();
      next?.();
    }
  }

  #releaseWrite(): void {
    this.#writer = false;
    if (this.#pendingWrites.length > 0) {
      const next = this.#pendingWrites.shift();
      next?.();
      return;
    }
    while (this.#pendingReads.length > 0) {
      const reader = this.#pendingReads.shift();
      reader?.();
    }
  }
}
