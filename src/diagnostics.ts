import type { BufferPoolStats } from "./storage/bufferPool.ts";
import type { WalStats } from "./storage/wal.ts";

export interface DiagnosticsSnapshot {
  bufferPool: BufferPoolStats;
  wal: WalStats;
  rssBytes: number;
  heapUsedBytes: number;
  reason: string;
}

export interface DiagnosticsSink {
  onSnapshot?(snapshot: DiagnosticsSnapshot): void;
  onAlert?(message: string, snapshot: DiagnosticsSnapshot): void;
}

export class ConsoleDiagnosticsSink implements DiagnosticsSink {
  onSnapshot(snapshot: DiagnosticsSnapshot): void {
    console.debug("[BPlusTree]", snapshot.reason, {
      rss: snapshot.rssBytes,
      heap: snapshot.heapUsedBytes,
      bufferLoads: snapshot.bufferPool.pageLoads,
      walCommits: snapshot.wal.commits,
    });
  }

  onAlert(message: string, snapshot: DiagnosticsSnapshot): void {
    console.warn("[BPlusTree][ALERT]", message, {
      rss: snapshot.rssBytes,
      heap: snapshot.heapUsedBytes,
      bufferFlushes: snapshot.bufferPool.pageFlushes,
      walFrames: snapshot.wal.framesWritten,
    });
  }
}
