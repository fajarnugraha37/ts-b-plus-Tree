import { BPlusTree } from "../index.ts";

async function main() {
  const tree = await BPlusTree.open({
    filePath: "./examples/oneHundred/db.db",
    io: {
      pageSize: 8192,
      readAheadPages: 4,
      walDirectory: "./examples/oneHundred/wal",
      segmentPages: 512 * 5 * 100,
    },
    walOptions: {
      // compressFrames: true,
      // checkpointIntervalOps: 1000 * 15,
      // checkpointIntervalMs: 1000 * 5,
    },
    maintenance: {
      backgroundVacuum: false,
      // backgroundVacuum: true,
      // vacuumOptions: { intervalMs: 5000, batchSize: 64 },
    },
  });

  try {
    console.log("loading 1000k keys with mixed payloads...");
    for (let i = 0; i < 1_000_000; i += 1) {
      const payload = i % 10 === 0 ? Buffer.alloc(4096, i & 0xff) : Buffer.from(`value-${i}`);
      await tree.set(i, payload);
      if (i % 10_000 === 0) {
        const memoryUsage = process.memoryUsage();
        const heapTotalMB = (memoryUsage.heapTotal / (1024 * 1024)).toFixed(2);
        const rssMB = (memoryUsage.rss / (1024 * 1024)).toFixed(2);
        console.log(`  inserted ${i} keys: ${heapTotalMB} MB heap used and ${rssMB} MB RSS`);
      }
    }

    console.log("range 100..150:");
    for await (const row of tree.range(100, 150)) {
      console.log(Number(row.key.readBigUInt64BE()), row.value.subarray(0, 12).toString());
    }
  } finally {
    await tree.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
