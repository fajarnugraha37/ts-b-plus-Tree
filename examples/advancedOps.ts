import { BPlusTree } from "../index.ts";

async function main() {
  const tree = await BPlusTree.open({
    filePath: "./advancedOps.db",
    io: {
      pageSize: 8192,
      readAheadPages: 4,
      walDirectory: "./wal",
      segmentPages: 512,
    },
    walOptions: {
      compressFrames: true,
      checkpointIntervalOps: 100,
      checkpointIntervalMs: 2000,
    },
    maintenance: {
      backgroundVacuum: true,
      vacuumOptions: { intervalMs: 5000, batchSize: 64 },
    },
  });

  try {
    console.log("loading 5k keys with mixed payloads...");
    for (let i = 0; i < 5000; i += 1) {
      const payload = i % 10 === 0 ? Buffer.alloc(4096, i & 0xff) : Buffer.from(`value-${i}`);
      await tree.set(i, payload);
    }

    console.log("range 100..150:");
    for await (const row of tree.range(100, 150)) {
      console.log(Number(row.key.readBigUInt64BE()), row.value.subarray(0, 12).toString());
    }

    console.log("vacuum + defragment sweep...");
    await tree.vacuum();
    await tree.defragment();
  } finally {
    await tree.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
