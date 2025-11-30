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
    console.log("range 600000..1000000:");
    for await (const row of tree.range(600_000, 1_000_000)) {
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
