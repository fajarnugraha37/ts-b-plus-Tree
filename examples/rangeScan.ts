import { BPlusTree } from "../index.ts";

async function main() {
  const tree = await BPlusTree.open({ filePath: "./rangeScan.db" });
  try {
    for (let i = 0; i < 20; i += 1) {
      await tree.set(i, Buffer.from(`value-${i}`));
    }

    console.log("Range 5..15:");
    for await (const { key, value } of tree.range(5, 15)) {
      console.log(Number(key.readBigUInt64BE()), value.toString("utf8"));
    }
  } finally {
    await tree.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
