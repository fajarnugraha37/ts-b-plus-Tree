import { BPlusTree, createRestorePoint, restoreFromPoint } from "../index.ts";

async function main() {
  const base = "./restorePointDemo.db";
  const tree = await BPlusTree.open({ filePath: base });
  try {
    await tree.set(1, Buffer.from("before"));
    await createRestorePoint(tree, "./restorePoints/rp-before", "before-upgrade");

    await tree.set(1, Buffer.from("after"));
    console.log("current value:", (await tree.get(1))?.toString());
  } finally {
    await tree.close();
  }

  // Restore into a new database file
  await restoreFromPoint("./restorePoints/rp-before", "./restorePointDemo-restored.db");
  const restored = await BPlusTree.open({ filePath: "./restorePointDemo-restored.db" });
  try {
    console.log("restored value:", (await restored.get(1))?.toString());
  } finally {
    await restored.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
