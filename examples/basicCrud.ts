import { BPlusTree } from "../index.ts";

async function main() {
  const tree = await BPlusTree.open({ filePath: "./basicCrud.db" });
  try {
    // Insert a few keys
    await tree.set(1, Buffer.from("hello"));
    await tree.set(2, Buffer.from("world"));

    // Update an existing key
    await tree.set(2, Buffer.from("world!"));

    // Get the values back
    console.log("key 1 =", (await tree.get(1))?.toString("utf8"));
    console.log("key 2 =", (await tree.get(2))?.toString("utf8"));

    // Delete a key
    await tree.delete(1);
    console.log("key 1 after delete =", await tree.get(1));
  } finally {
    await tree.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
