import { mkdtemp, rm } from "fs/promises";
import { join } from "path";
import { tmpdir } from "os";
import { BPlusTree } from "../index.ts";

async function main() {
  const dir = await mkdtemp(join(tmpdir(), "ts-btree-example-"));
  const filePath = join(dir, "data.db");

  const first = await BPlusTree.open({ filePath });
  await first.set(1, Buffer.from("first-run"));
  await first.close();

  const reopened = await BPlusTree.open({ filePath });
  const value = await reopened.get(1);
  console.log("value after reopen =", value?.toString("utf8"));
  await reopened.close();

  await rm(dir, { recursive: true, force: true });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
