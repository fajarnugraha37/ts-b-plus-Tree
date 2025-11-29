#!/usr/bin/env bun
import { Command } from "commander";
import { BPlusTree } from "../index.ts";

type Format = "utf8" | "hex" | "base64";

const program = new Command();
program
  .name("ts-btree")
  .description("Inspect and mutate a ts-btree database")
  .option("-f, --file <path>", "path to the database file", "./cli.db")
  .option("--wal <path>", "write-ahead log path (defaults to <file>.wal)")
  .option(
    "--format <mode>",
    "value encoding: utf8 (default), hex, or base64",
    "utf8",
  );

const globalOptions = (): { file: string; wal?: string; format: Format } => {
  const opts = program.opts<{ file: string; wal?: string; format: Format }>();
  return {
    file: opts.file,
    wal: opts.wal,
    format: (opts.format ?? "utf8") as Format,
  };
};

function parseKey(input: string): number {
  const value = Number(input);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`Invalid key "${input}" - expected a positive integer`);
  }
  return value;
}

function encodeValue(text: string, format: Format): Buffer {
  switch (format) {
    case "utf8":
      return Buffer.from(text, "utf8");
    case "hex":
      return Buffer.from(text, "hex");
    case "base64":
      return Buffer.from(text, "base64");
    default:
      throw new Error(`Unsupported format "${format}"`);
  }
}

function decodeValue(buffer: Buffer, format: Format): string {
  switch (format) {
    case "utf8":
      return buffer.toString("utf8");
    case "hex":
      return buffer.toString("hex");
    case "base64":
      return buffer.toString("base64");
    default:
      throw new Error(`Unsupported format "${format}"`);
  }
}

async function withTree<T>(
  opts: { file: string; wal?: string },
  fn: (tree: BPlusTree) => Promise<T>,
): Promise<T> {
  const tree = await BPlusTree.open({
    filePath: opts.file,
    walPath: opts.wal,
  });
  try {
    return await fn(tree);
  } finally {
    await tree.close();
  }
}

program
  .command("set")
  .argument("<key>", "integer key")
  .argument("<value>", "value encoded according to --format")
  .description("insert or update a key")
  .action(async (keyStr, valueStr) => {
    const opts = globalOptions();
    const key = parseKey(keyStr);
    const value = encodeValue(valueStr, opts.format);
    await withTree(opts, (tree) => tree.set(key, value));
    console.log(`set ${key}`);
  });

program
  .command("get")
  .argument("<key>", "integer key")
  .description("fetch and print a value")
  .action(async (keyStr) => {
    const opts = globalOptions();
    const key = parseKey(keyStr);
    await withTree(opts, async (tree) => {
      const value = await tree.get(key);
      if (!value) {
        console.log("(null)");
        return;
      }
      console.log(decodeValue(value, opts.format));
    });
  });

program
  .command("del")
  .argument("<key>", "integer key")
  .description("delete a key")
  .action(async (keyStr) => {
    const opts = globalOptions();
    const key = parseKey(keyStr);
    await withTree(opts, async (tree) => {
      const removed = await tree.delete(key);
      console.log(removed ? "deleted" : "not-found");
    });
  });

program
  .command("range")
  .argument("<start>", "inclusive start key")
  .argument("<end>", "inclusive end key")
  .option("-l, --limit <n>", "max rows to print", "100")
  .description("scan keys within a range")
  .action(async (startStr, endStr, cmdOpts: { limit: string }) => {
    const opts = globalOptions();
    const start = parseKey(startStr);
    const end = parseKey(endStr);
    const limit = Number(cmdOpts.limit ?? "100");
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new Error("limit must be a positive integer");
    }
    await withTree(opts, async (tree) => {
      let printed = 0;
      for await (const row of tree.range(start, end)) {
        const decoded = decodeValue(row.value, opts.format);
        const keyValue = row.key.readBigUInt64BE();
        console.log(`${keyValue}\t${decoded}`);
        printed += 1;
        if (printed >= limit) {
          break;
        }
      }
      if (printed === 0) {
        console.log("(empty)");
      }
    });
  });

program
  .command("stats")
  .description("print metadata, buffer stats, and WAL counters")
  .action(async () => {
    const opts = globalOptions();
    await withTree(opts, async (tree) => {
      const meta = tree.meta;
      const bufferStats = tree.bufferPool.getStats();
      const walStats = tree.wal.getStats();
      console.log(
        JSON.stringify(
          {
            meta,
            buffer: bufferStats,
            wal: walStats,
          },
          null,
          2,
        ),
      );
    });
  });

program
  .command("defrag")
  .description("run defragmentation and rebuild all pages")
  .action(async () => {
    const opts = globalOptions();
    await withTree(opts, (tree) => tree.defragment());
    console.log("defragmented");
  });

program
  .command("vacuum")
  .description("vacuum trailing free pages")
  .action(async () => {
    const opts = globalOptions();
    await withTree(opts, (tree) => tree.vacuum());
    console.log("vacuumed");
  });

program.parseAsync().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
