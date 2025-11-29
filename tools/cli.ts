#!/usr/bin/env bun
import { Command } from "commander";
import { BPlusTree, exportSnapshot, importSnapshot } from "../index.ts";

const program = new Command();
program.name("ts-btree").description("Backup and restore utilities");

program
  .command("snapshot")
  .requiredOption("-f, --file <path>", "database file to snapshot")
  .requiredOption("-o, --output <path>", "output snapshot path")
  .description("Write a binary snapshot of the database to disk")
  .action(async (options) => {
    const tree = await BPlusTree.open({ filePath: options.file });
    try {
      await exportSnapshot(tree, options.output);
      console.log(`Snapshot saved to ${options.output}`);
    } finally {
      await tree.close();
    }
  });

program
  .command("restore")
  .requiredOption("-f, --file <path>", "target database file (will be created if missing)")
  .requiredOption("-i, --input <path>", "snapshot file to restore from")
  .description("Restore a snapshot into the given database file")
  .action(async (options) => {
    const tree = await BPlusTree.open({ filePath: options.file });
    try {
      await importSnapshot(tree, options.input);
      console.log(`Database restored from ${options.input}`);
    } finally {
      await tree.close();
    }
  });

program
  .command("metrics")
  .requiredOption("-f, --file <path>", "database file")
  .description("Print fragmentation stats")
  .action(async (options) => {
    const tree = await BPlusTree.open({ filePath: options.file });
    try {
      const stats = await tree.pageManager.fragmentationStats();
      console.log(
        JSON.stringify(
          { totalPages: stats.totalPages, freePages: stats.freePages, fragmentation: stats.fragmentation },
          null,
          2,
        ),
      );
    } finally {
      await tree.close();
    }
  });

program.parseAsync().catch((err) => {
  console.error(err);
  process.exit(1);
});
