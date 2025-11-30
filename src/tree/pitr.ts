import { copyFile, mkdir, readFile, writeFile } from "fs/promises";
import { basename, join } from "path";
import type { BPlusTree } from "./bplusTree.ts";

const METADATA_FILE = "restore.json";

interface RestoreMetadata {
  label?: string;
  timestamp: string;
  dbFiles: string[];
  walFile: string;
}

export async function createRestorePoint(
  tree: BPlusTree,
  targetDir: string,
  label?: string,
): Promise<void> {
  await mkdir(targetDir, { recursive: true });
  await tree.bufferPool.flushAll();
  await tree.pageManager.sync();
  const dbFiles = await tree.pageManager.getFilePaths();
  if (dbFiles.length === 0) {
    throw new Error("No database files found to snapshot");
  }

  for (const file of dbFiles) {
    const name = basename(file);
    await copyFile(file, join(targetDir, name));
  }

  const walName = basename(tree.wal.walPath);
  await copyFile(tree.wal.walPath, join(targetDir, walName));

  const metadata: RestoreMetadata = {
    label,
    timestamp: new Date().toISOString(),
    dbFiles: dbFiles.map((file) => basename(file)),
    walFile: walName,
  };
  await writeFile(join(targetDir, METADATA_FILE), JSON.stringify(metadata, null, 2), "utf8");
}

export async function restoreFromPoint(
  sourceDir: string,
  targetBasePath: string,
): Promise<void> {
  const metadataPath = join(sourceDir, METADATA_FILE);
  const metadataRaw = await readFile(metadataPath, "utf8");
  const metadata = JSON.parse(metadataRaw) as RestoreMetadata;
  if (!metadata.dbFiles || metadata.dbFiles.length === 0) {
    throw new Error("Restore metadata missing database files");
  }
  const baseName = metadata.dbFiles[0]!;

  for (const file of metadata.dbFiles) {
    const source = join(sourceDir, file);
    let dest = targetBasePath;
    if (file !== baseName && file.startsWith(baseName)) {
      const suffix = file.slice(baseName.length);
      dest = `${targetBasePath}${suffix}`;
    } else if (file !== baseName) {
      dest = join(targetBasePath + ".files", file);
      await mkdir(join(targetBasePath + ".files"), { recursive: true });
    }
    await copyFile(source, dest);
  }

  const walTarget = `${targetBasePath}.wal`;
  await copyFile(join(sourceDir, metadata.walFile), walTarget);
}
