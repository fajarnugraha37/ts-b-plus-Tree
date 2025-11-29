export const PAGE_SIZE_BYTES = 4 * 1024; // 4KB default page size
export const BUFFER_POOL_PAGES = 25;
export const MAGIC = "BPTREE_V1";

export const PAGE_HEADER_SIZE = 32;
export const INTERNAL_CHILD_POINTER_BYTES = 4;
export const KEY_SIZE_BYTES = 8;
export const OVERFLOW_HEADER_SIZE = 16;

export const MAX_INTERNAL_KEYS = Math.floor(
  (PAGE_SIZE_BYTES - PAGE_HEADER_SIZE - INTERNAL_CHILD_POINTER_BYTES) /
    (KEY_SIZE_BYTES + INTERNAL_CHILD_POINTER_BYTES),
);

export const MIN_INTERNAL_KEYS = Math.floor(MAX_INTERNAL_KEYS / 2);

export const MAX_LEAF_KEYS = Math.floor(
  (PAGE_SIZE_BYTES - PAGE_HEADER_SIZE) / (KEY_SIZE_BYTES + 14),
);

export const MIN_LEAF_KEYS = Math.floor(MAX_LEAF_KEYS / 2);

export enum PageType {
  Meta = 0,
  Internal = 1,
  Leaf = 2,
  Overflow = 3,
}
