import type { InternalPage, LeafPage } from "./pages.ts";

export interface LoadedPage<T> {
  pageNumber: number;
  buffer: Buffer;
  page: T;
}

export interface InternalPathEntry extends LoadedPage<InternalPage> {
  dirty: boolean;
  childIndex: number;
}
