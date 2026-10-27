/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Sale id used when the URL has no `?sale=` parameter. */
  readonly VITE_SALE_ID?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
