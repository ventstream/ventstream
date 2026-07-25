/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_VS_HTTP?: string;
  readonly VITE_VS_WS?: string;
  readonly VITE_VS_TENANT?: string;
  readonly VITE_VS_TOKEN?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
