import type { FileInfo, PipelineEvent, PreviewData } from './types';

const BASE = '';   // Vite proxies /api/* → http://localhost:8000

// ── SSE reader ─────────────────────────────────────────────────────────────────

async function* readSSE(url: string, body: unknown, signal?: AbortSignal): AsyncGenerator<PipelineEvent> {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal,
  });

  if (!response.ok || !response.body) {
    throw new Error(`HTTP ${response.status}: ${await response.text()}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const parts = buf.split('\n\n');
      buf = parts.pop() ?? '';
      for (const block of parts) {
        const m = block.match(/^data: (.+)$/m);
        if (m) {
          try { yield JSON.parse(m[1]) as PipelineEvent; } catch { /* skip */ }
        }
      }
    }
  } finally {
    reader.cancel();
  }
}

// ── Scrape ─────────────────────────────────────────────────────────────────────

export interface ScrapeParams {
  url: string;
  output_dir: string;
  description: string;
  feedback?: string;
  html_elements?: string;
}
export function streamScrape(p: ScrapeParams, signal?: AbortSignal): AsyncGenerator<PipelineEvent> {
  return readSSE(`${BASE}/api/scrape`, p, signal);
}

// ── Extract direct (no LLM) ────────────────────────────────────────────────────

export interface DirectExtractParams {
  output_dir: string;
  save_as?: string;
}
export function streamExtractDirect(p: DirectExtractParams, signal?: AbortSignal): AsyncGenerator<PipelineEvent> {
  return readSSE(`${BASE}/api/extract-direct`, p, signal);
}

// ── Extract LLM (fallback for complex layouts) ─────────────────────────────────

export interface ExtractParams {
  output_dir: string;
  description: string;
  feedback?: string;
  save_as?: string;
}
export function streamExtract(p: ExtractParams, signal?: AbortSignal): AsyncGenerator<PipelineEvent> {
  return readSSE(`${BASE}/api/extract`, p, signal);
}

// ── Clean ──────────────────────────────────────────────────────────────────────

export interface CleanParams {
  output_dir: string;
  description: string;
  feedback?: string;
  single_file_only?: boolean;
}
export function streamClean(p: CleanParams, signal?: AbortSignal): AsyncGenerator<PipelineEvent> {
  return readSSE(`${BASE}/api/clean`, p, signal);
}

// ── Chat clean ─────────────────────────────────────────────────────────────────

export interface ChatCleanParams {
  output_dir: string;
  message: string;
}
export function streamChatClean(p: ChatCleanParams, signal?: AbortSignal): AsyncGenerator<PipelineEvent> {
  return readSSE(`${BASE}/api/clean-chat`, p, signal);
}

// ── REST helpers ───────────────────────────────────────────────────────────────

export async function uploadFiles(outputDir: string, files: File[]): Promise<{ saved: string[] }> {
  const form = new FormData();
  for (const f of files) form.append('files', f);
  const res = await fetch(`${BASE}/api/upload?output_dir=${encodeURIComponent(outputDir)}`, {
    method: 'POST', body: form,
  });
  if (!res.ok) throw new Error(`Upload failed: ${res.status}`);
  return res.json();
}

export async function listFiles(outputDir: string, stage: string): Promise<{ files: FileInfo[] }> {
  const res = await fetch(`${BASE}/api/files?output_dir=${encodeURIComponent(outputDir)}&stage=${stage}`);
  if (!res.ok) return { files: [] };
  return res.json();
}

export async function previewFile(path: string): Promise<PreviewData> {
  const res = await fetch(`${BASE}/api/preview?path=${encodeURIComponent(path)}`);
  if (!res.ok) return { type: 'error', message: `HTTP ${res.status}` };
  return res.json();
}

export function downloadUrl(outputDir: string, stage: string): string {
  return `${BASE}/api/download?output_dir=${encodeURIComponent(outputDir)}&stage=${stage}`;
}
