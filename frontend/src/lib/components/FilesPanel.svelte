<script lang="ts">
  import type { FileInfo } from '$lib/types';
  import { downloadUrl } from '$lib/api';

  let {
    files = [],
    outputDir = '',
    stage = 'raw',
  }: {
    files: FileInfo[];
    outputDir: string;
    stage?: string;
  } = $props();

  function formatSize(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }

  function fileIcon(ext: string): string {
    if (ext === '.pdf') return '📕';
    if (ext === '.xlsx' || ext === '.xls') return '📗';
    if (ext === '.csv') return '📊';
    if (ext === '.json') return '📋';
    return '📄';
  }
</script>

{#if files.length > 0}
  <div class="glass rounded-xl overflow-hidden">
    <div
      class="flex items-center justify-between px-4 py-2.5 border-b"
      style="border-color: var(--border)"
    >
      <span class="text-xs font-medium text-slate-400">
        📂 Output Files <span class="text-slate-600">({files.length})</span>
      </span>
      {#if outputDir}
        <a
          href={downloadUrl(outputDir, stage)}
          class="btn-ghost !px-2 !py-1 !text-xs"
          download
        >
          ↓ Download ZIP
        </a>
      {/if}
    </div>

    <div class="divide-y" style="divide-color: var(--border)">
      {#each files as file}
        <div class="flex items-center gap-3 px-4 py-2.5 hover:bg-white/[0.02] transition-colors">
          <span class="text-sm">{fileIcon(file.ext)}</span>
          <span class="text-xs text-slate-300 font-mono flex-1 truncate">{file.name}</span>
          <span class="text-xs text-slate-600 shrink-0">{formatSize(file.size)}</span>
        </div>
      {/each}
    </div>
  </div>
{/if}
