<script lang="ts">
  import type { PreviewData } from '$lib/types';

  let {
    data,
    filename = '',
    totalFiles = 0,
  }: {
    data: PreviewData | null;
    filename?: string;
    totalFiles?: number;
  } = $props();

  function formatValue(v: string | number | null): string {
    if (v === null || v === undefined) return '—';
    return String(v);
  }
</script>

{#if data}
  <div class="glass rounded-xl overflow-hidden">
    <!-- Header -->
    <div
      class="flex items-center justify-between px-4 py-2.5 border-b"
      style="border-color: var(--border)"
    >
      <div class="flex items-center gap-2 min-w-0">
        <span class="text-slate-500 text-xs shrink-0">📄</span>
        <span class="text-xs font-mono text-slate-300 truncate">{filename}</span>
        {#if totalFiles > 1}
          <span class="text-xs text-slate-600 shrink-0">+{totalFiles - 1} more</span>
        {/if}
      </div>
      {#if data.type === 'csv' && data.shape}
        <span class="text-xs text-slate-500 shrink-0 ml-2">
          {data.shape[0].toLocaleString()} rows × {data.shape[1]} cols
        </span>
      {/if}
    </div>

    <!-- Content -->
    {#if data.type === 'csv' && data.columns && data.rows}
      <div class="overflow-auto max-h-72" style="background: rgba(0,0,0,0.2)">
        <table class="w-full text-[11px] border-collapse">
          <thead>
            <tr class="sticky top-0" style="background: rgba(15,15,24,0.95)">
              {#each data.columns as col}
                <th
                  class="px-3 py-2 text-left font-medium text-slate-400 whitespace-nowrap border-b"
                  style="border-color: var(--border)"
                >
                  {col}
                </th>
              {/each}
            </tr>
          </thead>
          <tbody>
            {#each data.rows as row, i}
              <tr class="hover:bg-white/[0.03] transition-colors border-b border-white/[0.03]">
                {#each row as cell}
                  <td class="px-3 py-1.5 text-slate-300 whitespace-nowrap font-mono">
                    {formatValue(cell)}
                  </td>
                {/each}
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
      {#if data.shape && data.shape[0] > (data.rows?.length ?? 0)}
        <div
          class="px-4 py-2 text-xs text-slate-600 border-t text-center"
          style="border-color: var(--border)"
        >
          Showing first {data.rows?.length} of {data.shape[0].toLocaleString()} rows
        </div>
      {/if}
    {:else if data.type === 'text'}
      <pre
        class="p-4 text-xs text-slate-400 font-mono overflow-auto max-h-64 whitespace-pre-wrap">{data.content}</pre>
    {:else if data.type === 'error'}
      <div class="p-4 text-xs text-red-400 font-mono">{data.message}</div>
    {/if}
  </div>
{/if}
