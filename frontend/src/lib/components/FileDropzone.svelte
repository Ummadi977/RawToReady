<script lang="ts">
  let {
    onFiles,
    accept = '.pdf,.xlsx,.xls,.csv',
    disabled = false,
  }: {
    onFiles: (files: File[]) => void;
    accept?: string;
    disabled?: boolean;
  } = $props();

  let isDragging = $state(false);
  let selectedFiles = $state<File[]>([]);
  let input: HTMLInputElement | undefined = $state();

  function handleDrop(e: DragEvent) {
    e.preventDefault();
    isDragging = false;
    if (disabled) return;
    const files = Array.from(e.dataTransfer?.files ?? []);
    addFiles(files);
  }

  function handleChange(e: Event) {
    const files = Array.from((e.target as HTMLInputElement).files ?? []);
    addFiles(files);
    // Reset input so same file can be re-selected
    if (input) input.value = '';
  }

  function addFiles(files: File[]) {
    selectedFiles = [...selectedFiles, ...files];
    onFiles(selectedFiles);
  }

  function removeFile(index: number) {
    selectedFiles = selectedFiles.filter((_, i) => i !== index);
    onFiles(selectedFiles);
  }

  function formatSize(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  }

  function fileIcon(name: string): string {
    const ext = name.split('.').pop()?.toLowerCase();
    if (ext === 'pdf') return '📕';
    if (ext === 'xlsx' || ext === 'xls') return '📗';
    if (ext === 'csv') return '📊';
    return '📄';
  }
</script>

<div class="space-y-2">
  <!-- Drop zone -->
  <div
    class="rounded-xl border-2 border-dashed transition-all duration-200 cursor-pointer select-none
           {isDragging
      ? 'border-accent bg-accent/10'
      : 'border-white/10 hover:border-white/20 hover:bg-white/[0.02]'}
           {disabled ? 'opacity-40 pointer-events-none' : ''}"
    role="button"
    tabindex="0"
    ondrop={handleDrop}
    ondragover={(e) => {
      e.preventDefault();
      isDragging = true;
    }}
    ondragleave={() => (isDragging = false)}
    ondragend={() => (isDragging = false)}
    onclick={() => input?.click()}
    onkeydown={(e) => e.key === 'Enter' && input?.click()}
  >
    <input
      bind:this={input}
      type="file"
      {accept}
      multiple
      class="hidden"
      onchange={handleChange}
    />

    <div class="py-8 px-6 text-center">
      <div class="text-3xl mb-3 opacity-60">{isDragging ? '📂' : '📁'}</div>
      <p class="text-sm text-slate-400 font-medium">
        {isDragging ? 'Drop files here' : 'Drop files or click to browse'}
      </p>
      <p class="text-xs text-slate-600 mt-1">PDF, Excel (.xlsx/.xls), CSV</p>
    </div>
  </div>

  <!-- Selected files -->
  {#if selectedFiles.length > 0}
    <div class="space-y-1.5">
      {#each selectedFiles as file, i}
        <div
          class="glass-2 rounded-lg px-3 py-2 flex items-center justify-between gap-3 group"
        >
          <div class="flex items-center gap-2 min-w-0">
            <span class="text-sm">{fileIcon(file.name)}</span>
            <span class="text-xs text-slate-300 truncate">{file.name}</span>
          </div>
          <div class="flex items-center gap-2 shrink-0">
            <span class="text-xs text-slate-600">{formatSize(file.size)}</span>
            <button
              class="text-slate-600 hover:text-red-400 transition-colors w-4 h-4 flex items-center justify-center"
              onclick={() => removeFile(i)}
              aria-label="Remove file"
            >
              ×
            </button>
          </div>
        </div>
      {/each}
    </div>
  {/if}
</div>
