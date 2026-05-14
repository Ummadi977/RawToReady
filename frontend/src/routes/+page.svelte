<script lang="ts">
  import EventLog from '$lib/components/EventLog.svelte';
  import DataPreview from '$lib/components/DataPreview.svelte';
  import FileDropzone from '$lib/components/FileDropzone.svelte';
  import FilesPanel from '$lib/components/FilesPanel.svelte';
  import {
    streamScrape,
    streamExtract,
    streamExtractDirect,
    streamClean,
    streamChatClean,
    uploadFiles,
    listFiles,
    previewFile,
    downloadUrl,
  } from '$lib/api';
  import type { FileInfo, PipelineEvent, PreviewData, StepStatus } from '$lib/types';

  // ── Nav ────────────────────────────────────────────────────────────────────
  let activeStep = $state(0);   // 0 scrape · 1 extract · 2 clean

  // ── Scrape ─────────────────────────────────────────────────────────────────
  let scrapeUrl       = $state('');
  let scrapeDir       = $state('');
  let scrapeDesc      = $state('');
  let scrapeFeedback  = $state('');
  let scrapeStatus    = $state<StepStatus>('idle');
  let scrapeEvents    = $state<PipelineEvent[]>([]);
  let scrapeFiles     = $state<FileInfo[]>([]);
  let scrapePreviews  = $state<Record<string, PreviewData>>({});
  let scrapeAbort     = $state<AbortController | null>(null);

  // ── Extract ────────────────────────────────────────────────────────────────
  let extractDir      = $state('');
  let extractSaveAs   = $state('');
  let extractFeedback = $state('');
  let extractDesc     = $state('');
  let extractStatus   = $state<StepStatus>('idle');
  let extractEvents   = $state<PipelineEvent[]>([]);
  let extractFiles    = $state<FileInfo[]>([]);
  let extractPreviews = $state<Record<string, PreviewData>>({});
  let uploadedFiles   = $state<File[]>([]);
  let isUploading     = $state(false);
  let extractMode     = $state<'direct'|'ai'>('direct');
  let extractAbort    = $state<AbortController | null>(null);

  // ── Clean ──────────────────────────────────────────────────────────────────
  let cleanDir        = $state('');
  let cleanDesc       = $state('');
  let cleanFeedback   = $state('');
  let cleanSingleFile = $state(true);
  let cleanStatus     = $state<StepStatus>('idle');
  let cleanEvents     = $state<PipelineEvent[]>([]);
  let cleanFiles      = $state<FileInfo[]>([]);
  let cleanPreviews   = $state<Record<string, PreviewData>>({});
  let cleanAbort      = $state<AbortController | null>(null);

  // Chat cleaning
  interface ChatMsg { role: 'user'|'assistant'; text: string; }
  let chatMessages    = $state<ChatMsg[]>([]);
  let chatInput       = $state('');
  let chatRunning     = $state(false);
  let chatEvents      = $state<PipelineEvent[]>([]);
  let chatAbort       = $state<AbortController | null>(null);

  // ── Helpers ────────────────────────────────────────────────────────────────
  async function loadPreviews(dir: string, stage: string) {
    const { files } = await listFiles(dir, stage);
    const previews: Record<string, PreviewData> = {};
    for (const f of files.filter(fi => fi.ext === '.csv').slice(0, 3)) {
      try { previews[f.name] = await previewFile(f.path); } catch {}
    }
    return { files, previews };
  }

  function isAbort(e: unknown): boolean {
    return e instanceof Error && (e.name === 'AbortError' || e.message.includes('abort'));
  }

  // ── Run scrape ─────────────────────────────────────────────────────────────
  async function runScrape(retry = false) {
    if (!scrapeUrl.trim() || !scrapeDir.trim()) return;
    scrapeAbort?.abort();
    const ctrl = new AbortController();
    scrapeAbort = ctrl;
    scrapeStatus = 'running';
    if (!retry) { scrapeEvents = []; scrapeFiles = []; scrapePreviews = {}; }

    try {
      for await (const ev of streamScrape({
        url: scrapeUrl, output_dir: scrapeDir,
        description: scrapeDesc, feedback: retry ? scrapeFeedback : '',
      }, ctrl.signal)) {
        scrapeEvents = [...scrapeEvents, ev];
        if (ev.type === 'result') {
          scrapeStatus = ev.success ? 'done' : 'error';
          if (ev.success) {
            const r = await loadPreviews(scrapeDir, 'raw');
            scrapeFiles = r.files; scrapePreviews = r.previews;
            if (!extractDir) { extractDir = scrapeDir; extractDesc = scrapeDesc; }
          }
        }
      }
    } catch (e) {
      if (isAbort(e)) { scrapeStatus = 'idle'; }
      else {
        scrapeStatus = 'error';
        scrapeEvents = [...scrapeEvents, { type: 'error', content: String(e) }];
      }
    } finally {
      scrapeAbort = null;
    }
  }

  function stopScrape() { scrapeAbort?.abort(); scrapeStatus = 'idle'; scrapeAbort = null; }

  // ── Run extract (direct) ───────────────────────────────────────────────────
  async function runExtractDirect() {
    if (!extractDir.trim()) return;
    if (uploadedFiles.length > 0) {
      isUploading = true;
      try { await uploadFiles(extractDir, uploadedFiles); uploadedFiles = []; }
      finally { isUploading = false; }
    }

    extractAbort?.abort();
    const ctrl = new AbortController();
    extractAbort = ctrl;
    extractStatus = 'running'; extractEvents = []; extractFiles = []; extractPreviews = {};

    try {
      for await (const ev of streamExtractDirect({ output_dir: extractDir, save_as: extractSaveAs || undefined }, ctrl.signal)) {
        extractEvents = [...extractEvents, ev];
        if (ev.type === 'result') {
          extractStatus = ev.success ? 'done' : 'error';
          if (ev.success) {
            const r = await loadPreviews(extractDir, 'interim');
            extractFiles = r.files; extractPreviews = r.previews;
            if (!cleanDir) { cleanDir = extractDir; cleanDesc = extractDesc; }
          }
        }
      }
    } catch (e) {
      if (isAbort(e)) { extractStatus = 'idle'; }
      else {
        extractStatus = 'error';
        extractEvents = [...extractEvents, { type: 'error', content: String(e) }];
      }
    } finally {
      extractAbort = null;
    }
  }

  // ── Run extract (AI) ───────────────────────────────────────────────────────
  async function runExtractAI(retry = false) {
    if (!extractDir.trim()) return;
    extractAbort?.abort();
    const ctrl = new AbortController();
    extractAbort = ctrl;
    extractStatus = 'running';
    if (!retry) { extractEvents = []; extractFiles = []; extractPreviews = {}; }

    try {
      for await (const ev of streamExtract({
        output_dir: extractDir, description: extractDesc,
        feedback: retry ? extractFeedback : '',
        save_as: extractSaveAs || undefined,
      }, ctrl.signal)) {
        extractEvents = [...extractEvents, ev];
        if (ev.type === 'result') {
          extractStatus = ev.success ? 'done' : 'error';
          if (ev.success) {
            const r = await loadPreviews(extractDir, 'interim');
            extractFiles = r.files; extractPreviews = r.previews;
            if (!cleanDir) { cleanDir = extractDir; cleanDesc = extractDesc; }
          }
        }
      }
    } catch (e) {
      if (isAbort(e)) { extractStatus = 'idle'; }
      else {
        extractStatus = 'error';
        extractEvents = [...extractEvents, { type: 'error', content: String(e) }];
      }
    } finally {
      extractAbort = null;
    }
  }

  function stopExtract() { extractAbort?.abort(); extractStatus = 'idle'; extractAbort = null; }

  // ── Run clean ──────────────────────────────────────────────────────────────
  async function runClean(retry = false) {
    if (!cleanDir.trim()) return;
    cleanAbort?.abort();
    const ctrl = new AbortController();
    cleanAbort = ctrl;
    cleanStatus = 'running';
    if (!retry) { cleanEvents = []; cleanFiles = []; cleanPreviews = {}; }

    try {
      for await (const ev of streamClean({
        output_dir: cleanDir, description: cleanDesc,
        feedback: retry ? cleanFeedback : '',
        single_file_only: cleanSingleFile,
      }, ctrl.signal)) {
        cleanEvents = [...cleanEvents, ev];
        if (ev.type === 'result') {
          cleanStatus = ev.success ? 'done' : 'error';
          if (ev.success) {
            const r = await loadPreviews(cleanDir, 'processed');
            cleanFiles = r.files; cleanPreviews = r.previews;
          }
        }
      }
    } catch (e) {
      if (isAbort(e)) { cleanStatus = 'idle'; }
      else {
        cleanStatus = 'error';
        cleanEvents = [...cleanEvents, { type: 'error', content: String(e) }];
      }
    } finally {
      cleanAbort = null;
    }
  }

  function stopClean() { cleanAbort?.abort(); cleanStatus = 'idle'; cleanAbort = null; }

  // ── Chat clean ─────────────────────────────────────────────────────────────
  async function sendChat() {
    const msg = chatInput.trim();
    if (!msg || chatRunning || !cleanDir.trim()) return;
    chatInput = '';
    chatRunning = true;
    chatMessages = [...chatMessages, { role: 'user', text: msg }];
    chatEvents = [];

    chatAbort?.abort();
    const ctrl = new AbortController();
    chatAbort = ctrl;

    try {
      let lastResult: PipelineEvent | null = null;
      for await (const ev of streamChatClean({ output_dir: cleanDir, message: msg }, ctrl.signal)) {
        chatEvents = [...chatEvents, ev];
        if (ev.type === 'result') lastResult = ev;
      }
      if (lastResult?.success) {
        chatMessages = [...chatMessages, { role: 'assistant', text: '✓ Applied — preview updated.' }];
        const r = await loadPreviews(cleanDir, 'processed');
        cleanFiles = r.files; cleanPreviews = r.previews;
      } else {
        chatMessages = [...chatMessages, { role: 'assistant', text: `✗ ${lastResult?.error ?? 'Something went wrong.'}` }];
      }
    } catch (e) {
      if (!isAbort(e)) {
        chatMessages = [...chatMessages, { role: 'assistant', text: `✗ ${e}` }];
      }
    } finally {
      chatRunning = false;
      chatAbort = null;
      chatEvents = [];
    }
  }

  // ── Derived ────────────────────────────────────────────────────────────────
  const firstScrapePreview  = $derived(Object.entries(scrapePreviews)[0]  as [string,PreviewData]|undefined);
  const firstExtractPreview = $derived(Object.entries(extractPreviews)[0] as [string,PreviewData]|undefined);
  const firstCleanPreview   = $derived(Object.entries(cleanPreviews)[0]   as [string,PreviewData]|undefined);
  const steps = ['Scrape', 'Extract', 'Clean'];
  const stepStatuses = $derived([scrapeStatus, extractStatus, cleanStatus]);

  const runningStepIndex = $derived(
    scrapeStatus === 'running' ? 0 : extractStatus === 'running' ? 1 : cleanStatus === 'running' ? 2 : -1
  );
  const runningEvents = $derived(
    runningStepIndex === 0 ? scrapeEvents : runningStepIndex === 1 ? extractEvents : runningStepIndex === 2 ? cleanEvents : []
  );
  const latestRunningEvent = $derived(
    [...runningEvents].reverse().find(e => e.type !== 'result' && e.type !== 'done')
  );

  function stopStep(i: number) {
    if (i === 0) stopScrape();
    else if (i === 1) stopExtract();
    else stopClean();
  }

  function eventBarColor(type: string): string {
    switch (type) {
      case 'tool_call': return '#60a5fa';
      case 'tool_result': return '#34d399';
      case 'error': return '#f87171';
      default: return '#94a3b8';
    }
  }
</script>

<!-- ══════════════════════════════════════════════════════════════════════════ -->
<!-- NAV                                                                         -->
<!-- ══════════════════════════════════════════════════════════════════════════ -->
<div class="min-h-screen" style="background: var(--bg)">
  <nav class="sticky top-0 z-20 border-b px-6 py-4"
       style="background: rgba(10,10,15,0.85); backdrop-filter:blur(20px); border-color:var(--border)">
    <div class="max-w-3xl mx-auto flex items-center justify-between gap-6">
      <!-- Logo -->
      <div class="flex items-center gap-2.5 shrink-0">
        <div class="w-7 h-7 rounded-lg flex items-center justify-center text-white text-xs font-bold"
             style="background:linear-gradient(135deg,#6366f1,#818cf8)">D</div>
        <span class="font-semibold text-white">DataFlow <span style="color:var(--accent)">Agents</span></span>
      </div>

      <!-- Step tabs — always clickable -->
      <div class="flex items-center gap-0 flex-1 max-w-xs">
        {#each steps as label, i}
          <div class="flex items-center gap-0 flex-1">
            <button
              class="step-dot z-10"
              style="{stepStatuses[i]==='done'
                ? 'background:#22c55e;color:#000'
                : stepStatuses[i]==='error'
                  ? 'background:var(--error);color:#fff'
                  : i===activeStep
                    ? 'background:var(--accent);color:#fff;box-shadow:0 0 0 2px rgba(99,102,241,.4)'
                    : 'border:1px solid var(--border-2);color:#475569;cursor:pointer'}"
              onclick={() => activeStep = i}
              title="Go to {label}"
            >
              {#if stepStatuses[i]==='done'}✓
              {:else if stepStatuses[i]==='running'}
                <span class="w-2 h-2 rounded-full bg-current animate-pulse" style="animation-duration:.8s"></span>
              {:else if stepStatuses[i]==='error'}!
              {:else}{i+1}{/if}
            </button>
            {#if i===activeStep}
              <span class="text-xs font-medium ml-1.5 whitespace-nowrap" style="color:var(--accent-2)">{label}</span>
            {/if}
            {#if i < steps.length-1}
              <div class="step-line ml-1"
                   style="background:{stepStatuses[i]==='done' ? 'var(--success)' : 'var(--border)'}"></div>
            {/if}
          </div>
        {/each}
      </div>
    </div>
  </nav>

  <!-- ════════════════════════════════════════════════════════════════════════ -->
  <!-- MAIN                                                                      -->
  <!-- ════════════════════════════════════════════════════════════════════════ -->
  <main class="max-w-3xl mx-auto px-6 py-8 pb-24 space-y-5">

    <!-- ── SCRAPE ─────────────────────────────────────────────────────────── -->
    {#if activeStep === 0}
      <div class="glass rounded-2xl p-6 space-y-5">
        <div class="flex items-center gap-2 mb-1">
          <span class="text-xs font-mono uppercase tracking-widest" style="color:var(--accent)">Step 1</span>
          {#if scrapeStatus==='running'}<span class="tag-running">Running…</span>
          {:else if scrapeStatus==='done'}<span class="tag-success">Done</span>
          {:else if scrapeStatus==='error'}<span class="tag-error">Failed</span>{/if}
        </div>
        <h2 class="text-xl font-semibold text-white -mt-1">Scrape</h2>
        <p class="text-sm" style="color:var(--text-2)">
          Point the AI scraper at a URL and describe what to download.
          <span class="text-slate-600">— or skip this step and upload files directly in Extract.</span>
        </p>

        <div class="space-y-4">
          <div class="space-y-1.5">
            <label for="s-url" class="label">URL</label>
            <input id="s-url" class="input" type="url" placeholder="https://example.gov/data"
                   bind:value={scrapeUrl} disabled={scrapeStatus==='running'} />
          </div>
          <div class="grid grid-cols-2 gap-4">
            <div class="space-y-1.5">
              <label for="s-dir" class="label">Dataset Name</label>
              <input id="s-dir" class="input" placeholder="e.g. crime_2023"
                     bind:value={scrapeDir} disabled={scrapeStatus==='running'} />
            </div>
            <div class="space-y-1.5">
              <label for="s-desc" class="label">Description</label>
              <input id="s-desc" class="input" placeholder="What data to download"
                     bind:value={scrapeDesc} disabled={scrapeStatus==='running'} />
            </div>
          </div>
        </div>

        <div class="flex gap-3 flex-wrap">
          {#if scrapeStatus !== 'running'}
            <button class="btn-primary" onclick={() => runScrape(false)}
                    disabled={!scrapeUrl.trim() || !scrapeDir.trim()}>
              ▶ Run Scraper
            </button>
          {:else}
            <button class="btn-primary" disabled>
              <span class="w-3.5 h-3.5 rounded-full border-2 border-white/30 border-t-white animate-spin"></span>
              Scraping…
            </button>
            <button class="btn-ghost" style="border:1px solid var(--error);color:var(--error)"
                    onclick={stopScrape}>
              ⏹ Stop
            </button>
          {/if}
          {#if scrapeStatus==='done' || scrapeStatus==='error'}
            <button class="btn-ghost" onclick={() => runScrape(true)}>↩ Retry</button>
          {/if}
        </div>

        {#if scrapeEvents.length > 0 || scrapeStatus === 'running'}
          <EventLog events={scrapeEvents} running={scrapeStatus==='running'} />
        {/if}
        {#if scrapeFiles.length > 0}
          <FilesPanel files={scrapeFiles} outputDir={scrapeDir} stage="raw" />
        {/if}
        {#if firstScrapePreview}
          <DataPreview data={firstScrapePreview[1]} filename={firstScrapePreview[0]} totalFiles={scrapeFiles.length} />
        {/if}

        {#if scrapeStatus==='done' || scrapeStatus==='error'}
          <div class="space-y-3 pt-2 border-t" style="border-color:var(--border)">
            <label for="s-fb" class="label">Feedback <span class="text-slate-600 normal-case font-normal tracking-normal">(optional)</span></label>
            <textarea id="s-fb" class="input resize-none" rows="2"
              placeholder="e.g. Missing files from 2020–2022…" bind:value={scrapeFeedback}></textarea>
            <div class="flex justify-between">
              <button class="btn-ghost text-xs" onclick={() => runScrape(true)} disabled={!scrapeFeedback.trim()}>
                ↩ Re-run with feedback
              </button>
              <button class="btn-primary" onclick={() => { extractDir = scrapeDir || extractDir; activeStep = 1; }}>
                Next: Extract →
              </button>
            </div>
          </div>
        {:else if scrapeStatus === 'idle'}
          <div class="flex justify-end pt-2">
            <button class="btn-ghost" onclick={() => activeStep = 1}>
              Skip → Extract directly
            </button>
          </div>
        {/if}
      </div>

    <!-- ── EXTRACT ────────────────────────────────────────────────────────── -->
    {:else if activeStep === 1}
      {#if scrapeStatus==='done'}
        <button class="glass rounded-xl px-5 py-3 w-full flex items-center justify-between hover:bg-white/[0.03] transition-colors"
                onclick={() => activeStep = 0}>
          <div class="flex items-center gap-3">
            <span class="tag-success shrink-0">✓ Scraped</span>
            <span class="text-sm text-slate-400 truncate">{scrapeDir}</span>
            <span class="text-xs text-slate-600">{scrapeFiles.length} files</span>
          </div>
          <span class="text-xs text-slate-600">edit ↑</span>
        </button>
      {/if}

      <div class="glass rounded-2xl p-6 space-y-5">
        <div class="flex items-center gap-2 mb-1">
          <span class="text-xs font-mono uppercase tracking-widest" style="color:var(--accent)">Step 2</span>
          {#if extractStatus==='running'}<span class="tag-running">Running…</span>
          {:else if extractStatus==='done'}<span class="tag-success">Done</span>
          {:else if extractStatus==='error'}<span class="tag-error">Failed</span>{/if}
        </div>
        <h2 class="text-xl font-semibold text-white -mt-1">Extract</h2>
        <p class="text-sm" style="color:var(--text-2)">
          Upload PDFs or Excel files (or use already-scraped files) and extract all tables.
          PDFs are extracted using LLM vision. Use <em>AI Extract</em> for multi-file tasks or custom instructions.
        </p>

        <!-- Dataset name -->
        <div class="space-y-1.5">
          <label for="e-dir" class="label">Dataset Name</label>
          <input id="e-dir" class="input" placeholder="e.g. crime_2023 — must match the folder name in data/raw/"
                 bind:value={extractDir} disabled={extractStatus==='running'} />
        </div>

        <!-- Save as -->
        <div class="space-y-1.5">
          <label for="e-save" class="label">
            Save as <span class="text-slate-600 normal-case font-normal tracking-normal">(optional — leave blank to keep one CSV per table)</span>
          </label>
          <input id="e-save" class="input" placeholder="e.g. combined.csv — combines all extracted tables into one file"
                 bind:value={extractSaveAs} disabled={extractStatus==='running'} />
        </div>

        <!-- File upload -->
        <div class="space-y-1.5">
          <p class="label">Upload Files <span class="text-slate-600 normal-case font-normal tracking-normal">(PDF, Excel, CSV) — optional if already scraped</span></p>
          <FileDropzone onFiles={(f) => uploadedFiles = f} disabled={extractStatus==='running'} />
        </div>

        {#if scrapeFiles.length > 0 && extractDir === scrapeDir}
          <div class="glass-2 rounded-lg px-4 py-3 flex items-center gap-2 text-xs text-slate-400">
            <span class="text-emerald-400">✓</span>
            {scrapeFiles.length} scraped file(s) ready in
            <code class="text-slate-300">data/raw/{scrapeDir}/</code>
          </div>
        {/if}

        <!-- Actions -->
        <div class="flex flex-wrap gap-3">
          {#if extractStatus !== 'running'}
            <button class="btn-primary" onclick={() => { extractMode='direct'; runExtractDirect(); }}
                    disabled={!extractDir.trim()}>
              ⚡ Extract Tables
            </button>
            <button class="btn-ghost" style="border:1px solid var(--border)"
                    onclick={() => { extractMode='ai'; runExtractAI(false); }}
                    disabled={!extractDir.trim()}>
              🤖 AI Extract
            </button>
          {:else}
            <button class="btn-primary" disabled>
              <span class="w-3.5 h-3.5 rounded-full border-2 border-white/30 border-t-white animate-spin"></span>
              {extractMode === 'direct' ? 'Extracting…' : 'AI Extracting…'}
            </button>
            <button class="btn-ghost" style="border:1px solid var(--error);color:var(--error)"
                    onclick={stopExtract}>
              ⏹ Stop
            </button>
          {/if}
          {#if extractStatus==='done' || extractStatus==='error'}
            <button class="btn-ghost" onclick={() => extractMode==='direct' ? runExtractDirect() : runExtractAI(true)}>
              ↩ Retry
            </button>
          {/if}
        </div>

        {#if extractEvents.length > 0 || extractStatus === 'running'}
          <EventLog events={extractEvents} running={extractStatus==='running'} />
        {/if}
        {#if extractFiles.length > 0}
          <FilesPanel files={extractFiles} outputDir={extractDir} stage="interim" />
        {/if}
        {#if firstExtractPreview}
          <DataPreview data={firstExtractPreview[1]} filename={firstExtractPreview[0]} totalFiles={extractFiles.length} />
        {/if}

        {#if extractMode==='ai' && (extractStatus==='done' || extractStatus==='error')}
          <div class="space-y-3 pt-2 border-t" style="border-color:var(--border)">
            <label for="e-fb" class="label">Feedback for AI extractor</label>
            <textarea id="e-fb" class="input resize-none" rows="2"
              placeholder="e.g. Use stream method, skip header rows, only page 3…"
              bind:value={extractFeedback}></textarea>
            <button class="btn-ghost text-xs" onclick={() => runExtractAI(true)} disabled={!extractFeedback.trim()}>
              ↩ Re-run AI with feedback
            </button>
          </div>
        {/if}

        <div class="flex justify-between items-center pt-2">
          <button class="btn-ghost text-xs" onclick={() => activeStep = 0}>← Back to Scrape</button>
          <button class="btn-primary" onclick={() => { if (extractDir) cleanDir = cleanDir || extractDir; activeStep = 2; }}>
            {extractStatus === 'done' ? 'Next: Clean →' : 'Skip to Clean →'}
          </button>
        </div>
      </div>

    <!-- ── CLEAN ──────────────────────────────────────────────────────────── -->
    {:else if activeStep === 2}
      {#if scrapeStatus==='done'}
        <button class="glass rounded-xl px-5 py-3 w-full flex items-center justify-between hover:bg-white/[0.03] transition-colors"
                onclick={() => activeStep = 0}>
          <div class="flex items-center gap-3">
            <span class="tag-success shrink-0">✓ Scraped</span>
            <span class="text-sm text-slate-400 truncate">{scrapeDir}</span>
          </div>
          <span class="text-xs text-slate-600">edit ↑</span>
        </button>
      {/if}
      {#if extractStatus==='done'}
        <button class="glass rounded-xl px-5 py-3 w-full flex items-center justify-between hover:bg-white/[0.03] transition-colors"
                onclick={() => activeStep = 1}>
          <div class="flex items-center gap-3">
            <span class="tag-success shrink-0">✓ Extracted</span>
            <span class="text-sm text-slate-400 truncate">{extractDir}</span>
            <span class="text-xs text-slate-600">{extractFiles.length} CSVs</span>
          </div>
          <span class="text-xs text-slate-600">edit ↑</span>
        </button>
      {/if}

      <div class="glass rounded-2xl p-6 space-y-5">
        <div class="flex items-center gap-2 mb-1">
          <span class="text-xs font-mono uppercase tracking-widest" style="color:var(--accent)">Step 3</span>
          {#if cleanStatus==='running'}<span class="tag-running">Running…</span>
          {:else if cleanStatus==='done'}<span class="tag-success">Done</span>
          {:else if cleanStatus==='error'}<span class="tag-error">Failed</span>{/if}
        </div>
        <h2 class="text-xl font-semibold text-white -mt-1">Clean</h2>
        <p class="text-sm" style="color:var(--text-2)">
          The AI normalises interim CSVs into clean datasets.
          <span class="text-slate-600">— you can start here if interim/ files already exist.</span>
        </p>

        <!-- Config -->
        <div class="grid grid-cols-2 gap-4">
          <div class="space-y-1.5">
            <label for="c-dir" class="label">Dataset Name</label>
            <input id="c-dir" class="input" placeholder="e.g. crime_2023 — must match data/interim/ folder"
                   bind:value={cleanDir} disabled={cleanStatus==='running'} />
          </div>
          <div class="space-y-1.5">
            <label for="c-desc" class="label">Description</label>
            <input id="c-desc" class="input" placeholder="What the data represents"
                   bind:value={cleanDesc} disabled={cleanStatus==='running'} />
          </div>
        </div>

        <!-- Toggle: single file -->
        <div class="glass-2 rounded-xl p-4 flex items-center justify-between gap-4">
          <div>
            <p class="text-sm font-medium text-slate-200">Test on single file first</p>
            <p class="text-xs mt-0.5" style="color:var(--text-2)">Process one file, review, then run on all.</p>
          </div>
          <button
            class="relative w-11 h-6 rounded-full transition-colors shrink-0"
            style="background:{cleanSingleFile ? 'var(--accent)' : 'rgba(255,255,255,0.1)'}"
            onclick={() => cleanSingleFile = !cleanSingleFile}
            role="switch" aria-checked={cleanSingleFile} aria-label="Test on single file first">
            <span class="absolute top-0.5 w-5 h-5 bg-white rounded-full shadow transition-transform"
                  style="transform:translateX({cleanSingleFile ? '22px' : '2px'})"></span>
          </button>
        </div>

        <!-- Actions -->
        <div class="flex flex-wrap gap-3">
          {#if cleanStatus !== 'running'}
            <button class="btn-primary" onclick={() => runClean(false)} disabled={!cleanDir.trim()}>
              {cleanSingleFile ? '▶ Clean (test file)' : '▶ Clean All Files'}
            </button>
            {#if cleanStatus==='done' && cleanSingleFile}
              <button class="btn-ghost" onclick={() => { cleanSingleFile=false; runClean(false); }}>
                ▶ Process All Files
              </button>
            {/if}
            {#if cleanStatus==='done' || cleanStatus==='error'}
              <button class="btn-ghost" onclick={() => runClean(true)}>↩ Retry</button>
            {/if}
          {:else}
            <button class="btn-primary" disabled>
              <span class="w-3.5 h-3.5 rounded-full border-2 border-white/30 border-t-white animate-spin"></span>
              Cleaning…
            </button>
            <button class="btn-ghost" style="border:1px solid var(--error);color:var(--error)"
                    onclick={stopClean}>
              ⏹ Stop
            </button>
          {/if}
        </div>

        {#if cleanEvents.length > 0 || cleanStatus === 'running'}
          <EventLog events={cleanEvents} running={cleanStatus==='running'} />
        {/if}
        {#if cleanFiles.length > 0}
          <FilesPanel files={cleanFiles} outputDir={cleanDir} stage="processed" />
        {/if}
        {#if firstCleanPreview}
          <DataPreview data={firstCleanPreview[1]} filename={firstCleanPreview[0]} totalFiles={cleanFiles.length} />
        {/if}

        <!-- Standard feedback -->
        {#if cleanStatus==='done' || cleanStatus==='error'}
          <div class="space-y-3 pt-3 border-t" style="border-color:var(--border)">
            <label for="c-fb" class="label">Feedback</label>
            <textarea id="c-fb" class="input resize-none" rows="2"
              placeholder="e.g. Rename columns to snake_case, drop empty rows…"
              bind:value={cleanFeedback}></textarea>
            <div class="flex justify-between items-center">
              <button class="btn-ghost text-xs" onclick={() => runClean(true)} disabled={!cleanFeedback.trim()}>
                ↩ Re-run with feedback
              </button>
              {#if cleanStatus==='done'}
                <a href={downloadUrl(cleanDir,'processed')} class="btn-primary" download>↓ Download</a>
              {/if}
            </div>
          </div>
        {/if}

        <!-- Chat cleaning (always visible if cleanDir is set) -->
        {#if cleanDir.trim()}
          <div class="space-y-3 pt-3 border-t" style="border-color:var(--border)">
            <p class="text-xs font-medium text-slate-400 uppercase tracking-wide">
              Chat Transformations
              <span class="text-slate-600 normal-case font-normal tracking-normal ml-1">— describe any change in plain English</span>
            </p>

            {#if chatMessages.length > 0}
              <div class="glass-2 rounded-xl p-3 space-y-2 max-h-52 overflow-y-auto">
                {#each chatMessages as msg}
                  <div class="flex gap-2 {msg.role==='user' ? 'justify-end' : 'justify-start'}">
                    <div class="max-w-[85%] rounded-xl px-3 py-2 text-xs leading-relaxed
                      {msg.role==='user' ? 'text-white' : 'text-slate-300'}"
                      style="{msg.role==='user' ? 'background:var(--accent)' : 'background:rgba(255,255,255,0.06)'}">
                      {msg.text}
                    </div>
                  </div>
                {/each}
                {#if chatRunning}
                  <div class="flex justify-start">
                    <div class="rounded-xl px-3 py-2 text-xs text-slate-500 animate-pulse"
                         style="background:rgba(255,255,255,0.06)">Applying…</div>
                  </div>
                {/if}
              </div>
            {/if}

            {#if chatRunning && chatEvents.length > 0}
              <EventLog events={chatEvents} running={true} />
            {/if}

            <div class="flex gap-2">
              <input
                class="input flex-1 text-sm"
                placeholder='e.g. "rename column X to state_name" or "melt year columns"'
                bind:value={chatInput}
                disabled={chatRunning}
                onkeydown={(e) => e.key==='Enter' && !e.shiftKey && sendChat()}
              />
              {#if !chatRunning}
                <button class="btn-primary shrink-0" onclick={sendChat}
                        disabled={!chatInput.trim() || !cleanDir.trim()}>Send</button>
              {:else}
                <button class="btn-ghost shrink-0" style="border:1px solid var(--error);color:var(--error)"
                        onclick={() => { chatAbort?.abort(); chatRunning = false; chatAbort = null; chatEvents = []; }}>
                  ⏹ Stop
                </button>
              {/if}
            </div>
          </div>
        {/if}

        <!-- Back button -->
        <div class="flex justify-between items-center pt-2">
          <button class="btn-ghost text-xs" onclick={() => activeStep = 1}>← Back to Extract</button>
          {#if cleanStatus==='done' && !cleanSingleFile}
            <div class="rounded-xl px-4 py-2 text-sm font-medium text-emerald-400"
                 style="background:rgba(34,197,94,0.08);border:1px solid rgba(34,197,94,0.2)">
              🎉 Pipeline complete — <a href={downloadUrl(cleanDir,'processed')} download class="underline">download</a>
            </div>
          {/if}
        </div>

        {#if cleanStatus==='done' && !cleanSingleFile}
          <div class="text-center pt-2">
            <button class="btn-ghost text-xs" onclick={() => {
              activeStep=0;
              scrapeStatus='idle'; scrapeEvents=[];
              extractStatus='idle'; extractEvents=[];
              cleanStatus='idle'; cleanEvents=[];
              chatMessages=[];
            }}>↺ Start a new pipeline</button>
          </div>
        {/if}
      </div>
    {/if}
  </main>

  <!-- ── FLOATING ACTIVITY BAR ─────────────────────────────────────────────── -->
  {#if runningStepIndex !== -1}
    <div class="fixed bottom-0 inset-x-0 z-50 border-t px-6 py-3"
         style="background:rgba(10,10,15,0.94);backdrop-filter:blur(20px);border-color:var(--border)">
      <div class="max-w-3xl mx-auto flex items-center gap-3">

        <!-- Step name — click to jump -->
        <button class="flex items-center gap-2 shrink-0 text-sm font-medium text-white hover:opacity-80 transition-opacity"
                onclick={() => activeStep = runningStepIndex}>
          <span class="w-2 h-2 rounded-full bg-indigo-400 animate-pulse" style="animation-duration:.8s"></span>
          {steps[runningStepIndex]}
        </button>

        <div class="w-px h-4 shrink-0" style="background:var(--border)"></div>

        <!-- Latest event content -->
        <span class="flex-1 min-w-0 font-mono text-xs truncate"
              style="color:{latestRunningEvent ? eventBarColor(latestRunningEvent.type) : '#475569'}">
          {latestRunningEvent?.content ?? 'Starting…'}
        </span>

        <!-- Event count badge -->
        <span class="shrink-0 text-xs tabular-nums" style="color:#475569">
          {runningEvents.filter(e => e.type !== 'result' && e.type !== 'done').length} events
        </span>

        <!-- Stop button -->
        <button class="shrink-0 text-xs px-3 py-1 rounded-lg transition-colors"
                style="border:1px solid var(--error);color:var(--error)"
                onclick={() => stopStep(runningStepIndex)}>
          ⏹ Stop
        </button>
      </div>
    </div>
  {/if}
</div>
