<script lang="ts">
  import type { PipelineEvent } from '$lib/types';

  let {
    events = [],
    running = false,
  }: {
    events: PipelineEvent[];
    running?: boolean;
  } = $props();

  let container: HTMLDivElement | undefined = $state();

  // Auto-scroll to bottom when new events arrive
  $effect(() => {
    // Touch events array to register dependency
    void events.length;
    if (container) {
      container.scrollTop = container.scrollHeight;
    }
  });

  function eventColor(type: string): string {
    switch (type) {
      case 'tool_call':
        return 'text-blue-400';
      case 'tool_result':
        return 'text-emerald-400';
      case 'thought':
        return 'text-slate-400';
      case 'error':
        return 'text-red-400';
      case 'stopped':
        return 'text-yellow-400';
      default:
        return 'text-slate-300';
    }
  }

  function eventIcon(type: string): string {
    switch (type) {
      case 'tool_call':
        return '⚡';
      case 'tool_result':
        return '✓';
      case 'thought':
        return '◈';
      case 'error':
        return '✗';
      case 'stopped':
        return '⏹';
      default:
        return '›';
    }
  }

  const displayEvents = $derived(events.filter((e) => e.type !== 'result' && e.type !== 'done'));
</script>

<div class="glass rounded-xl overflow-hidden">
  <!-- Header -->
  <div
    class="flex items-center justify-between px-4 py-2.5 border-b"
    style="border-color: var(--border)"
  >
    <span class="text-xs font-mono font-medium uppercase tracking-widest text-slate-500"
      >Event Log</span
    >
    {#if running}
      <span class="tag-running flex items-center gap-1.5">
        <span
          class="w-1.5 h-1.5 rounded-full bg-indigo-400 animate-pulse"
          style="animation-duration: 1s"
        ></span>
        Live
      </span>
    {:else if events.length > 0}
      <span class="text-xs text-slate-600">{displayEvents.length} events</span>
    {/if}
  </div>

  <!-- Events -->
  <div
    bind:this={container}
    class="{running ? 'h-80' : 'h-56'} overflow-y-auto p-3 space-y-0.5 font-mono text-[11px] leading-5 transition-all duration-300"
    style="background: rgba(0,0,0,0.25)"
  >
    {#if displayEvents.length === 0}
      <p class="text-slate-700 italic pl-1">Waiting for events…</p>
    {/if}
    {#each displayEvents as event, i (i)}
      <div class="flex gap-2 {eventColor(event.type)} group">
        <span class="shrink-0 w-3 opacity-60">{eventIcon(event.type)}</span>
        <span class="break-all whitespace-pre-wrap">{event.content ?? ''}</span>
      </div>
    {/each}
    {#if running}
      <div class="flex gap-2 text-slate-600">
        <span class="shrink-0 w-3">›</span>
        <span class="animate-pulse">▌</span>
      </div>
    {/if}
  </div>
</div>
