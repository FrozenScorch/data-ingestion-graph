<script lang="ts">
  import { graph, execution } from '$lib/stores';

  let {
    graphId = ''
  } = $props();

  async function handleSave() {
    if (!graphId) return;
    const success = await graph.saveVersion(graphId);
    if (success) {
      // Brief visual feedback could go here
    }
  }

  async function handleRun() {
    if (!graphId) return;
    // Save first, then run
    const saved = await graph.saveVersion(graphId);
    if (saved) {
      await execution.startRun(graphId);
    }
  }

  async function handleCancel() {
    if (!execution.currentRun) return;
    await execution.cancelRun(execution.currentRun.run.id);
  }
</script>

<div class="flex items-center gap-2 px-3 py-1.5 bg-gray-900 border-b border-gray-800">
  <!-- Save -->
  <button
    onclick={handleSave}
    disabled={graph.saving || !graphId}
    class="flex items-center gap-1.5 px-2.5 py-1 text-xs rounded-md border transition-colors
      {graph.saving
        ? 'bg-gray-800 text-gray-500 border-gray-700'
        : 'bg-gray-800 text-gray-300 border-gray-700 hover:bg-gray-700 hover:text-white disabled:opacity-50'
      }"
    title="Save graph (Ctrl+S)"
  >
    <svg class="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z"/>
      <polyline points="17 21 17 13 7 13 7 21"/>
      <polyline points="7 3 7 8 15 8"/>
    </svg>
    {graph.saving ? 'Saving...' : 'Save'}
  </button>

  <!-- Divider -->
  <div class="w-px h-5 bg-gray-800"></div>

  <!-- Run -->
  {#if execution.isRunning}
    <button
      onclick={handleCancel}
      class="flex items-center gap-1.5 px-2.5 py-1 text-xs rounded-md bg-red-500/10 text-red-400 border border-red-500/20 hover:bg-red-500/20 transition-colors"
      title="Cancel run"
    >
      <svg class="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <rect x="6" y="6" width="12" height="12" rx="1"/>
      </svg>
      Cancel
    </button>
  {:else}
    <button
      onclick={handleRun}
      disabled={graph.saving || !graphId || graph.nodes.length === 0}
      class="flex items-center gap-1.5 px-2.5 py-1 text-xs rounded-md bg-indigo-600 text-white hover:bg-indigo-500 disabled:bg-gray-700 disabled:text-gray-500 transition-colors"
      title="Run pipeline"
    >
      <svg class="w-3.5 h-3.5" viewBox="0 0 24 24" fill="currentColor">
        <polygon points="5 3 19 12 5 21 5 3"/>
      </svg>
      Run
    </button>
  {/if}

  <!-- Run status indicator (clickable) -->
  {#if execution.currentRun}
    <a
      href="/graphs/{graphId}/runs/{execution.currentRun.run.id}"
      class="flex items-center gap-1.5 ml-2 px-2 py-0.5 rounded-full text-xs cursor-pointer
        {execution.currentRun.run.status === 'running' ? 'bg-indigo-500/10 text-indigo-400 border border-indigo-500/20' : ''}
        {execution.currentRun.run.status === 'completed' ? 'bg-green-500/10 text-green-400 border border-green-500/20' : ''}
        {execution.currentRun.run.status === 'failed' ? 'bg-red-500/10 text-red-400 border border-red-500/20' : ''}
      "
      title="View run details"
    >
      <div class="w-1.5 h-1.5 rounded-full
        {execution.currentRun.run.status === 'running' ? 'bg-indigo-400 animate-pulse' : ''}
        {execution.currentRun.run.status === 'completed' ? 'bg-green-400' : ''}
        {execution.currentRun.run.status === 'failed' ? 'bg-red-400' : ''}
      "></div>
      {execution.currentRun.run.status}
    </a>
  {/if}

  <!-- Spacer -->
  <div class="flex-1"></div>

  <!-- Run History -->
  {#if graphId}
    <a
      href="/graphs/{graphId}/runs"
      class="flex items-center gap-1.5 px-2.5 py-1 text-xs rounded-md border border-gray-700 bg-gray-800 text-gray-400 hover:bg-gray-700 hover:text-gray-200 transition-colors"
      title="View run history"
    >
      <svg class="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <circle cx="12" cy="12" r="10"/>
        <polyline points="12 6 12 12 16 14"/>
      </svg>
      History
    </a>
  {/if}

  <!-- Graph name -->
  {#if graph.currentGraph}
    <span class="text-xs text-gray-500 truncate max-w-48">{graph.currentGraph.name}</span>
  {/if}
</div>
