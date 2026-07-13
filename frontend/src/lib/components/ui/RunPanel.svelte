<script lang="ts">
  import { graph, execution } from '$lib/stores';
  import type { NodeRunStatus } from '$lib/types';

  let graphId = $derived(graph.currentGraph?.id);

  // Deduplicate nodes by id (guards against duplicates from backend data or Svelte Flow)
  let uniqueNodes = $derived(() => {
    const seen = new Set<string>();
    const result: typeof graph.nodes = [];
    for (const n of graph.nodes) {
      if (!seen.has(n.id)) {
        seen.add(n.id);
        result.push(n);
      }
    }
    return result;
  });

  async function handleStartRun() {
    if (!graphId) return;
    // Save first
    await graph.saveVersion(graphId);
    // Then run
    await execution.startRun(graphId);
  }

  async function handleCancelRun() {
    if (!execution.currentRun) return;
    await execution.cancelRun(execution.currentRun.run.id);
  }

  function getStatusColor(status: NodeRunStatus): string {
    switch (status) {
      case 'completed': return 'text-green-400 bg-green-500/10 border-green-500/20';
      case 'running': return 'text-indigo-400 bg-indigo-500/10 border-indigo-500/20 animate-pulse';
      case 'failed': return 'text-red-400 bg-red-500/10 border-red-500/20';
      case 'cancelled': return 'text-gray-400 bg-gray-500/10 border-gray-500/20';
      case 'skipped': return 'text-yellow-400 bg-yellow-500/10 border-yellow-500/20';
      default: return 'text-gray-500 bg-gray-500/10 border-gray-500/20';
    }
  }

  function getRunStatusColor(status: string): string {
    switch (status) {
      case 'completed': return 'text-green-400';
      case 'running': return 'text-indigo-400';
      case 'failed': return 'text-red-400';
      case 'cancelled': return 'text-gray-400';
      case 'superseded': return 'text-gray-500';
      default: return 'text-gray-500';
    }
  }
</script>

<div class="bg-gray-900 border-t border-gray-800">
  <!-- Run controls bar -->
  <div class="flex items-center justify-between px-4 py-2 border-b border-gray-800">
    <div class="flex items-center gap-3">
      {#if execution.currentRun}
        <div class="flex items-center gap-2">
          <div class="w-2 h-2 rounded-full
            {execution.currentRun.run.status === 'running' ? 'bg-indigo-400 animate-pulse' : ''}
            {execution.currentRun.run.status === 'completed' ? 'bg-green-400' : ''}
            {execution.currentRun.run.status === 'failed' ? 'bg-red-400' : ''}
            {execution.currentRun.run.status === 'cancelled' ? 'bg-gray-400' : ''}
          "></div>
          <span class="text-xs {getRunStatusColor(execution.currentRun.run.status)}">
            {execution.currentRun.run.status.charAt(0).toUpperCase() + execution.currentRun.run.status.slice(1)}
          </span>
          <span class="text-xs text-gray-600">|</span>
          <span class="text-xs text-gray-500">Run {execution.currentRun.run.id.slice(0, 8)}</span>
        </div>
      {:else}
        <span class="text-xs text-gray-600">No active run</span>
      {/if}
    </div>

    <div class="flex items-center gap-2">
      {#if execution.isRunning}
        <button
          onclick={handleCancelRun}
          class="px-3 py-1 text-xs bg-red-500/10 text-red-400 border border-red-500/20 rounded-lg hover:bg-red-500/20 transition-colors"
        >
          Cancel
        </button>
      {:else}
        <button
          onclick={handleStartRun}
          disabled={graph.loading || graph.saving || execution.loading}
          class="px-3 py-1 text-xs bg-indigo-600 text-white rounded-lg hover:bg-indigo-500 disabled:bg-gray-700 disabled:text-gray-500 transition-colors"
        >
          {graph.saving ? 'Saving...' : execution.loading ? 'Starting...' : 'Run Pipeline'}
        </button>
      {/if}
    </div>
  </div>

  <!-- Node progress -->
  {#if uniqueNodes().length > 0}
    <div class="px-4 py-2 flex items-center gap-2 overflow-x-auto">
      {#each uniqueNodes() as node}
        {@const status = execution.nodeStatuses.get(node.id)}
        <div class="flex items-center gap-1.5 shrink-0 px-2 py-1 rounded border text-xs {status ? getStatusColor(status.status) : 'text-gray-600 bg-gray-800/50 border-gray-800'}">
          <span class="truncate max-w-24">{node.data.label}</span>
          {#if status}
            {#if status.status === 'running'}
              <span class="shrink-0">...</span>
            {:else if status.status === 'completed' && status.items_processed}
              <span class="shrink-0 text-gray-500">({status.items_processed})</span>
            {:else if status.status === 'failed'}
              <span class="shrink-0 text-red-400" title={status.error_message || ''}>!</span>
            {/if}
          {/if}
        </div>
      {/each}
    </div>
  {:else}
    <div class="px-4 py-3 text-xs text-gray-600 text-center">
      Add nodes to the canvas to build a pipeline
    </div>
  {/if}
</div>
