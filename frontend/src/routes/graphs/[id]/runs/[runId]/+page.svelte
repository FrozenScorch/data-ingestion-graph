<script lang="ts">
  import { page } from '$app/stores';
  import { onMount } from 'svelte';
  import { execution } from '$lib/stores';
  import LineageViewer from '$lib/components/ui/LineageViewer.svelte';
  import type { NodeRunStatus } from '$lib/types';

  let graphId = $derived($page.params.id);
  let runId = $derived($page.params.runId);

  onMount(() => {
    execution.getRun(runId);
  });

  function getStatusBadge(status: NodeRunStatus): string {
    switch (status) {
      case 'completed': return 'text-green-400 bg-green-500/10 border-green-500/20';
      case 'running': return 'text-indigo-400 bg-indigo-500/10 border-indigo-500/20';
      case 'failed': return 'text-red-400 bg-red-500/10 border-red-500/20';
      case 'cancelled': return 'text-gray-400 bg-gray-500/10 border-gray-500/20';
      case 'skipped': return 'text-yellow-400 bg-yellow-500/10 border-yellow-500/20';
      default: return 'text-gray-400 bg-gray-500/10 border-gray-500/20';
    }
  }
</script>

<div class="p-6 space-y-6">
  <div>
    <a href="/graphs/{graphId}" class="text-xs text-gray-500 hover:text-gray-300 transition-colors">&larr; Back to graph</a>
    <a href="/graphs/{graphId}/runs" class="text-xs text-gray-500 hover:text-gray-300 transition-colors ml-3">&larr; Run history</a>
    <h1 class="text-xl font-bold text-gray-100 mt-1">Run Detail</h1>
    {#if execution.currentRun}
      <p class="text-sm text-gray-500 mt-0.5">Run {runId.slice(0, 8)}... | {execution.currentRun.run.status} | {execution.currentRun.run.trigger_type}</p>
    {/if}
  </div>

  {#if execution.loading}
    <div class="text-sm text-gray-500 py-4 text-center">Loading run...</div>
  {:else if execution.currentRun}
    <!-- Node statuses -->
    <div class="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
      <div class="px-4 py-3 border-b border-gray-800">
        <h2 class="text-sm font-medium text-gray-300">Node Execution Status</h2>
      </div>

      {#if execution.currentRun.run_nodes.length === 0}
        <div class="px-4 py-6 text-sm text-gray-600 text-center">No node execution data</div>
      {:else}
        <table class="w-full text-sm">
          <thead>
            <tr class="border-b border-gray-800">
              <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Status</th>
              <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Node</th>
              <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Type</th>
              <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Items</th>
              <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Duration</th>
              <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Retries</th>
              <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Error</th>
            </tr>
          </thead>
          <tbody>
            {#each execution.currentRun.run_nodes as rn (rn.id)}
              <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
                <td class="px-4 py-2">
                  <span class="text-xs px-2 py-0.5 rounded-full border {getStatusBadge(rn.status)}">{rn.status}</span>
                </td>
                <td class="px-4 py-2 text-gray-300 font-mono text-xs">{rn.node_id}</td>
                <td class="px-4 py-2 text-gray-500 text-xs">{rn.node_type}</td>
                <td class="px-4 py-2 text-gray-400 text-xs">{rn.items_processed ?? '-'}</td>
                <td class="px-4 py-2 text-gray-400 text-xs">{rn.duration_ms ? `${rn.duration_ms}ms` : '-'}</td>
                <td class="px-4 py-2 text-gray-400 text-xs">{rn.attempt_count}/{rn.max_retries}</td>
                <td class="px-4 py-2 text-xs text-red-400 max-w-xs truncate" title={rn.error_message || ''}>{rn.error_message || '-'}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </div>

    <!-- Lineage -->
    <LineageViewer
      nodes={execution.currentRun.run_nodes}
      edges={[]}
    />
  {:else}
    <div class="text-sm text-gray-500 py-4 text-center">Run not found</div>
  {/if}
</div>
