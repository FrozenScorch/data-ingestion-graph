<script lang="ts">
  import { execution } from '$lib/stores';
  import { onMount } from 'svelte';
  import type { RunStatus } from '$lib/types';

  onMount(() => {
    execution.listAllExecutions();
  });

  function getStatusBadge(status: RunStatus): string {
    switch (status) {
      case 'completed': return 'text-green-400 bg-green-500/10 border-green-500/20';
      case 'running': return 'text-indigo-400 bg-indigo-500/10 border-indigo-500/20';
      case 'paused': return 'text-amber-400 bg-amber-500/10 border-amber-500/20';
      case 'failed': return 'text-red-400 bg-red-500/10 border-red-500/20';
      case 'cancelled': return 'text-gray-400 bg-gray-500/10 border-gray-500/20';
      case 'superseded': return 'text-gray-500 bg-gray-500/10 border-gray-500/20';
      default: return 'text-yellow-400 bg-yellow-500/10 border-yellow-500/20';
    }
  }

  function formatTime(iso: string): string {
    return new Date(iso).toLocaleString();
  }
</script>

<div class="p-6 space-y-6">
  <div class="flex items-center justify-between">
    <div>
      <h1 class="text-xl font-bold text-gray-100">All Executions</h1>
      <p class="text-sm text-gray-500 mt-0.5">{execution.totalRuns} total execution{execution.totalRuns !== 1 ? 's' : ''}</p>
    </div>
    <button
      onclick={() => execution.listAllExecutions()}
      class="text-xs text-gray-500 hover:text-gray-300 transition-colors"
    >
      Refresh
    </button>
  </div>

  {#if execution.loading}
    <div class="text-sm text-gray-500 py-4 text-center">Loading executions...</div>
  {:else if execution.runs.length === 0}
    <div class="text-sm text-gray-600 py-8 text-center">No executions yet</div>
  {:else}
    <div class="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
      <table class="w-full text-sm">
        <thead>
          <tr class="border-b border-gray-800">
            <th class="text-left px-4 py-3 text-xs text-gray-500 font-medium">Status</th>
            <th class="text-left px-4 py-3 text-xs text-gray-500 font-medium">Run ID</th>
            <th class="text-left px-4 py-3 text-xs text-gray-500 font-medium">Graph</th>
            <th class="text-left px-4 py-3 text-xs text-gray-500 font-medium">Trigger</th>
            <th class="text-left px-4 py-3 text-xs text-gray-500 font-medium">Created</th>
            <th class="text-left px-4 py-3 text-xs text-gray-500 font-medium">Error</th>
          </tr>
        </thead>
        <tbody>
          {#each execution.runs as run (run.id)}
            <tr class="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors">
              <td class="px-4 py-3">
                <span class="text-xs px-2 py-0.5 rounded-full border {getStatusBadge(run.status)}">{run.status}</span>
              </td>
              <td class="px-4 py-3">
                <a href="/graphs/{run.graph_id}/runs/{run.id}" class="text-gray-300 hover:text-white transition-colors font-mono text-xs">{run.id.slice(0, 8)}...</a>
              </td>
              <td class="px-4 py-3">
                <a href="/graphs/{run.graph_id}" class="text-gray-400 hover:text-white transition-colors text-xs">{run.graph_id.slice(0, 8)}...</a>
              </td>
              <td class="px-4 py-3 text-gray-500 text-xs">{run.trigger_type}</td>
              <td class="px-4 py-3 text-gray-500 text-xs">{formatTime(run.created_at)}</td>
              <td class="px-4 py-3 text-xs text-red-400 max-w-xs truncate">{run.error_message || '-'}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>
