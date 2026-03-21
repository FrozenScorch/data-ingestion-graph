<script lang="ts">
  import { execution } from '$lib/stores';
  import type { RunStatus } from '$lib/types';

  let {
    graphId = '',
    onRefresh = () => {}
  } = $props();

  function getStatusBadge(status: RunStatus): string {
    switch (status) {
      case 'completed': return 'text-green-400 bg-green-500/10 border-green-500/20';
      case 'running': return 'text-indigo-400 bg-indigo-500/10 border-indigo-500/20';
      case 'failed': return 'text-red-400 bg-red-500/10 border-red-500/20';
      case 'cancelled': return 'text-gray-400 bg-gray-500/10 border-gray-500/20';
      default: return 'text-yellow-400 bg-yellow-500/10 border-yellow-500/20';
    }
  }

  function formatTime(iso: string): string {
    return new Date(iso).toLocaleString();
  }

  function formatDuration(ms: number): string {
    if (!ms) return '-';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
  }
</script>

<div class="space-y-2">
  <div class="flex items-center justify-between">
    <h3 class="text-sm font-medium text-gray-300">Run History</h3>
    <button
      onclick={() => execution.listRuns(graphId)}
      class="text-xs text-gray-500 hover:text-gray-300 transition-colors"
    >
      Refresh
    </button>
  </div>

  {#if execution.loading}
    <div class="text-sm text-gray-500 py-4 text-center">Loading runs...</div>
  {:else if execution.runs.length === 0}
    <div class="text-sm text-gray-600 py-4 text-center">No runs yet</div>
  {:else}
    <div class="space-y-1">
      {#each execution.runs as run (run.id)}
        <a
          href="/graphs/{graphId}/runs/{run.id}"
          class="block px-3 py-2 bg-gray-800/50 border border-gray-800 rounded-lg hover:border-gray-700 transition-colors"
        >
          <div class="flex items-center justify-between">
            <div class="flex items-center gap-2">
              <span class="text-xs px-1.5 py-0.5 rounded border {getStatusBadge(run.status)}">
                {run.status}
              </span>
              <span class="text-sm text-gray-300">{run.id.slice(0, 8)}</span>
            </div>
            <span class="text-xs text-gray-600">{run.trigger_type}</span>
          </div>
          <div class="flex items-center gap-3 mt-1 text-xs text-gray-500">
            <span>{formatTime(run.created_at)}</span>
            {#if run.error_message}
              <span class="text-red-400 truncate" title={run.error_message}>{run.error_message}</span>
            {/if}
          </div>
        </a>
      {/each}
    </div>
  {/if}
</div>
