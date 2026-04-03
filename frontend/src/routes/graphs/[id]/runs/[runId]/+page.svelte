<script lang="ts">
  import { page } from '$app/stores';
  import { onMount } from 'svelte';
  import { execution } from '$lib/stores';
  import LineageViewer from '$lib/components/ui/LineageViewer.svelte';
  import type { NodeRunStatus, RunNode } from '$lib/types';

  let graphId = $derived($page.params.id);
  let runId = $derived($page.params.runId);

  // Track which nodes have their preview expanded
  let expandedNodes = $state<Set<string>>(new Set());

  onMount(() => {
    execution.getRun(runId);

    // Cleanup WebSocket and execution state on navigation
    return () => {
      execution.disconnectWs();
    };
  });

  function togglePreview(nodeId: string) {
    const next = new Set(expandedNodes);
    if (next.has(nodeId)) {
      next.delete(nodeId);
    } else {
      next.add(nodeId);
    }
    expandedNodes = next;
  }

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

  function getOutputSummary(rn: RunNode): { hasData: boolean; type: string; count: number; preview: string; isEmpty: boolean } {
    const out = rn.output_data;
    if (!out || typeof out !== 'object') {
      return { hasData: false, type: '-', count: 0, preview: '', isEmpty: true };
    }

    // Check for common output shapes: array, items, chunks, documents, embeddings, rows, or any top-level key
    let items: unknown[] = [];
    let shapeName = '';

    if (Array.isArray(out)) {
      items = out;
      shapeName = 'array';
    } else {
      const obj = out as Record<string, unknown>;
      for (const key of ['items', 'chunks', 'documents', 'embeddings', 'rows', 'results', 'records']) {
        if (Array.isArray(obj[key])) {
          items = obj[key] as unknown[];
          shapeName = key;
          break;
        }
      }
      if (items.length === 0 && !shapeName) {
        // Not a recognized array shape — show the full object as a single item
        const otherKeys = Object.keys(obj).filter(k => typeof obj[k] !== 'object' || obj[k] === null);
        if (otherKeys.length > 0) {
          items = [obj];
          shapeName = 'object';
        }
      }
    }

    const isEmpty = items.length === 0;

    const previewItems = items.slice(0, 15);
    const preview = isEmpty
      ? '(empty — no data produced)'
      : previewItems
          .map(item => {
            if (typeof item === 'string') return item.length > 120 ? item.slice(0, 120) + '...' : item;
            try {
              const str = JSON.stringify(item);
              return str.length > 200 ? str.slice(0, 200) + '...' : str;
            } catch {
              return String(item);
            }
          })
          .join('\n');

    const dataType = Array.isArray(out)
      ? `array`
      : shapeName || 'object';

    return {
      hasData: true,
      type: dataType,
      count: items.length,
      preview,
      isEmpty
    };
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
              {@const summary = getOutputSummary(rn)}
              {@const isExpanded = expandedNodes.has(rn.id)}
              <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
                <td class="px-4 py-2">
                  <span class="text-xs px-2 py-0.5 rounded-full border {getStatusBadge(rn.status)}">{rn.status}</span>
                </td>
                <td class="px-4 py-2 text-gray-300 font-mono text-xs">{rn.node_id}</td>
                <td class="px-4 py-2 text-gray-500 text-xs">{rn.node_type}</td>
                <td class="px-4 py-2 text-gray-400 text-xs">
                  {rn.items_processed ?? '-'}
                  {#if summary.hasData && summary.count > 0}
                    <span class="text-gray-600 ml-1">({summary.type}, {summary.count})</span>
                  {/if}
                </td>
                <td class="px-4 py-2 text-gray-400 text-xs">{rn.duration_ms ? `${rn.duration_ms}ms` : '-'}</td>
                <td class="px-4 py-2 text-gray-400 text-xs">{rn.attempt_count}/{rn.max_retries}</td>
                <td class="px-4 py-2 text-xs text-red-400 max-w-xs truncate" title={rn.error_message || ''}>{rn.error_message || '-'}</td>
              </tr>
              {#if summary.hasData}
                <tr class="border-b border-gray-800/50">
                  <td colspan="7" class="px-4 py-1">
                    <button
                      onclick={() => togglePreview(rn.id)}
                      class="flex items-center gap-1.5 text-xs text-gray-500 hover:text-gray-300 transition-colors"
                    >
                      <svg
                        class="w-3 h-3 transition-transform {isExpanded ? 'rotate-90' : ''}"
                        viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
                      >
                        <polyline points="9 18 15 12 9 6"/>
                      </svg>
                      {isExpanded ? 'Hide' : 'Preview'} output
                      {#if summary.isEmpty}
                        <span class="text-yellow-600 ml-1">(empty)</span>
                      {:else if summary.count > 0}
                        <span class="text-gray-600 ml-1">({summary.count} {summary.type})</span>
                      {/if}
                    </button>
                    {#if isExpanded}
                      <div class="mt-2 mb-1 rounded-lg border border-gray-800 bg-gray-950 overflow-hidden">
                        <div class="flex items-center justify-between px-3 py-1.5 border-b border-gray-800 bg-gray-900/50">
                          <span class="text-xs text-gray-500">Output Preview {summary.count > 15 ? `(showing 15 of ${summary.count})` : ''}</span>
                          <span class="text-xs text-gray-600 font-mono">{summary.type}</span>
                        </div>
                        <pre class="px-3 py-2 text-xs text-gray-300 font-mono overflow-x-auto whitespace-pre-wrap max-h-80 overflow-y-auto">{summary.preview || '(no previewable items)'}</pre>
                      </div>
                    {/if}
                  </td>
                </tr>
              {/if}
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
