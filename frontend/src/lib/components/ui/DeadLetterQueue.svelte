<script lang="ts">
  // Dead Letter Queue viewer component
  // The DLQ is viewed via the API - this provides the UI shell

  import { dlqService } from '$lib/services';
  import type { DlqItem } from '$lib/services';
  import { ApiError } from '$lib/services';

  let items = $state<DlqItem[]>([]);
  let loading = $state(false);
  let selectedId = $state<string | null>(null);
  let error = $state<string | null>(null);

  async function loadItems() {
    loading = true;
    error = null;
    try {
      const response = await dlqService.listItems();
      items = response.items;
    } catch (e: unknown) {
      if (e instanceof ApiError) {
        error = e.detail;
      } else {
        error = e instanceof Error ? e.message : 'Failed to load DLQ items';
      }
      items = [];
    } finally {
      loading = false;
    }
  }

  async function retryItem(item: DlqItem) {
    try {
      await dlqService.retryItem(item.id);
      await loadItems();
    } catch (e: unknown) {
      error = e instanceof ApiError ? e.detail : e instanceof Error ? e.message : 'Retry failed';
    }
  }

  async function resolveItem(item: DlqItem) {
    try {
      await dlqService.resolveItem(item.id, 'Manually resolved');
      await loadItems();
    } catch (e: unknown) {
      error = e instanceof ApiError ? e.detail : e instanceof Error ? e.message : 'Resolve failed';
    }
  }

  async function retryAll() {
    const unresolved = items.filter(i => !i.resolved);
    for (const item of unresolved) {
      try {
        await dlqService.retryItem(item.id);
      } catch {
        // Continue retrying remaining items even if one fails
      }
    }
    await loadItems();
  }
</script>

<div class="p-6 space-y-6">
  <div class="flex items-center justify-between">
    <div>
      <h2 class="text-lg font-medium text-gray-200">Dead Letter Queue</h2>
      <p class="text-sm text-gray-500 mt-1">Items that failed processing and were routed here for inspection</p>
    </div>
    <div class="flex items-center gap-2">
      <button
        onclick={loadItems}
        disabled={loading}
        class="px-3 py-1.5 text-xs bg-gray-800 text-gray-300 border border-gray-700 rounded-lg hover:bg-gray-700 transition-colors disabled:opacity-50"
      >
        {loading ? 'Loading...' : 'Refresh'}
      </button>
      <button
        onclick={retryAll}
        class="px-3 py-1.5 text-xs bg-indigo-600 text-white rounded-lg hover:bg-indigo-500 transition-colors"
      >
        Retry All
      </button>
    </div>
  </div>

  {#if error}
    <div class="px-4 py-2 bg-red-500/10 border border-red-500/20 rounded-lg text-red-400 text-sm">
      {error}
      <button onclick={() => error = null} class="ml-2 text-red-500 hover:text-red-300">&times;</button>
    </div>
  {/if}

  {#if loading}
    <div class="flex items-center justify-center py-12">
      <div class="text-gray-500 text-sm">Loading DLQ items...</div>
    </div>
  {:else if items.length === 0}
    <div class="flex flex-col items-center justify-center py-16 text-center">
      <svg class="w-12 h-12 text-gray-700 mb-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
        <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
        <polyline points="22 4 12 14.01 9 11.01"/>
      </svg>
      <p class="text-gray-500 text-sm">No items in the Dead Letter Queue</p>
      <p class="text-gray-600 text-xs mt-1">Failed items will appear here for inspection and retry</p>
    </div>
  {:else}
    <div class="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
      <table class="w-full text-sm">
        <thead>
          <tr class="border-b border-gray-800">
            <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Node</th>
            <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Error</th>
            <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Run</th>
            <th class="text-left px-4 py-2 text-xs text-gray-500 font-medium">Created</th>
            <th class="text-right px-4 py-2 text-xs text-gray-500 font-medium">Actions</th>
          </tr>
        </thead>
        <tbody>
          {#each items as item (item.id)}
            <tr class="border-b border-gray-800/50 hover:bg-gray-800/30">
              <td class="px-4 py-2 text-gray-300">{item.node_id}</td>
              <td class="px-4 py-2 text-red-400 max-w-xs truncate">{item.error_message}</td>
              <td class="px-4 py-2 text-gray-500">{item.run_id?.slice(0, 8)}</td>
              <td class="px-4 py-2 text-gray-500">{item.created_at ? new Date(item.created_at).toLocaleString() : '-'}</td>
              <td class="px-4 py-2 text-right">
                <div class="flex items-center justify-end gap-1">
                  <button onclick={() => retryItem(item)} class="text-xs px-2 py-1 text-indigo-400 hover:bg-indigo-500/10 rounded transition-colors">Retry</button>
                  <button onclick={() => resolveItem(item)} class="text-xs px-2 py-1 text-gray-500 hover:bg-gray-700 rounded transition-colors">Resolve</button>
                </div>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>
