<script lang="ts">
  import { graph } from '$lib/stores';
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';
  import { graphTemplateService } from '$lib/services/graphTemplateService.js';
  import type { GraphTemplateSummary } from '$lib/types/graphTemplate.js';

  let showCreateDialog = $state(false);
  let newName = $state('');
  let newDescription = $state('');
  let creating = $state(false);
  let templates = $state<GraphTemplateSummary[]>([]);
  let selectedTemplate = $state<string | null>(null);

  onMount(() => {
    graph.listGraphs();
    graphTemplateService.list().then(response => templates = response.templates);
  });

  async function handleCreate() {
    if (!newName.trim()) return;
    creating = true;
    const g = await graph.createGraph(
      newName.trim(),
      newDescription.trim() || undefined,
      selectedTemplate || undefined
    );
    creating = false;
    if (g) {
      showCreateDialog = false;
      newName = '';
      newDescription = '';
      selectedTemplate = null;
      goto(`/graphs/${g.id}`);
    }
  }

  function formatTime(iso: string): string {
    return new Date(iso).toLocaleDateString();
  }

  function getStatusBadge(status: string): string {
    switch (status) {
      case 'active': return 'text-green-400 bg-green-500/10 border-green-500/20';
      case 'draft': return 'text-yellow-400 bg-yellow-500/10 border-yellow-500/20';
      case 'archived': return 'text-gray-400 bg-gray-500/10 border-gray-500/20';
      default: return 'text-gray-400 bg-gray-500/10 border-gray-500/20';
    }
  }
</script>

<div class="p-6 space-y-6">
  <div class="flex items-center justify-between">
    <div>
      <h1 class="text-xl font-bold text-gray-100">Graphs</h1>
      <p class="text-sm text-gray-500 mt-0.5">{graph.totalGraphs} pipeline{graph.totalGraphs !== 1 ? 's' : ''}</p>
    </div>
    <button
      onclick={() => showCreateDialog = true}
      class="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-500 transition-colors"
    >
      <svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <line x1="12" y1="5" x2="12" y2="19"/>
        <line x1="5" y1="12" x2="19" y2="12"/>
      </svg>
      New Graph
    </button>
  </div>

  <!-- Create dialog -->
  {#if showCreateDialog}
    <div class="fixed inset-0 flex items-center justify-center z-50" role="dialog" aria-modal="true" aria-labelledby="create-graph-title">
      <button
        type="button"
        class="absolute inset-0 bg-black/50 cursor-default"
        aria-label="Close create graph dialog"
        onclick={() => showCreateDialog = false}
      ></button>
      <div class="relative bg-gray-900 border border-gray-800 rounded-xl p-6 w-full max-w-md">
        <h2 id="create-graph-title" class="text-lg font-medium text-gray-200 mb-4">Create New Graph</h2>
        <div class="space-y-3">
          <div>
            <span class="block text-sm text-gray-400 mb-2">Starting point</span>
            <div class="grid grid-cols-2 gap-2 max-h-56 overflow-y-auto">
              <button
                type="button"
                onclick={() => selectedTemplate = null}
                class="rounded-lg border p-2 text-left {selectedTemplate === null ? 'border-indigo-500 bg-indigo-500/10' : 'border-gray-700 bg-gray-800'}"
              >
                <span class="block text-xs font-medium text-gray-200">Blank canvas</span>
                <span class="block text-[10px] text-gray-500 mt-1">Build from individual nodes</span>
              </button>
              {#each templates as template (template.id)}
                <button
                  type="button"
                  onclick={() => { selectedTemplate = template.id; if (!newName) newName = template.name; }}
                  class="rounded-lg border p-2 text-left {selectedTemplate === template.id ? 'border-indigo-500 bg-indigo-500/10' : 'border-gray-700 bg-gray-800'}"
                >
                  <span class="flex items-center gap-1 text-xs font-medium text-gray-200">
                    {template.name}
                    {#if template.sdk_node_count > 0}<span class="rounded bg-cyan-500/15 px-1 text-[9px] text-cyan-300">SDK</span>{/if}
                  </span>
                  <span class="block text-[10px] text-gray-500 mt-1">{template.node_count} nodes · {template.category}</span>
                </button>
              {/each}
            </div>
            {#if selectedTemplate}
              {@const template = templates.find(item => item.id === selectedTemplate)}
              {#if template}
                <p class="mt-2 text-[10px] text-gray-500">Setup: {template.setup.join(' · ')}</p>
              {/if}
            {/if}
          </div>
          <div>
            <!-- svelte-ignore a11y_label_has_associated_control -->
            <label class="block text-sm text-gray-400 mb-1">Name</label>
            <input
              type="text"
              bind:value={newName}
              placeholder="My Pipeline"
              class="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-100 placeholder-gray-500 focus:outline-none focus:border-indigo-500"
              onkeydown={(e) => e.key === 'Enter' && handleCreate()}
            />
          </div>
          <div>
            <!-- svelte-ignore a11y_label_has_associated_control -->
            <label class="block text-sm text-gray-400 mb-1">Description</label>
            <textarea
              bind:value={newDescription}
              placeholder="Optional description..."
              rows="3"
              class="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-100 placeholder-gray-500 focus:outline-none focus:border-indigo-500 resize-none"
            ></textarea>
          </div>
        </div>
        <div class="flex items-center justify-end gap-2 mt-4">
          <button
            onclick={() => showCreateDialog = false}
            class="px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200 transition-colors"
          >
            Cancel
          </button>
          <button
            onclick={handleCreate}
            disabled={!newName.trim() || creating}
            class="px-3 py-1.5 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-500 disabled:bg-gray-700 disabled:text-gray-500 transition-colors"
          >
            {creating ? 'Creating...' : 'Create'}
          </button>
        </div>
      </div>
    </div>
  {/if}

  <!-- Graph list -->
  {#if graph.loading}
    <div class="text-center py-12 text-gray-500 text-sm">Loading graphs...</div>
  {:else if graph.graphs.length === 0}
    <div class="flex flex-col items-center justify-center py-16 text-center">
      <svg class="w-16 h-16 text-gray-800 mb-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
        <circle cx="5" cy="6" r="3"/>
        <circle cx="19" cy="6" r="3"/>
        <circle cx="12" cy="18" r="3"/>
        <line x1="8" y1="6" x2="16" y2="6"/>
        <line x1="7" y1="8" x2="10" y2="16"/>
        <line x1="17" y1="8" x2="14" y2="16"/>
      </svg>
      <h3 class="text-gray-400 font-medium mb-1">No pipelines yet</h3>
      <p class="text-gray-600 text-sm">Create your first data ingestion pipeline to get started</p>
    </div>
  {:else}
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {#each graph.graphs as g (g.id)}
        <a
          href="/graphs/{g.id}"
          class="block bg-gray-900 border border-gray-800 rounded-xl p-4 hover:border-gray-700 hover:bg-gray-900/80 transition-all group"
        >
          <div class="flex items-start justify-between">
            <div class="flex-1 min-w-0">
              <h3 class="text-sm font-medium text-gray-200 group-hover:text-white truncate">{g.name}</h3>
              {#if g.description}
                <p class="text-xs text-gray-500 mt-1 line-clamp-2">{g.description}</p>
              {/if}
            </div>
            <span class="text-xs px-1.5 py-0.5 rounded border shrink-0 ml-2 {getStatusBadge(g.status)}">
              {g.status}
            </span>
          </div>

          <div class="flex items-center gap-3 mt-3 text-xs text-gray-600">
            <span>Created {formatTime(g.created_at)}</span>
            <span>Updated {formatTime(g.updated_at)}</span>
          </div>

          {#if g.tags && g.tags.length > 0}
            <div class="flex items-center gap-1 mt-2 flex-wrap">
              {#each g.tags as tag}
                <span class="text-[10px] px-1.5 py-0.5 rounded bg-gray-800 text-gray-500">{tag}</span>
              {/each}
            </div>
          {/if}
        </a>
      {/each}
    </div>
  {/if}
</div>
