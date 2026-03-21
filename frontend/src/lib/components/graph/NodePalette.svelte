<script lang="ts">
  import { nodeRegistry } from '$lib/stores';
  import { onMount } from 'svelte';
  import type { NodeTypeDef, NodeCategory } from '$lib/types';

  interface CategoryGroup {
    id: NodeCategory;
    label: string;
    icon: string;
    color: string;
    nodes: NodeTypeDef[];
  }

  let collapsed = $state<Record<string, boolean>>({});

  let groups = $derived.by(() => {
    const all = nodeRegistry.nodeTypes;
    const categories: CategoryGroup[] = [
      { id: 'source', label: 'Sources', icon: 'M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z', color: 'blue', nodes: [] },
      { id: 'processing', label: 'Processing', icon: 'M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z', color: 'green', nodes: [] },
      { id: 'ai', label: 'AI / LLM', icon: 'M12 2a4 4 0 014 4v2H8V6a4 4 0 014-4zM6 14a6 6 0 0112 0v4a2 2 0 01-2 2H8a2 2 0 01-2-2v-4z', color: 'purple', nodes: [] },
      { id: 'output', label: 'Outputs', icon: 'M9 3H5a2 2 0 00-2 2v4m6-6h10a2 2 0 012 2v4M9 3v18m0 0h10a2 2 0 002-2V9M9 21H5a2 2 0 01-2-2V9m0 0h18', color: 'orange', nodes: [] }
    ];

    for (const node of all) {
      const group = categories.find(c => c.id === node.category);
      if (group) {
        group.nodes.push(node);
      }
    }

    return categories.filter(g => g.nodes.length > 0);
  });

  function toggleGroup(id: string) {
    collapsed[id] = !collapsed[id];
  }

  function getColorClasses(color: string): string {
    switch (color) {
      case 'blue': return 'bg-blue-500/10 border-blue-500/20 text-blue-400 hover:bg-blue-500/20';
      case 'green': return 'bg-green-500/10 border-green-500/20 text-green-400 hover:bg-green-500/20';
      case 'purple': return 'bg-purple-500/10 border-purple-500/20 text-purple-400 hover:bg-purple-500/20';
      case 'orange': return 'bg-orange-500/10 border-orange-500/20 text-orange-400 hover:bg-orange-500/20';
      default: return 'bg-gray-500/10 border-gray-500/20 text-gray-400 hover:bg-gray-500/20';
    }
  }

  function getDotColor(color: string): string {
    switch (color) {
      case 'blue': return 'bg-blue-500';
      case 'green': return 'bg-green-500';
      case 'purple': return 'bg-purple-500';
      case 'orange': return 'bg-orange-500';
      default: return 'bg-gray-500';
    }
  }

  // Drag start handler
  function handleDragStart(event: DragEvent, nodeType: NodeTypeDef) {
    if (event.dataTransfer) {
      event.dataTransfer.setData('application/ingestion-node', JSON.stringify(nodeType));
      event.dataTransfer.effectAllowed = 'move';
    }
  }
</script>

<aside class="w-56 bg-gray-900 border-r border-gray-800 flex flex-col overflow-hidden shrink-0">
  <div class="px-3 py-2 border-b border-gray-800">
    <h2 class="text-xs font-medium text-gray-400 uppercase tracking-wider">Nodes</h2>
  </div>

  <div class="flex-1 overflow-auto p-2 space-y-1">
    {#each groups as group (group.id)}
      <div>
        <button
          onclick={() => toggleGroup(group.id)}
          class="w-full flex items-center gap-2 px-2 py-1.5 text-xs font-medium text-gray-400 hover:text-gray-200 transition-colors"
        >
          <svg class="w-3 h-3 transition-transform {collapsed[group.id] ? '' : 'rotate-90'}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <polyline points="9 18 15 12 9 6"/>
          </svg>
          <span>{group.label}</span>
          <span class="text-gray-600 ml-auto">{group.nodes.length}</span>
        </button>

        {#if !collapsed[group.id]}
          <div class="ml-1 space-y-0.5">
            {#each group.nodes as node (node.type)}
              <!-- svelte-ignore a11y_no_static_element_interactions a11y_interactive_supports_focus -->
              <div
                draggable="true"
                role="button"
                tabindex="0"
                ondragstart={(e) => handleDragStart(e, node)}
                class="flex items-center gap-2 px-2 py-1.5 rounded-md border text-xs cursor-grab active:cursor-grabbing transition-colors {getColorClasses(group.color)}"
                title={node.description}
              >
                <div class="w-1.5 h-1.5 rounded-full {getDotColor(group.color)} shrink-0"></div>
                <span class="truncate">{node.display_name}</span>
              </div>
            {/each}
          </div>
        {/if}
      </div>
    {/each}

    {#if groups.length === 0 && !nodeRegistry.loading}
      <div class="text-xs text-gray-600 text-center py-4">No node types loaded</div>
    {/if}

    {#if nodeRegistry.loading}
      <div class="text-xs text-gray-600 text-center py-4">Loading nodes...</div>
    {/if}
  </div>
</aside>
