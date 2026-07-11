<script lang="ts">
  import { page } from '$app/stores';
  import { onMount } from 'svelte';
  import { graph, execution, nodeRegistry } from '$lib/stores';
  import GraphCanvas from '$lib/components/graph/GraphCanvas.svelte';
  import NodePalette from '$lib/components/graph/NodePalette.svelte';
  import GraphToolbar from '$lib/components/graph/GraphToolbar.svelte';
  import NodeConfigPanel from '$lib/components/ui/NodeConfigPanel.svelte';
  import RunPanel from '$lib/components/ui/RunPanel.svelte';

  let showConfigPanel = $state(false);
  let graphId = $derived($page.params.id ?? '');

  onMount(() => {
    void Promise.all([
      nodeRegistry.loadNodeTypes(),
      graph.loadGraph(graphId)
    ]);

    // Cleanup WebSocket and execution state on navigation away
    return () => {
      execution.disconnectWs();
    };
  });

  function handleNodeSelect(nodeId: string | null) {
    showConfigPanel = !!nodeId;
  }

  function handleCloseConfig() {
    showConfigPanel = false;
    graph.selectNode(null);
  }

  function handleDeleteSelected() {
    if (graph.selectedNodeId) {
      graph.removeNode(graph.selectedNodeId);
      showConfigPanel = false;
    }
  }

  // Keyboard shortcut for save and delete
  function handleKeydown(e: KeyboardEvent) {
    // Don't handle shortcuts when typing in an input/textarea/select
    const tag = (e.target as HTMLElement).tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;

    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
      e.preventDefault();
      graph.saveVersion(graphId);
    }

    if (e.key === 'Delete' || e.key === 'Backspace') {
      e.preventDefault();
      handleDeleteSelected();
    }
  }
</script>

<svelte:window onkeydown={handleKeydown} />

{#if graph.loading}
  <div class="flex items-center justify-center h-full">
    <div class="text-gray-500 text-sm">Loading graph...</div>
  </div>
{:else if graph.currentGraph}
  <div class="h-full flex flex-col">
    <!-- Toolbar -->
    <GraphToolbar {graphId} />

    <!-- Main content area -->
    <div class="flex-1 flex overflow-hidden">
      <!-- Node Palette (left sidebar) -->
      <NodePalette />

      <!-- Canvas -->
      <GraphCanvas onNodeSelect={handleNodeSelect} />

      <!-- Config Panel (right sidebar, conditionally shown) -->
      {#if showConfigPanel}
        <div class="w-72 overflow-hidden shrink-0">
          <NodeConfigPanel onClose={handleCloseConfig} onDelete={handleDeleteSelected} />
        </div>
      {/if}
    </div>

    <!-- Run Panel (bottom) -->
    <RunPanel />
  </div>
{:else}
  <div class="flex items-center justify-center h-full">
    <div class="text-center">
      <p class="text-gray-500 text-sm">Graph not found</p>
      <a href="/graphs" class="text-indigo-400 text-sm hover:text-indigo-300 mt-2 inline-block">Back to graphs</a>
    </div>
  </div>
{/if}
