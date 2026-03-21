<script lang="ts">
  import type { RunNode } from '$lib/types';

  let {
    nodes = [],
    edges = []
  } = $props();

  interface LineageItem {
    node: RunNode;
    children: LineageItem[];
  }

  function buildLineage(): LineageItem[] {
    const nodeMap = new Map<string, RunNode>();
    for (const node of nodes) {
      nodeMap.set(node.node_id, node);
    }

    // Find root nodes (no incoming edges)
    const targets = new Set(edges.map(e => e.target));
    const rootIds = nodes.filter(n => !targets.has(n.node_id)).map(n => n.node_id);

    function buildTree(nodeId: string, visited: Set<string>): LineageItem | null {
      if (visited.has(nodeId)) return null;
      visited.add(nodeId);

      const node = nodeMap.get(nodeId);
      if (!node) return null;

      const childEdges = edges.filter(e => e.source === nodeId);
      const children = childEdges
        .map(e => buildTree(e.target, new Set(visited)))
        .filter(Boolean) as LineageItem[];

      return { node, children };
    }

    return rootIds
      .map(id => buildTree(id, new Set()))
      .filter(Boolean) as LineageItem[];
  }

  const lineageTree = $derived(buildLineage());

  function getStatusDot(status: string): string {
    switch (status) {
      case 'completed': return 'bg-green-400';
      case 'running': return 'bg-indigo-400 animate-pulse';
      case 'failed': return 'bg-red-400';
      default: return 'bg-gray-500';
    }
  }
</script>

<div class="p-4 space-y-3">
  <h3 class="text-sm font-medium text-gray-300">Data Lineage</h3>

  {#if lineageTree.length === 0}
    <p class="text-xs text-gray-600">No lineage data available for this run</p>
  {:else}
    {#each lineageTree as item, i}
      {@render lineageNode(item, 0)}
    {/each}
  {/if}
</div>

{#snippet lineageNode(item: { node: RunNode; children: any[] }, depth: number)}
  <div class="flex items-start gap-2" style="padding-left: {depth * 16}px">
    <div class="flex items-center gap-1.5 shrink-0">
      <div class="w-2 h-2 rounded-full {getStatusDot(item.node.status)}"></div>
      <span class="text-xs text-gray-300">{item.node.node_type}</span>
      {#if item.node.items_processed}
        <span class="text-xs text-gray-600">({item.node.items_processed} items)</span>
      {/if}
      {#if item.node.duration_ms}
        <span class="text-xs text-gray-600">{item.node.duration_ms}ms</span>
      {/if}
    </div>
  </div>
  {#if item.children.length > 0}
    <div class="ml-4 border-l border-gray-800">
      {#each item.children as child}
        {@render lineageNode(child, depth + 1)}
      {/each}
    </div>
  {/if}
{/snippet}
