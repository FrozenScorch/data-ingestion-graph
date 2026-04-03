<script lang="ts">
  import { SvelteFlow, Background, Controls, MiniMap, applyNodeChanges, applyEdgeChanges } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';
  import { graph, execution, nodeRegistry } from '$lib/stores';
  import { nodeTypes } from '$lib/components/nodes/nodeTypeRegistry.js';
  import type { Connection, Node, Edge, NodeChange, EdgeChange, NodeTypeDef } from '$lib/types';
  import { onMount } from 'svelte';

  let {
    onNodeSelect
  }: { onNodeSelect: (nodeId: string | null) => void } = $props();

  // Debounce timer for auto-save
  let autoSaveTimer: ReturnType<typeof setTimeout> | null = null;
  const AUTO_SAVE_DELAY = 2000;

  function triggerAutoSave() {
    // Only auto-save if the graph has been saved at least once (has a graph ID)
    if (!graph.currentGraph?.id) return;

    if (autoSaveTimer) clearTimeout(autoSaveTimer);
    autoSaveTimer = setTimeout(() => {
      graph.saveVersion(graph.currentGraph!.id);
      autoSaveTimer = null;
    }, AUTO_SAVE_DELAY);
  }

  let nodeIdCounter = $state(0);

  function generateNodeId(type: string): string {
    nodeIdCounter += 1;
    return `${type}_${Date.now()}_${nodeIdCounter}`;
  }

  // Handle new connections (dragging from one handle to another)
  function handleConnect(connection: Connection) {
    const newEdge: Edge = {
      id: `e-${connection.source}-${connection.sourceHandle || 'default'}-${connection.target}-${connection.targetHandle || 'default'}`,
      source: connection.source,
      target: connection.target,
      sourceHandle: connection.sourceHandle || undefined,
      targetHandle: connection.targetHandle || undefined
    };
    graph.addEdge(newEdge);
  }

  // Handle node click
  // @xyflow/svelte v1.x passes a single { node, event } object, not (event, node)
  function handleNodeClick({ node }: { node: Node; event: MouseEvent | TouchEvent }) {
    console.log('[GraphCanvas] node clicked:', node.id);
    graph.selectNode(node.id);
    onNodeSelect(node.id);
  }

  // Handle pane click (deselect)
  function handlePaneClick() {
    graph.selectNode(null);
    onNodeSelect(null);
  }

  // Handle node changes (position, selection, removal, etc.)
  function handleNodesChange(changes: NodeChange[]) {
    const updated = applyNodeChanges(changes, graph.nodes);
    graph.setNodes(updated);
    triggerAutoSave();
  }

  // Handle edge changes (selection, removal, etc.)
  function handleEdgesChange(changes: EdgeChange[]) {
    const updated = applyEdgeChanges(changes, graph.edges);
    graph.setEdges(updated);
    triggerAutoSave();
  }

  // Handle drag over (from palette)
  function handleDragOver(event: DragEvent) {
    event.preventDefault();
    if (event.dataTransfer) {
      event.dataTransfer.dropEffect = 'move';
    }
  }

  // Handle drop (from palette)
  function handleDrop(event: DragEvent) {
    event.preventDefault();
    const data = event.dataTransfer?.getData('application/ingestion-node');
    if (!data) return;

    const nodeTypeStr: NodeTypeDef = JSON.parse(data);
    const bounds = (event.currentTarget as HTMLElement).getBoundingClientRect();

    // Calculate position relative to the canvas, accounting for zoom and pan
    const position = {
      x: event.clientX - bounds.left - 90,
      y: event.clientY - bounds.top - 25
    };

    const newId = generateNodeId(nodeTypeStr.type);

    graph.addNode({
      id: newId,
      type: nodeTypeStr.type,
      position,
      data: {
        label: nodeTypeStr.display_name,
        config: {},
        category: nodeTypeStr.category,
        inputs: nodeTypeStr.inputs,
        outputs: nodeTypeStr.outputs
      }
    });
  }

  // Validate connection types
  function isValidConnection(connection: Connection): boolean {
    // Prevent self-connections (connecting a node to itself)
    if (connection.source === connection.target) {
      return false;
    }

    // Prevent duplicate connections (same source+target handles already exists)
    const isDuplicate = graph.edges.some(
      (e) =>
        e.source === connection.source &&
        e.target === connection.target &&
        e.sourceHandle === (connection.sourceHandle || undefined) &&
        e.targetHandle === (connection.targetHandle || undefined)
    );
    if (isDuplicate) {
      return false;
    }

    return true;
  }

  // Default edge options for smooth bezier curves with consistent styling
  const defaultEdgeOptions = {
    type: 'smoothstep',
    animated: false,
    style: { stroke: '#6366f1', strokeWidth: 2 },
    markerEnd: { type: 'arrowclosed' as const, width: 15, height: 15, color: '#6366f1' }
  };

  // Handle nodes delete
  function handleDelete({ nodes }: { nodes: Node[] }) {
    for (const node of nodes) {
      graph.removeNode(node.id);
    }
  }
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div class="flex-1 relative" role="region" ondragover={handleDragOver} ondrop={handleDrop}>
  <SvelteFlow
    nodes={graph.nodes}
    edges={graph.edges}
    {nodeTypes}
    {defaultEdgeOptions}
    onconnect={handleConnect}
    onnodeschange={handleNodesChange}
    onedgeschange={handleEdgesChange}
    onnodeclick={handleNodeClick}
    onpaneclick={handlePaneClick}
    ondelete={handleDelete}
    {isValidConnection}
    fitView
    snapGrid={[15, 15]}
    minZoom={0.1}
    maxZoom={2}
    colorMode="dark"
    class="bg-gray-950"
    proOptions={{ hideAttribution: true }}
    connectionLineType="bezier"
    connectionLineStyle="stroke: #6366f1; stroke-width: 2; stroke-dasharray: 5;"
  >
    <Background
      gap={20}
      size={1}
      color="#1a1a2e"
    />
    <Controls
      position="bottom-right"
    />
    <MiniMap
      position="bottom-left"
      nodeColor={(node: Node) => {
        const cat = node.data?.category;
        switch (cat) {
          case 'source': return '#3b82f6';
          case 'processing': return '#22c55e';
          case 'ai': return '#a855f7';
          case 'output': return '#f97316';
          default: return '#6366f1';
        }
      }}
      maskColor="rgba(0, 0, 0, 0.7)"
      style="width: 150px; height: 100px;"
    />
  </SvelteFlow>
</div>
