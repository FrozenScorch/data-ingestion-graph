<script lang="ts">
  import {
    SvelteFlow,
    Background,
    Controls,
    MiniMap,
    ConnectionLineType,
    MarkerType,
    type Connection,
    type DefaultEdgeOptions,
    type IsValidConnection,
    type Node,
    type NodeTypes
  } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';
  import { graph, nodeRegistry } from '$lib/stores';
  import PipelineNode from '$lib/components/nodes/PipelineNode.svelte';
  import type { GraphNode, GraphEdge, NodeTypeDef } from '$lib/types';

  let {
    onNodeSelect
  }: { onNodeSelect: (nodeId: string | null) => void } = $props();

  // Debounce timer for auto-save
  let autoSaveTimer: ReturnType<typeof setTimeout> | null = null;
  const AUTO_SAVE_DELAY = 2000;

  let dynamicNodeTypes = $derived.by(() => {
    const names = new Set([
      ...nodeRegistry.nodeTypes.map(node => node.type),
      ...graph.nodes.map(node => node.type)
    ]);
    return Object.fromEntries([...names].map(type => [type, PipelineNode])) as NodeTypes;
  });

  function triggerAutoSave() {
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
    const newEdge: GraphEdge = {
      id: `e-${connection.source}-${connection.sourceHandle || 'default'}-${connection.target}-${connection.targetHandle || 'default'}`,
      source: connection.source,
      target: connection.target,
      sourceHandle: connection.sourceHandle || undefined,
      targetHandle: connection.targetHandle || undefined,
      source_port: connection.sourceHandle || 'output',
      target_port: connection.targetHandle || 'input'
    };
    graph.addEdge(newEdge);
    triggerAutoSave();
  }

  // Handle node click
  // @xyflow/svelte v1.x passes a single { node, event } object, not (event, node)
  function handleNodeClick({ node }: { node: GraphNode; event: MouseEvent | TouchEvent }) {
    console.log('[GraphCanvas] node clicked:', node.id);
    graph.selectNode(node.id);
    onNodeSelect(node.id);
  }

  // Handle pane click (deselect)
  function handlePaneClick() {
    graph.selectNode(null);
    onNodeSelect(null);
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

    const defaultConfig = Object.fromEntries(
      Object.entries(nodeTypeStr.config_schema.properties)
        .filter(([, field]) => field.default !== undefined)
        .map(([key, field]) => [key, field.default])
    );

    graph.addNode({
      id: newId,
      type: nodeTypeStr.type,
      position,
      data: {
        label: nodeTypeStr.display_name,
        config: defaultConfig,
        category: nodeTypeStr.category,
        inputs: nodeTypeStr.inputs,
        outputs: nodeTypeStr.outputs,
        implementation: nodeTypeStr.implementation,
        sdk_component: nodeTypeStr.sdk_component
      }
    });
    triggerAutoSave();
  }

  // Validate connection types
  const isValidConnection: IsValidConnection = (connection) => {
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

    const sourceNode = graph.nodes.find(node => node.id === connection.source);
    const targetNode = graph.nodes.find(node => node.id === connection.target);
    const sourceDefinition = sourceNode ? nodeRegistry.getNodeByType(sourceNode.type) : undefined;
    const targetDefinition = targetNode ? nodeRegistry.getNodeByType(targetNode.type) : undefined;
    const sourcePort = sourceDefinition?.outputs.find(
      port => port.name === (connection.sourceHandle || 'output')
    );
    const targetPort = targetDefinition?.inputs.find(
      port => port.name === (connection.targetHandle || 'input')
    );
    if (!sourcePort || !targetPort) {
      return false;
    }

    if (targetPort.data_type !== 'any' && sourcePort.data_type !== targetPort.data_type) {
      return false;
    }

    return true;
  };

  // Default edge options for smooth bezier curves with consistent styling
  const defaultEdgeOptions: DefaultEdgeOptions = {
    type: 'smoothstep',
    animated: false,
    style: 'stroke: #6366f1; stroke-width: 2;',
    markerEnd: { type: MarkerType.ArrowClosed, width: 15, height: 15, color: '#6366f1' }
  };

  // Handle nodes delete
  function handleDelete({ nodes }: { nodes: GraphNode[] }) {
    for (const node of nodes) {
      graph.removeNode(node.id);
    }
    triggerAutoSave();
  }
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div class="flex-1 relative" role="region" ondragover={handleDragOver} ondrop={handleDrop}>
  <SvelteFlow
    bind:nodes={graph.nodes}
    bind:edges={graph.edges}
    nodeTypes={dynamicNodeTypes}
    {defaultEdgeOptions}
    onconnect={handleConnect}
    onnodeclick={handleNodeClick}
    onnodedragstop={triggerAutoSave}
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
    connectionLineType={ConnectionLineType.Bezier}
    connectionLineStyle="stroke: #6366f1; stroke-width: 2; stroke-dasharray: 5;"
  >
    <Background
      gap={20}
      size={1}
      patternColor="#1a1a2e"
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
