/**
 * Graph store — current graph nodes/edges, selected node.
 * Uses Svelte 5 runes for reactivity.
 */

import type { GraphNode, GraphEdge, GraphDetail, Graph } from '$lib/types';
import { graphService } from '$lib/services/graphService.js';
import { nodeRegistry } from './nodeRegistry.svelte.js';

class GraphState {
  // Current graph metadata
  currentGraph = $state<GraphDetail | null>(null);
  graphs = $state<Graph[]>([]);
  totalGraphs = $state(0);

  // Svelte Flow data
  nodes = $state<GraphNode[]>([]);
  edges = $state<GraphEdge[]>([]);

  // Selection
  selectedNodeId = $state<string | null>(null);

  // UI state
  loading = $state(false);
  saving = $state(false);
  error = $state<string | null>(null);

  get selectedNode(): GraphNode | null {
    if (!this.selectedNodeId) return null;
    return this.nodes.find(n => n.id === this.selectedNodeId) ?? null;
  }

  async listGraphs(): Promise<void> {
    this.loading = true;
    try {
      const response = await graphService.listGraphs();
      this.graphs = response.graphs;
      this.totalGraphs = response.total;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to load graphs';
    } finally {
      this.loading = false;
    }
  }

  async loadGraph(id: string): Promise<void> {
    this.loading = true;
    this.error = null;
    try {
      const detail = await graphService.getGraph(id);
      this.currentGraph = detail;

      // Restore nodes/edges from latest version (deduplicate by id to guard against backend duplicates)
      if (detail.latest_version) {
        const v = detail.latest_version;
        const rawNodes = (v.nodes_data?.nodes as GraphNode[]) ?? [];
        const seen = new Set<string>();
        const deduped: GraphNode[] = [];
        for (const n of rawNodes) {
          if (!seen.has(n.id)) {
            seen.add(n.id);
            const config = (v.node_configs?.[n.id] as Record<string, unknown>) ?? {};
            deduped.push({ ...n, data: { ...n.data, config } });
          }
        }
        this.nodes = deduped;
        this.edges = (v.edges_data?.edges as GraphEdge[]) ?? [];
      } else {
        this.nodes = [];
        this.edges = [];
      }
      this.selectedNodeId = null;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to load graph';
    } finally {
      this.loading = false;
    }
  }

  async createGraph(name: string, description?: string, templateId?: string): Promise<Graph | null> {
    try {
      const graph = await graphService.createGraph({
        name,
        description,
        template_id: templateId
      });
      this.graphs.unshift(graph);
      this.totalGraphs += 1;
      return graph;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to create graph';
      return null;
    }
  }

  setNodes(nodes: GraphNode[]): void {
    this.nodes = nodes;
  }

  setEdges(edges: GraphEdge[]): void {
    this.edges = edges;
  }

  addNode(node: GraphNode): void {
    this.nodes = [...this.nodes, node];
  }

  removeNode(nodeId: string): void {
    this.nodes = this.nodes.filter(n => n.id !== nodeId);
    this.edges = this.edges.filter(e => e.source !== nodeId && e.target !== nodeId);
    if (this.selectedNodeId === nodeId) {
      this.selectedNodeId = null;
    }
  }

  updateNodeData(nodeId: string, data: Partial<GraphNode['data']>): void {
    this.nodes = this.nodes.map(n =>
      n.id === nodeId ? { ...n, data: { ...n.data, ...data } } : n
    );
  }

  addEdge(edge: GraphEdge): void {
    // Prevent duplicate edges
    const exists = this.edges.some(
      e => e.source === edge.source && e.target === edge.target &&
           e.sourceHandle === edge.sourceHandle && e.targetHandle === edge.targetHandle
    );
    if (!exists) {
      this.edges = [...this.edges, edge];
    }
  }

  removeEdge(edgeId: string): void {
    this.edges = this.edges.filter(e => e.id !== edgeId);
  }

  selectNode(nodeId: string | null): void {
    this.selectedNodeId = nodeId;
  }

  async saveVersion(graphId: string): Promise<boolean> {
    this.saving = true;
    this.error = null;
    try {
      // Build node configs from node data
      const nodeConfigs: Record<string, Record<string, unknown>> = {};
      for (const node of this.nodes) {
        const definition = nodeRegistry.getNodeByType(node.type);
        const allowed = new Set(Object.keys(definition?.config_schema.properties ?? {}));
        nodeConfigs[node.id] = Object.fromEntries(
          Object.entries(node.data.config || {}).filter(([key]) => allowed.has(key))
        );
      }

      const nodesWithoutConfigs = this.nodes.map(node => ({
        ...node,
        data: { ...node.data, config: {} }
      }));
      await graphService.saveVersion(graphId, {
        nodes_data: { nodes: nodesWithoutConfigs },
        edges_data: { edges: this.edges },
        node_configs: nodeConfigs
      });
      return true;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to save graph';
      return false;
    } finally {
      this.saving = false;
    }
  }

  clear(): void {
    this.currentGraph = null;
    this.graphs = [];
    this.totalGraphs = 0;
    this.nodes = [];
    this.edges = [];
    this.selectedNodeId = null;
    this.loading = false;
    this.saving = false;
    this.error = null;
  }
}

export const graph = new GraphState();
