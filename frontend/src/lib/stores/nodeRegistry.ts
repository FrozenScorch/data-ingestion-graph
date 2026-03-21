/**
 * Node registry store — available node types.
 * Uses Svelte 5 runes for reactivity.
 */

import type { NodeTypeDef, NodeCategory } from '$lib/types';
import { nodeRegistryService } from '$lib/services/nodeRegistryService.js';

class NodeRegistryState {
  nodeTypes = $state<NodeTypeDef[]>([]);
  total = $state(0);
  loading = $state(false);
  error = $state<string | null>(null);
  loaded = $state(false);

  getNodeByType(type: string): NodeTypeDef | undefined {
    return this.nodeTypes.find(n => n.type === type);
  }

  getNodesByCategory(category: NodeCategory): NodeTypeDef[] {
    return this.nodeTypes.filter(n => n.category === category);
  }

  async loadNodeTypes(): Promise<void> {
    if (this.loaded) return;
    this.loading = true;
    try {
      const response = await nodeRegistryService.getNodeTypes();
      this.nodeTypes = response.nodes;
      this.total = response.total;
      this.loaded = true;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to load node types';
    } finally {
      this.loading = false;
    }
  }
}

export const nodeRegistry = new NodeRegistryState();
