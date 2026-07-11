/**
 * Graph types matching Svelte Flow and backend API.
 */

import type { Edge, Node } from '@xyflow/svelte';

// --- Svelte Flow compatible Node/Edge ---

export interface GraphNodeData extends Record<string, unknown> {
  label: string;
  config: Record<string, unknown>;
  implementation?: 'studio' | 'sdk-adapter';
  sdk_component?: string | null;
}

export type GraphNode = Node<GraphNodeData, string> & { type: string };

export interface GraphEdgeData extends Record<string, unknown> {
  sourcePort?: string;
  targetPort?: string;
  dataType?: string;
}

export type GraphEdge = Edge<GraphEdgeData> & {
  source_port?: string;
  target_port?: string;
};

// --- Backend API types ---

export interface Graph {
  id: string;
  name: string;
  description: string | null;
  owner_id: string;
  status: 'draft' | 'active' | 'archived';
  tags: string[] | null;
  created_at: string;
  updated_at: string;
}

export interface GraphVersion {
  id: string;
  graph_id: string;
  version_number: number;
  nodes_data: Record<string, unknown> | null;
  edges_data: Record<string, unknown> | null;
  node_configs: Record<string, unknown> | null;
  checksum: string | null;
  created_at: string;
}

export interface GraphDetail extends Graph {
  latest_version: GraphVersion | null;
}

export interface GraphCreate {
  name: string;
  description?: string;
  tags?: string[];
  template_id?: string;
}

export interface GraphUpdate {
  name?: string;
  description?: string;
  status?: 'draft' | 'active' | 'archived';
  tags?: string[];
}

export interface GraphVersionSave {
  nodes_data: Record<string, unknown>;
  edges_data: Record<string, unknown>;
  node_configs: Record<string, unknown>;
}

export interface GraphListResponse {
  graphs: Graph[];
  total: number;
}

export interface ConnectionResponse {
  id: string;
  user_id: string;
  name: string;
  type: 'postgres' | 'discord' | 'github' | 'webhook';
  config: Record<string, unknown> | null;
  is_valid: boolean;
  created_at: string;
}
