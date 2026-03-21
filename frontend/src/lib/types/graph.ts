/**
 * Graph types matching Svelte Flow and backend API.
 */

// --- Svelte Flow compatible Node/Edge ---

export interface GraphNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: {
    label: string;
    config: Record<string, unknown>;
    [key: string]: unknown;
  };
  width?: number;
  height?: number;
}

export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
  targetHandle?: string;
  type?: string;
  animated?: boolean;
  style?: Record<string, unknown>;
  data?: {
    sourcePort?: string;
    targetPort?: string;
    dataType?: string;
  };
}

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
