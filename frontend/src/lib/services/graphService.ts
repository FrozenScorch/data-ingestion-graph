/**
 * Graph CRUD and version save service.
 */

import { api } from './api.js';
import type {
  Graph,
  GraphDetail,
  GraphListResponse,
  GraphCreate,
  GraphUpdate,
  GraphVersionSave,
  GraphVersion
} from '$lib/types';

export const graphService = {
  /** List all graphs */
  async listGraphs(): Promise<GraphListResponse> {
    return api.get<GraphListResponse>('/api/graphs');
  },

  /** Get single graph with latest version */
  async getGraph(id: string): Promise<GraphDetail> {
    return api.get<GraphDetail>(`/api/graphs/${id}`);
  },

  /** Create a new graph */
  async createGraph(data: GraphCreate): Promise<Graph> {
    return api.post<Graph>('/api/graphs', data);
  },

  /** Update graph metadata */
  async updateGraph(id: string, data: GraphUpdate): Promise<Graph> {
    return api.patch<Graph>(`/api/graphs/${id}`, data);
  },

  /** Delete a graph */
  async deleteGraph(id: string): Promise<void> {
    return api.delete(`/api/graphs/${id}`);
  },

  /** Save a new version (nodes, edges, configs) */
  async saveVersion(id: string, data: GraphVersionSave): Promise<GraphVersion> {
    return api.post<GraphVersion>(`/api/graphs/${id}/save`, data);
  }
};
