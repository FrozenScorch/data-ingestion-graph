/**
 * Node registry service.
 */

import { api } from './api.js';
import type { NodeRegistryResponse, NodeValidateResponse, NodeValidateRequest } from '$lib/types';

export const nodeRegistryService = {
  /** Fetch all available node types */
  async getNodeTypes(): Promise<NodeRegistryResponse> {
    return api.get<NodeRegistryResponse>('/nodes/types');
  },

  /** Validate node configuration */
  async validateConfig(nodeType: string, data: NodeValidateRequest): Promise<NodeValidateResponse> {
    return api.post<NodeValidateResponse>(`/nodes/${nodeType}/validate`, data);
  }
};
