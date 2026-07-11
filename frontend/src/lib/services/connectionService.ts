import { api } from './api.js';
import type { ConnectionTypeDefinition, SavedConnection } from '$lib/types/connection.js';

export const connectionService = {
  list(type?: string): Promise<{ connections: SavedConnection[]; total: number }> {
    return api.get('/connections', type ? { type } : undefined);
  },

  types(): Promise<{ types: ConnectionTypeDefinition[] }> {
    return api.get('/connections/types');
  },

  create(input: { name: string; type: string; config: Record<string, unknown> }): Promise<SavedConnection> {
    return api.post('/connections', input);
  },

  test(id: string): Promise<{ success: boolean; message: string }> {
    return api.post(`/connections/${id}/test`);
  },

  delete(id: string): Promise<void> {
    return api.delete(`/connections/${id}`);
  }
};
