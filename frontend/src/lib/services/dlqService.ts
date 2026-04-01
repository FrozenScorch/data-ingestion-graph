/**
 * Dead Letter Queue service.
 */

import { api } from './api.js';

export interface DlqItem {
  id: string;
  run_id: string | null;
  node_id: string | null;
  node_type: string | null;
  error_type: string | null;
  error_message: string | null;
  input_data: unknown;
  retry_count: number;
  resolved: boolean;
  resolution_note: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface DlqListResponse {
  items: DlqItem[];
  total: number;
  offset: number;
  limit: number;
}

export interface DlqRetryResponse {
  success: boolean;
  message: string;
  items_processed?: number;
  item?: {
    id: string;
    resolved: boolean;
    resolution_note: string | null;
  };
}

export interface DlqResolveResponse {
  id: string;
  resolved: boolean;
  resolution_note: string | null;
  updated_at: string | null;
}

export const dlqService = {
  /** List DLQ items with optional filtering */
  async listItems(params?: { resolved?: boolean; node_type?: string; offset?: number; limit?: number }): Promise<DlqListResponse> {
    return api.get<DlqListResponse>('/dead-letter', params as Record<string, string>);
  },

  /** Retry a failed DLQ item */
  async retryItem(itemId: string): Promise<DlqRetryResponse> {
    return api.post<DlqRetryResponse>(`/dead-letter/${itemId}/retry`);
  },

  /** Mark a DLQ item as resolved with a note */
  async resolveItem(itemId: string, note: string): Promise<DlqResolveResponse> {
    return api.post<DlqResolveResponse>(`/dead-letter/${itemId}/resolve`, { note });
  },

  /** Delete a DLQ item */
  async deleteItem(itemId: string): Promise<void> {
    return api.delete(`/dead-letter/${itemId}`);
  }
};
