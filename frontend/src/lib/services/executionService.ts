/**
 * Execution (run) service.
 */

import { api } from './api.js';
import type {
  Run,
  RunDetail,
  RunListResponse,
  RunCreate
} from '$lib/types';

export interface QueryHit {
  score: number | null;
  envelope: Record<string, unknown>;
}

export interface RunQueryResponse {
  run_id: string;
  query: string | null;
  count: number;
  hits: QueryHit[];
}

export const executionService = {
  /** Start a run on a graph */
  async startRun(graphId: string, data?: RunCreate): Promise<Run> {
    return api.post<Run>(`/graphs/${graphId}/run`, data || {});
  },

  /** Get a single run detail with node statuses */
  async getRun(runId: string): Promise<RunDetail> {
    return api.get<RunDetail>(`/executions/${runId}`);
  },

  /** List runs for a graph */
  async listRuns(graphId: string): Promise<RunListResponse> {
    const params = new URLSearchParams({ graph_id: graphId });
    return api.get<RunListResponse>(`/executions?${params.toString()}`);
  },

  /** List all executions across graphs */
  async listAllExecutions(): Promise<RunListResponse> {
    return api.get<RunListResponse>('/executions');
  },

  /** Cancel a running execution */
  async cancelRun(runId: string): Promise<Run> {
    return api.post<Run>(`/executions/${runId}/cancel`);
  },

  /** Search output materialized by the SDK Queryable Test Store node. */
  async queryRun(runId: string, query = ''): Promise<RunQueryResponse> {
    const params = new URLSearchParams();
    if (query.trim()) params.set('q', query.trim());
    return api.get<RunQueryResponse>(`/executions/${runId}/query?${params.toString()}`);
  }
};
