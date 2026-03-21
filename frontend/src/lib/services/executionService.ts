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

export const executionService = {
  /** Start a run on a graph */
  async startRun(graphId: string, data?: RunCreate): Promise<Run> {
    return api.post<Run>(`/api/graphs/${graphId}/run`, data || {});
  },

  /** Get a single run detail with node statuses */
  async getRun(runId: string): Promise<RunDetail> {
    return api.get<RunDetail>(`/api/executions/${runId}`);
  },

  /** List runs for a graph */
  async listRuns(graphId: string): Promise<RunListResponse> {
    return api.get<RunListResponse>(`/api/graphs/${graphId}/runs`);
  },

  /** List all executions across graphs */
  async listAllExecutions(): Promise<RunListResponse> {
    return api.get<RunListResponse>('/api/executions');
  },

  /** Cancel a running execution */
  async cancelRun(runId: string): Promise<Run> {
    return api.post<Run>(`/api/executions/${runId}/cancel`);
  }
};
