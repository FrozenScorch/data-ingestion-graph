/**
 * Execution store — run state, node statuses, live updates.
 * Uses Svelte 5 runes for reactivity.
 */

import type { Run, RunNode, RunDetail, RunListResponse, WsEvent } from '$lib/types';
import { executionService } from '$lib/services/executionService.js';
import { createRunWebSocket } from '$lib/services/wsService.js';

class ExecutionState {
  currentRun = $state<RunDetail | null>(null);
  runs = $state<Run[]>([]);
  totalRuns = $state(0);
  loading = $state(false);
  error = $state<string | null>(null);

  // Live node status map: nodeId -> RunNode
  nodeStatuses = $state<Map<string, RunNode>>(new Map());

  // WebSocket cleanup
  private wsCleanup: { close: () => void } | null = null;

  get isRunning(): boolean {
    return this.currentRun?.run.status === 'running';
  }

  async startRun(graphId: string): Promise<Run | null> {
    this.loading = true;
    this.error = null;
    try {
      const run = await executionService.startRun(graphId);
      this.currentRun = { run, run_nodes: [] };
      this.nodeStatuses = new Map();
      // Connect WebSocket for live updates
      this.connectWs(run.id);
      return run;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to start run';
      return null;
    } finally {
      this.loading = false;
    }
  }

  async getRun(runId: string): Promise<void> {
    this.loading = true;
    this.error = null;
    try {
      const detail = await executionService.getRun(runId);
      this.currentRun = detail;
      // Build node status map
      const statuses = new Map<string, RunNode>();
      for (const rn of detail.run_nodes) {
        statuses.set(rn.node_id, rn);
      }
      this.nodeStatuses = statuses;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to load run';
    } finally {
      this.loading = false;
    }
  }

  async listRuns(graphId: string): Promise<void> {
    this.loading = true;
    try {
      const response: RunListResponse = await executionService.listRuns(graphId);
      this.runs = response.runs;
      this.totalRuns = response.total;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to load runs';
    } finally {
      this.loading = false;
    }
  }

  async listAllExecutions(): Promise<void> {
    this.loading = true;
    try {
      const response: RunListResponse = await executionService.listAllExecutions();
      this.runs = response.runs;
      this.totalRuns = response.total;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to load executions';
    } finally {
      this.loading = false;
    }
  }

  async cancelRun(runId: string): Promise<boolean> {
    try {
      const run = await executionService.cancelRun(runId);
      if (this.currentRun && this.currentRun.run.id === runId) {
        this.currentRun = { ...this.currentRun, run };
      }
      this.disconnectWs();
      return true;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to cancel run';
      return false;
    }
  }

  connectWs(runId: string): void {
    this.disconnectWs();
    this.wsCleanup = createRunWebSocket(
      runId,
      (event: WsEvent) => this.handleWsEvent(event),
      () => {
        console.error('WebSocket error for run', runId);
      },
      () => {
        console.log('WebSocket closed for run', runId);
      }
    );
  }

  disconnectWs(): void {
    this.wsCleanup?.close();
    this.wsCleanup = null;
  }

  private handleWsEvent(event: WsEvent): void {
    const data = event.data;
    switch (event.type) {
      case 'node_started':
      case 'node_completed':
      case 'node_failed':
      case 'node_retried': {
        const nodeId = data.node_id as string;
        const runNode = data as unknown as RunNode;
        const newStatuses = new Map(this.nodeStatuses);
        newStatuses.set(nodeId, runNode);
        this.nodeStatuses = newStatuses;
        break;
      }
      case 'run_completed':
      case 'run_failed':
      case 'run_cancelled': {
        if (this.currentRun) {
          const status = event.type === 'run_completed' ? 'completed'
            : event.type === 'run_failed' ? 'failed'
            : 'cancelled';
          this.currentRun = {
            ...this.currentRun,
            run: { ...this.currentRun.run, status }
          };
        }
        this.disconnectWs();
        break;
      }
    }
  }

  clear(): void {
    this.disconnectWs();
    this.currentRun = null;
    this.runs = [];
    this.nodeStatuses = new Map();
    this.error = null;
  }
}

export const execution = new ExecutionState();
