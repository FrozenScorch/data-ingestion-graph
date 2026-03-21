/**
 * Execution (run) types matching backend API.
 */

export type RunStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
export type TriggerType = 'manual' | 'webhook' | 'schedule';
export type NodeRunStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled' | 'skipped';

export interface Run {
  id: string;
  graph_id: string;
  graph_version_id: string | null;
  trigger_type: TriggerType;
  triggered_by: string | null;
  status: RunStatus;
  error_message: string | null;
  created_at: string;
  updated_at: string;
}

export interface RunNode {
  id: string;
  run_id: string;
  node_id: string;
  node_type: string;
  status: NodeRunStatus;
  attempt_count: number;
  max_retries: number;
  input_data: Record<string, unknown> | null;
  output_data: Record<string, unknown> | null;
  items_processed: number | null;
  duration_ms: number | null;
  error_message: string | null;
  created_at: string;
}

export interface RunDetail {
  run: Run;
  run_nodes: RunNode[];
}

export interface RunListResponse {
  runs: Run[];
  total: number;
}

export interface RunCreate {
  trigger_type?: TriggerType;
  graph_version_id?: string;
}

// WebSocket event types
export type WsEventType =
  | 'run_started'
  | 'run_completed'
  | 'run_failed'
  | 'run_cancelled'
  | 'node_started'
  | 'node_completed'
  | 'node_failed'
  | 'node_progress'
  | 'node_retried';

export interface WsEvent {
  type: WsEventType;
  run_id: string;
  data: Record<string, unknown>;
}
