/**
 * Managed schedule and webhook trigger types.
 *
 * Secrets are deliberately absent from TriggerResponse. The API only returns a
 * plaintext secret from create/rotate operations.
 */

export type ManagedTriggerType = 'schedule' | 'webhook';
export type ScheduleKind = 'interval' | 'cron';

export interface TriggerResponse {
  id: string;
  graph_id: string;
  graph_version_id: string | null;
  name: string;
  trigger_type: ManagedTriggerType;
  enabled: boolean;
  schedule_kind: ScheduleKind | null;
  interval_seconds: number | null;
  cron_expression: string | null;
  timezone: string | null;
  next_run_at: string | null;
  last_run_at: string | null;
  last_run_id: string | null;
  rate_limit_per_minute: number | null;
  webhook_path: string | null;
  created_at: string;
  updated_at: string;
}

export interface TriggerCreate {
  name: string;
  trigger_type: ManagedTriggerType;
  graph_version_id?: string;
  enabled: boolean;
  schedule_kind?: ScheduleKind;
  interval_seconds?: number;
  cron_expression?: string;
  timezone?: string;
  rate_limit_per_minute?: number;
}

export interface TriggerUpdate {
  name?: string;
  graph_version_id?: string | null;
  enabled?: boolean;
  schedule_kind?: ScheduleKind;
  interval_seconds?: number;
  cron_expression?: string;
  timezone?: string;
  rate_limit_per_minute?: number;
}

export interface TriggerCreateResult {
  trigger: TriggerResponse;
  secret?: string;
}

export interface TriggerSecretResult {
  trigger: TriggerResponse;
  secret: string;
}
