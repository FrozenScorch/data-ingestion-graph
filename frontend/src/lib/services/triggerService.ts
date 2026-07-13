/** Schedule and webhook trigger management service. */

import { api } from './api.js';
import type {
  TriggerCreate,
  TriggerCreateResult,
  TriggerResponse,
  TriggerSecretResult,
  TriggerUpdate
} from '$lib/types';

export const triggerService = {
  async list(graphId: string): Promise<TriggerResponse[]> {
    return api.get<TriggerResponse[]>(`/graphs/${graphId}/triggers`);
  },

  async create(graphId: string, data: TriggerCreate): Promise<TriggerCreateResult> {
    return api.post<TriggerCreateResult>(`/graphs/${graphId}/triggers`, data);
  },

  async update(triggerId: string, data: TriggerUpdate): Promise<TriggerResponse> {
    return api.patch<TriggerResponse>(`/triggers/${triggerId}`, data);
  },

  async delete(triggerId: string): Promise<void> {
    return api.delete<void>(`/triggers/${triggerId}`);
  },

  async rotateSecret(triggerId: string): Promise<TriggerSecretResult> {
    return api.post<TriggerSecretResult>(`/triggers/${triggerId}/rotate-secret`);
  }
};
