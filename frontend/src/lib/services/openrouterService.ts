/**
 * OpenRouter model listing service.
 */

import { api } from './api.js';
import type { ModelListResponse, CostEstimate } from '$lib/types';

export const openrouterService = {
  /** List all models (or filtered by category) */
  async listModels(category?: 'chat' | 'embedding'): Promise<ModelListResponse> {
    const params = category ? { category } : undefined;
    return api.get<ModelListResponse>('/openrouter/models', params);
  },

  /** List embedding models only */
  async listEmbeddingModels(): Promise<ModelListResponse> {
    return api.get<ModelListResponse>('/openrouter/embedding-models');
  },

  /** Estimate cost for a model invocation */
  async estimateCost(
    model: string,
    inputTokens: number = 1000,
    outputTokens: number = 500
  ): Promise<CostEstimate> {
    return api.get<CostEstimate>('/openrouter/cost-estimate', {
      model,
      input_tokens: String(inputTokens),
      output_tokens: String(outputTokens)
    });
  }
};
