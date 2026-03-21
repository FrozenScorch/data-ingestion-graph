/**
 * OpenRouter model types.
 */

export interface ModelPricing {
  prompt: string;      // e.g. "0.15" per 1M tokens
  completion: string;  // e.g. "0.6" per 1M tokens
}

export interface OpenRouterModel {
  id: string;
  name: string;
  context_length: number;
  pricing: ModelPricing;
  is_free: boolean;
  category?: 'chat' | 'embedding';
}

export interface ModelListResponse {
  models: OpenRouterModel[];
  total: number;
}

export interface CostEstimate {
  model: string;
  input_tokens: number;
  output_tokens: number;
  is_free: boolean;
  prompt_cost: number;
  completion_cost: number;
  total_cost: number;
}
