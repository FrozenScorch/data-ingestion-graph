/**
 * OpenRouter store — models list, selected model.
 * Uses Svelte 5 runes for reactivity.
 */

import type { OpenRouterModel } from '$lib/types';
import { openrouterService } from '$lib/services/openrouterService.js';

class OpenRouterState {
  chatModels = $state<OpenRouterModel[]>([]);
  embeddingModels = $state<OpenRouterModel[]>([]);
  loading = $state(false);
  error = $state<string | null>(null);
  loaded = $state(false);

  async loadModels(): Promise<void> {
    if (this.loaded) return;
    this.loading = true;
    try {
      const [chatRes, embRes] = await Promise.all([
        openrouterService.listModels('chat'),
        openrouterService.listEmbeddingModels()
      ]);
      this.chatModels = chatRes.models;
      this.embeddingModels = embRes.models;
      this.loaded = true;
    } catch (e: unknown) {
      this.error = e instanceof Error ? e.message : 'Failed to load models';
    } finally {
      this.loading = false;
    }
  }

  getChatModelById(id: string): OpenRouterModel | undefined {
    return this.chatModels.find(m => m.id === id);
  }

  getEmbeddingModelById(id: string): OpenRouterModel | undefined {
    return this.embeddingModels.find(m => m.id === id);
  }
}

export const openrouter = new OpenRouterState();
