import { api } from './api.js';
import type { GraphTemplateListResponse } from '$lib/types/graphTemplate.js';

export const graphTemplateService = {
  async list(): Promise<GraphTemplateListResponse> {
    return api.get<GraphTemplateListResponse>('/graph-templates');
  }
};
