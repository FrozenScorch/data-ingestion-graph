export interface GraphTemplateSummary {
  id: string;
  name: string;
  description: string;
  category: string;
  setup: string[];
  node_count: number;
  sdk_node_count: number;
}

export interface GraphTemplateListResponse {
  templates: GraphTemplateSummary[];
}
