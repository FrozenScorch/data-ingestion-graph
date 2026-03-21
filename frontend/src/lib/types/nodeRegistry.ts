/**
 * Node registry types matching backend API.
 */

export type PortDataType =
  | 'any'
  | 'file_list'
  | 'table'
  | 'json'
  | 'document'
  | 'chunks'
  | 'embeddings'
  | 'text'
  | 'items';

export type NodeCategory = 'source' | 'processing' | 'ai' | 'output';

export interface PortDef {
  name: string;
  data_type: PortDataType;
  label: string;
  required: boolean;
  multi: boolean;
}

export interface ConfigField {
  type: string;
  description?: string;
  enum?: string[];
  default?: unknown;
  properties?: Record<string, ConfigField>;
  items?: ConfigField;
  required?: string[];
}

export interface NodeTypeDef {
  type: string;
  display_name: string;
  category: NodeCategory;
  description: string;
  inputs: PortDef[];
  outputs: PortDef[];
  config_schema: {
    type: string;
    properties: Record<string, ConfigField>;
    required?: string[];
  };
}

export interface NodeRegistryResponse {
  nodes: NodeTypeDef[];
  total: number;
}

export interface NodeValidateRequest {
  config: Record<string, unknown>;
}

export interface NodeValidateResponse {
  valid: boolean;
  errors: string[];
}

// Category metadata for UI grouping
export interface NodeCategoryMeta {
  id: NodeCategory;
  label: string;
  icon: string;
  color: string;
}
