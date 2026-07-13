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
  format?: string;
  connection_type?: string;
  description?: string;
  enum?: string[];
  default?: unknown;
  properties?: Record<string, ConfigField>;
  items?: ConfigField;
  accepted_extensions?: string[];
  required?: string[];
}

export interface NodeTypeDef {
  type: string;
  display_name: string;
  category: NodeCategory;
  description: string;
  implementation: 'studio' | 'sdk-adapter';
  sdk_component: string | null;
  connector_manifest?: {
    name: string;
    version: string;
    capabilities: {
      incremental: boolean;
      resumable_full_refresh: boolean;
      deletes: boolean;
      schema_discovery: boolean;
      rate_limits: boolean;
    };
  } | null;
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
