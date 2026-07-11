export interface SavedConnection {
  id: string;
  user_id: string;
  name: string;
  type: string;
  config: { configured?: boolean } | null;
  is_valid: boolean;
  created_at: string;
}

export interface ConnectionFieldDefinition {
  name: string;
  label: string;
  type: 'string' | 'integer';
  format?: 'password';
  required?: boolean;
  default?: string | number;
}

export interface ConnectionTypeDefinition {
  type: string;
  label: string;
  description: string;
  fields: ConnectionFieldDefinition[];
}
