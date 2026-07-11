export interface IngestionFile {
  id: string;
  name: string;
  size: number;
  content_type: string;
  created_at: string;
}

export interface IngestionFileList {
  files: IngestionFile[];
  total: number;
}
