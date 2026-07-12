import { api, ApiError } from './api.js';
import type { IngestionFile, IngestionFileList } from '$lib/types/artifact.js';

function token(): string | null {
  return typeof localStorage === 'undefined' ? null : localStorage.getItem('auth_token');
}

export const artifactService = {
  list(): Promise<IngestionFileList> {
    return api.get('/files');
  },

  delete(id: string): Promise<void> {
    return api.delete(`/files/${id}`);
  },

  upload(files: File[], onProgress?: (percent: number) => void): Promise<IngestionFile[]> {
    return new Promise((resolve, reject) => {
      const request = new XMLHttpRequest();
      request.open('POST', '/api/files');
      const authToken = token();
      if (authToken) request.setRequestHeader('Authorization', `Bearer ${authToken}`);
      request.upload.onprogress = (event) => {
        if (event.lengthComputable) onProgress?.(Math.round((event.loaded / event.total) * 100));
      };
      request.onload = () => {
        let body: unknown;
        try { body = request.responseText ? JSON.parse(request.responseText) : undefined; } catch { body = undefined; }
        if (request.status >= 200 && request.status < 300) {
          resolve(body as IngestionFile[]);
          return;
        }
        const detail = typeof body === 'object' && body && 'detail' in body
          ? String((body as { detail: unknown }).detail)
          : `HTTP ${request.status}`;
        if (request.status === 401) {
          localStorage.removeItem('auth_token');
          window.location.href = '/login';
        }
        reject(new ApiError(request.status, detail, body));
      };
      request.onerror = () => reject(new ApiError(0, 'Network request failed'));
      const form = new FormData();
      for (const file of files) form.append('files', file, file.name);
      request.send(form);
    });
  }
};
