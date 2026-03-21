/**
 * Base HTTP client with JWT auth header and error handling.
 */

const API_BASE = '';

interface RequestOptions extends Omit<RequestInit, 'body'> {
  body?: unknown;
  params?: Record<string, string>;
}

class ApiError extends Error {
  constructor(
    public status: number,
    public detail: string,
    public data?: unknown
  ) {
    super(detail);
    this.name = 'ApiError';
  }
}

function getToken(): string | null {
  if (typeof localStorage === 'undefined') return null;
  return localStorage.getItem('auth_token');
}

function buildUrl(path: string, params?: Record<string, string>): string {
  const url = new URL(path, window.location.origin);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      url.searchParams.set(key, value);
    }
  }
  return url.toString();
}

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { body, params, headers: extraHeaders, ...rest } = options;

  const token = getToken();
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(extraHeaders as Record<string, string> || {})
  };

  const url = buildUrl(`${API_BASE}${path}`, params);

  const response = await fetch(url, {
    ...rest,
    headers,
    body: body ? JSON.stringify(body) : undefined
  });

  if (!response.ok) {
    let detail = `HTTP ${response.status}`;
    let data: unknown;
    try {
      const errorBody = await response.json();
      data = errorBody;
      if (typeof errorBody.detail === 'string') {
        detail = errorBody.detail;
      } else if (typeof errorBody.message === 'string') {
        detail = errorBody.message;
      }
    } catch {
      // ignore parse errors
    }
    throw new ApiError(response.status, detail, data);
  }

  // Handle 204 No Content
  if (response.status === 204) {
    return undefined as T;
  }

  return response.json();
}

export const api = {
  get<T>(path: string, params?: Record<string, string>): Promise<T> {
    return request<T>(path, { method: 'GET', params });
  },

  post<T>(path: string, body?: unknown): Promise<T> {
    return request<T>(path, { method: 'POST', body });
  },

  put<T>(path: string, body?: unknown): Promise<T> {
    return request<T>(path, { method: 'PUT', body });
  },

  patch<T>(path: string, body?: unknown): Promise<T> {
    return request<T>(path, { method: 'PATCH', body });
  },

  delete<T>(path: string): Promise<T> {
    return request<T>(path, { method: 'DELETE' });
  }
};

export { ApiError };
