/**
 * Base HTTP client with JWT auth header and error handling.
 */

const API_BASE = '';

interface RequestOptions extends Omit<RequestInit, 'body'> {
  body?: unknown;
  params?: Record<string, string>;
  /** Number of retry attempts for transient network failures (default: 0). */
  retries?: number;
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

/** Delay helper for retry backoff. */
function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { body, params, headers: extraHeaders, retries = 0, ...rest } = options;
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
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

      // Handle HTTP-level errors
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

        // 401 Unauthorized: clear token and redirect to login
        if (response.status === 401) {
          if (typeof localStorage !== 'undefined') {
            localStorage.removeItem('auth_token');
          }
          // Avoid redirect loops if already on the login page
          if (typeof window !== 'undefined' && !window.location.pathname.startsWith('/login')) {
            window.location.href = '/login';
          }
        }

        throw new ApiError(response.status, detail, data);
      }

      // Handle 204 No Content
      if (response.status === 204) {
        return undefined as T;
      }

      return response.json();
    } catch (error: unknown) {
      lastError = error instanceof Error ? error : new Error(String(error));

      // Only retry on network errors (TypeError from fetch), not on ApiError (HTTP status)
      if (error instanceof ApiError) {
        throw error;
      }

      // If this is the last attempt, throw
      if (attempt >= retries) {
        throw new ApiError(0, lastError.message || 'Network request failed');
      }

      // Exponential backoff: 1s, 2s, 4s...
      await delay(1000 * Math.pow(2, attempt));
    }
  }

  // Should never reach here, but TypeScript needs it
  throw lastError || new ApiError(0, 'Request failed');
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
