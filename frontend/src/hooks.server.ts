import type { Handle } from '@sveltejs/kit';

const API_HOST = process.env.API_HOST || 'http://localhost:8040';

/**
 * Proxy /api/* and /ws/* requests to the FastAPI backend.
 * This avoids CORS issues and keeps the API URL opaque to the browser.
 */
export const handle: Handle = async ({ event, resolve }) => {
  const { request, url } = event;

  // Proxy API requests to backend
  if (url.pathname.startsWith('/api/') || url.pathname === '/health') {
    const targetUrl = `${API_HOST}${url.pathname}${url.search}`;
    const headers = new Headers(request.headers);
    headers.set('host', new URL(API_HOST).host);
    headers.delete('connection');

    const backendResponse = await fetch(targetUrl, {
      method: request.method,
      headers,
      body: request.method !== 'GET' && request.method !== 'HEAD' ? await request.arrayBuffer() : undefined,
    });

    const responseHeaders = new Headers(backendResponse.headers);
    responseHeaders.delete('transfer-encoding');

    return new Response(backendResponse.body, {
      status: backendResponse.status,
      statusText: backendResponse.statusText,
      headers: responseHeaders,
    });
  }

  // Proxy WebSocket upgrade requests to backend
  if (url.pathname.startsWith('/ws/')) {
    // WebSocket proxying requires a full TCP proxy.
    // For now, let the client connect directly to the backend.
    return resolve(event);
  }

  return resolve(event);
};
