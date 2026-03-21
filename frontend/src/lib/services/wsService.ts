/**
 * WebSocket service for live run progress events.
 */

import type { WsEvent } from '$lib/types';

type EventHandler = (event: WsEvent) => void;

function getWsBaseUrl(): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}`;
}

export function createRunWebSocket(
  runId: string,
  onEvent: EventHandler,
  onError?: (error: Event) => void,
  onClose?: (event: CloseEvent) => void
): { close: () => void } {
  const token = typeof localStorage !== 'undefined'
    ? localStorage.getItem('auth_token')
    : null;

  const wsUrl = token
    ? `${getWsBaseUrl()}/ws/executions/${runId}?token=${encodeURIComponent(token)}`
    : `${getWsBaseUrl()}/ws/executions/${runId}`;

  let ws: WebSocket | null = null;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let isClosed = false;

  function connect() {
    if (isClosed) return;

    ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as WsEvent;
        onEvent(data);
      } catch (e) {
        console.error('Failed to parse WS message:', e);
      }
    };

    ws.onerror = (event) => {
      console.error('WebSocket error:', event);
      onError?.(event);
    };

    ws.onclose = (event) => {
      onClose?.(event);
      if (!isClosed && !event.wasClean) {
        // Auto-reconnect after 3 seconds
        reconnectTimer = setTimeout(connect, 3000);
      }
    };
  }

  connect();

  return {
    close() {
      isClosed = true;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      ws?.close();
    }
  };
}
