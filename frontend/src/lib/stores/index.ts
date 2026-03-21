import { execution } from './execution.js';
import { graph } from './graph.js';

export { auth } from './auth.js';
export { graph } from './graph.js';
export { execution } from './execution.js';
export { nodeRegistry } from './nodeRegistry.js';
export { openrouter } from './openrouter.js';

/**
 * Clear all non-auth stores. Called on logout to prevent stale state
 * (e.g. disconnected WebSocket references, cached graph data).
 */
export function clearAllStores(): void {
  execution.disconnectWs();
  execution.clear();
  graph.clear();
}
