import { execution } from './execution.svelte.js';
import { graph } from './graph.svelte.js';

export { auth } from './auth.svelte.js';
export { graph } from './graph.svelte.js';
export { execution } from './execution.svelte.js';
export { nodeRegistry } from './nodeRegistry.svelte.js';
export { openrouter } from './openrouter.svelte.js';

/**
 * Clear all non-auth stores. Called on logout to prevent stale state
 * (e.g. disconnected WebSocket references, cached graph data).
 */
export function clearAllStores(): void {
  execution.disconnectWs();
  execution.clear();
  graph.clear();
}
