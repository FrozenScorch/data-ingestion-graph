<script lang="ts">
  import { graph, execution } from '$lib/stores';
  import { nodeRegistry } from '$lib/stores';
  import type { ConfigField } from '$lib/types';
  import ModelSelector from './ModelSelector.svelte';

  let { onClose, onDelete }: { onClose: () => void; onDelete: () => void } = $props();

  let node = $derived(graph.selectedNode);
  let nodeType = $derived(node ? nodeRegistry.getNodeByType(node.type) : null);
  let config = $derived(node?.data.config ?? {});

  // Latest run output for the selected node
  let nodeRunOutput = $derived(() => {
    if (!graph.selectedNodeId) return null;
    return execution.nodeStatuses.get(graph.selectedNodeId) ?? null;
  });

  let showRunOutput = $state(false);

  function formatOutputPreview(output: Record<string, unknown> | null): string {
    if (!output || typeof output !== 'object') return '';
    // Extract items from common output shapes
    const items: unknown[] = Array.isArray(output)
      ? output
      : Array.isArray((output as Record<string, unknown>).items)
        ? (output as Record<string, unknown>).items as unknown[]
        : Array.isArray((output as Record<string, unknown>).chunks)
          ? (output as Record<string, unknown>).chunks as unknown[]
          : Object.keys(output).length > 0
            ? [output]
            : [];
    const preview = items.slice(0, 10).map(item => {
      if (typeof item === 'string') return item.length > 100 ? item.slice(0, 100) + '...' : item;
      try {
        const str = JSON.stringify(item);
        return str.length > 150 ? str.slice(0, 150) + '...' : str;
      } catch { return String(item); }
    }).join('\n');
    return preview;
  }

  function getOutputItemCount(output: Record<string, unknown> | null): number {
    if (!output || typeof output !== 'object') return 0;
    if (Array.isArray(output)) return output.length;
    if (Array.isArray((output as Record<string, unknown>).items)) return ((output as Record<string, unknown>).items as unknown[]).length;
    if (Array.isArray((output as Record<string, unknown>).chunks)) return ((output as Record<string, unknown>).chunks as unknown[]).length;
    return Object.keys(output).length > 0 ? 1 : 0;
  }

  function updateConfig(key: string, value: unknown) {
    if (!graph.selectedNodeId) return;
    graph.updateNodeData(graph.selectedNodeId, {
      config: { ...config, [key]: value }
    });
  }

  function hasModelConfig(): boolean {
    if (!nodeType) return false;
    const props = nodeType.config_schema.properties;
    return 'model' in props || 'embedding_model' in props;
  }

  function getModelKey(): string {
    if (!nodeType) return 'model';
    const props = nodeType.config_schema.properties;
    if ('model' in props) return 'model';
    if ('embedding_model' in props) return 'embedding_model';
    return 'model';
  }

  function getModelCategory(): 'chat' | 'embedding' {
    return nodeType?.type === 'embedder' ? 'embedding' : 'chat';
  }
</script>

{#if node && nodeType}
  <div class="h-full flex flex-col bg-gray-900 border-l border-gray-800">
    <!-- Header -->
    <div class="flex items-center justify-between px-4 py-3 border-b border-gray-800 shrink-0">
      <div class="flex items-center gap-2 min-w-0">
        <div class="w-2 h-2 rounded-full
          {nodeType.category === 'source' ? 'bg-blue-500' : ''}
          {nodeType.category === 'processing' ? 'bg-green-500' : ''}
          {nodeType.category === 'ai' ? 'bg-purple-500' : ''}
          {nodeType.category === 'output' ? 'bg-orange-500' : ''}
        "></div>
        <h3 class="text-sm font-medium text-gray-200 truncate">{nodeType.display_name}</h3>
      </div>
      <div class="flex items-center gap-1 shrink-0">
        <button
          onclick={onDelete}
          class="text-gray-500 hover:text-red-400 transition-colors"
          aria-label="Delete node"
          title="Delete node (Delete)"
        >
          <svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <polyline points="3 6 5 6 21 6"/>
            <path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6"/>
            <path d="M10 11v6"/>
            <path d="M14 11v6"/>
            <path d="M9 6V4a1 1 0 011-1h4a1 1 0 011 1v2"/>
          </svg>
        </button>
        <button
          onclick={onClose}
          class="text-gray-500 hover:text-gray-300 transition-colors"
          aria-label="Close config panel"
        >
          <svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <line x1="18" y1="6" x2="6" y2="18"/>
            <line x1="6" y1="6" x2="18" y2="18"/>
          </svg>
        </button>
      </div>
    </div>

    <!-- Description -->
    <div class="px-4 py-2 border-b border-gray-800">
      <p class="text-xs text-gray-500">{nodeType.description}</p>
      <p class="text-xs text-gray-600 mt-1">Node ID: {node.id}</p>
    </div>

    <!-- Config Form -->
    <div class="flex-1 overflow-auto p-4 space-y-4">
      {#if nodeType.config_schema.required?.length}
        <p class="text-xs text-gray-600 mb-2">Fields marked with * are required</p>
      {/if}
      {#each Object.entries(nodeType.config_schema.properties) as [key, field]}
        {@const required = nodeType.config_schema.required?.includes(key)}
        {@const isEmptyRequired = required && !config[key] && !field.default}
        <div>
          <!-- svelte-ignore a11y_label_has_associated_control -->
          <label class="block text-xs text-gray-300 mb-0.5 font-medium" title={field.description}>
            {key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
            {#if required}
              <span class="text-red-400">*</span>
            {/if}
          </label>
          {#if field.description}
            <p class="text-xs text-gray-500 mb-1">{field.description}</p>
          {/if}

          {#if field.type === 'string' && field.enum}
            <!-- Enum dropdown -->
            <select
              value={(config[key] as string) || field.default || ''}
              onchange={(e) => updateConfig(key, (e.target as HTMLSelectElement).value)}
              class="w-full px-3 py-1.5 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-200 focus:outline-none focus:border-indigo-500"
            >
              <option value="">Select...</option>
              {#each field.enum as opt}
                <option value={opt}>{opt}</option>
              {/each}
            </select>

          {:else if key === 'model' || key === 'embedding_model'}
            <!-- Model selector (use onValueChange instead of bind since config is derived) -->
            <ModelSelector
              value={(config[key] as string) || ''}
              category={getModelCategory()}
              label=""
              onValueChange={(v: string) => updateConfig(key, v)}
            />

          {:else if field.type === 'string' && field.format === 'password'}
            <input
              type="password"
              value={(config[key] as string) || (field.default as string) || ''}
              oninput={(e) => updateConfig(key, (e.target as HTMLInputElement).value)}
              class="w-full px-3 py-1.5 bg-gray-800 border {isEmptyRequired ? 'border-amber-700' : 'border-gray-700'} rounded-lg text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-indigo-500"
              placeholder={field.description || key}
            />
            {#if isEmptyRequired}
              <p class="text-xs text-gray-600 mt-0.5">Required</p>
            {/if}

          {:else if field.type === 'string' && field.format === 'date'}
            <input
              type="date"
              value={(config[key] as string) || (field.default as string) || ''}
              oninput={(e) => updateConfig(key, (e.target as HTMLInputElement).value)}
              class="w-full px-3 py-1.5 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-200 focus:outline-none focus:border-indigo-500"
            />

          {:else if field.type === 'string' && field.format === 'textarea'}
            <textarea
              rows="4"
              value={(config[key] as string) || (field.default as string) || ''}
              oninput={(e) => updateConfig(key, (e.target as HTMLTextAreaElement).value)}
              class="w-full px-3 py-1.5 bg-gray-800 border {isEmptyRequired ? 'border-amber-700' : 'border-gray-700'} rounded-lg text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-indigo-500 resize-y"
              placeholder={field.description || key}
            ></textarea>
            {#if isEmptyRequired}
              <p class="text-xs text-gray-600 mt-0.5">Required</p>
            {/if}

          {:else if field.type === 'string'}
            <input
              type="text"
              value={(config[key] as string) || (field.default as string) || ''}
              oninput={(e) => updateConfig(key, (e.target as HTMLInputElement).value)}
              class="w-full px-3 py-1.5 bg-gray-800 border {isEmptyRequired ? 'border-amber-700' : 'border-gray-700'} rounded-lg text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-indigo-500"
              placeholder={field.description || key}
            />
            {#if isEmptyRequired}
              <p class="text-xs text-gray-600 mt-0.5">Required</p>
            {/if}

          {:else if field.type === 'number' || field.type === 'integer'}
            <input
              type="number"
              value={(config[key] as number) ?? (field.default as number) ?? 0}
              oninput={(e) => updateConfig(key, Number((e.target as HTMLInputElement).value))}
              class="w-full px-3 py-1.5 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-200 focus:outline-none focus:border-indigo-500"
            />

          {:else if field.type === 'boolean'}
            <label class="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={!!(config[key] ?? field.default)}
                onchange={(e) => updateConfig(key, (e.target as HTMLInputElement).checked)}
                class="w-4 h-4 rounded bg-gray-800 border-gray-700 text-indigo-500 focus:ring-indigo-500 focus:ring-offset-0"
              />
              <span class="text-sm text-gray-400">{field.description || key}</span>
            </label>

          {:else}
            <input
              type="text"
              value={JSON.stringify(config[key] ?? field.default ?? '')}
              oninput={(e) => {
                try { updateConfig(key, JSON.parse((e.target as HTMLInputElement).value)); } catch { /* ignore */ }
              }}
              class="w-full px-3 py-1.5 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-indigo-500 font-mono"
            />
          {/if}
        </div>
      {/each}

      <!-- Latest Run Output -->
      {#if nodeRunOutput() && nodeRunOutput().output_data}
        {@const runOut = nodeRunOutput()}
        <div class="border-t border-gray-800 pt-4">
          <button
            onclick={() => showRunOutput = !showRunOutput}
            class="flex items-center gap-1.5 text-xs text-gray-400 hover:text-gray-200 transition-colors w-full"
          >
            <svg
              class="w-3 h-3 transition-transform {showRunOutput ? 'rotate-90' : ''}"
              viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
            >
              <polyline points="9 18 15 12 9 6"/>
            </svg>
            Latest Run Output
            <span class="text-gray-600 ml-auto">
              {runOut.status}
              {#if getOutputItemCount(runOut.output_data) > 0}
                &middot; {getOutputItemCount(runOut.output_data)} items
              {/if}
            </span>
          </button>
          {#if showRunOutput}
            <div class="mt-2 rounded-lg border border-gray-800 bg-gray-950 overflow-hidden">
              <pre class="px-3 py-2 text-xs text-gray-300 font-mono overflow-x-auto whitespace-pre-wrap max-h-60 overflow-y-auto">{formatOutputPreview(runOut.output_data) || '(no previewable items)'}</pre>
            </div>
          {/if}
        </div>
      {/if}
    </div>
  </div>
{:else}
  <div class="h-full flex items-center justify-center bg-gray-900 border-l border-gray-800">
    <div class="text-center text-gray-600 text-sm">
      <svg class="w-8 h-8 mx-auto mb-2 text-gray-700" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
        <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
        <line x1="9" y1="3" x2="9" y2="21"/>
      </svg>
      <p>Select a node to configure</p>
    </div>
  </div>
{/if}
