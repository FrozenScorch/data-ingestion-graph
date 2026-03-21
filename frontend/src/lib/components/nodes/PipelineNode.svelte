<script lang="ts">
  import { Handle, Position } from '@xyflow/svelte';
  import type { NodeProps } from '@xyflow/svelte';
  import { execution } from '$lib/stores';
  import type { NodeCategory, PortDef, NodeRunStatus } from '$lib/types';

  interface PipelineNodeData {
    label: string;
    config: Record<string, unknown>;
    category?: NodeCategory;
    icon?: string;
    inputs?: PortDef[];
    outputs?: PortDef[];
  }

  let {
    data,
    id
  }: NodeProps & { data: PipelineNodeData } = $props();

  let category = $derived(data.category || 'processing');
  let inputs = $derived(data.inputs || []);
  let outputs = $derived(data.outputs || []);

  let runStatus = $derived.by(() => {
    const status = execution.nodeStatuses.get(id);
    return status?.status || null;
  });

  function getCategoryClass(): string {
    switch (category) {
      case 'source': return 'node-category-source';
      case 'processing': return 'node-category-processing';
      case 'ai': return 'node-category-ai';
      case 'output': return 'node-category-output';
      default: return 'node-category-processing';
    }
  }

  function getStatusClass(): string {
    if (!runStatus) return '';
    switch (runStatus) {
      case 'running': return 'node-status-running';
      case 'completed': return 'node-status-completed';
      case 'failed': return 'node-status-failed';
      case 'cancelled': return 'node-status-cancelled';
      default: return '';
    }
  }

  function getStatusColor(): string {
    switch (runStatus) {
      case 'running': return 'bg-indigo-400 animate-pulse';
      case 'completed': return 'bg-green-400';
      case 'failed': return 'bg-red-400';
      case 'cancelled': return 'bg-gray-400';
      default: return '';
    }
  }

  function getIcon(): string {
    if (data.icon) return data.icon;
    switch (category) {
      case 'source': return 'M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z';
      case 'ai': return 'M12 2a4 4 0 014 4v2H8V6a4 4 0 014-4zM6 14a6 6 0 0112 0v4a2 2 0 01-2 2H8a2 2 0 01-2-2v-4z';
      case 'output': return 'M9 3H5a2 2 0 00-2 2v4m6-6h10a2 2 0 012 2v4M9 3v18m0 0h10a2 2 0 002-2V9M9 21H5a2 2 0 01-2-2V9m0 0h18';
      default: return 'M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z';
    }
  }

  function getConfigSummary(): string {
    const config = data.config || {};
    if (config.model) return config.model as string;
    if (config.embedding_model) return config.embedding_model as string;
    if (config.source_type) return config.source_type as string;
    if (config.parser_type) return config.parser_type as string;
    if (config.condition) return 'Filter';
    if (config.expression) return 'Transform';
    if (config.chunk_size) return `${config.chunk_size} chunks`;
    if (config.url) return config.url as string;
    if (config.table) return config.table as string;
    return '';
  }
</script>

<div class="min-w-[180px] rounded-lg border-2 {getCategoryClass()} {getStatusClass()} shadow-lg transition-shadow">
  <!-- Input handles -->
  {#each inputs as input, i}
    <Handle
      type="target"
      position={Position.Left}
      id={input.name}
      style="top: {((i + 1) / (inputs.length + 1)) * 100}%"
    />
  {/each}

  <!-- Node content -->
  <div class="px-3 py-2">
    <div class="flex items-center gap-2">
      <!-- Status indicator -->
      <div class="w-5 h-5 flex items-center justify-center shrink-0">
        {#if runStatus}
          <div class="w-2.5 h-2.5 rounded-full {getStatusColor()}"></div>
        {:else}
          <svg class="w-4 h-4 text-gray-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
            <path d={getIcon()}/>
          </svg>
        {/if}
      </div>

      <div class="flex-1 min-w-0">
        <div class="text-xs font-medium text-gray-200 truncate">{data.label}</div>
        {#if getConfigSummary()}
          <div class="text-[10px] text-gray-500 truncate mt-0.5">{getConfigSummary()}</div>
        {/if}
      </div>
    </div>
  </div>

  <!-- Output handles -->
  {#each outputs as output, i}
    <Handle
      type="source"
      position={Position.Right}
      id={output.name}
      style="top: {((i + 1) / (outputs.length + 1)) * 100}%"
    />
  {/each}
</div>
