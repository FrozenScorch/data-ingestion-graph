<script lang="ts">
  import { openrouter } from '$lib/stores';
  import { onMount } from 'svelte';
  import type { OpenRouterModel } from '$lib/types';

  let {
    value = $bindable(''),
    category = 'chat',
    label = 'Model',
    placeholder = 'Select a model...',
    onValueChange
  }: {
    value?: string;
    category?: string;
    label?: string;
    placeholder?: string;
    onValueChange?: (value: string) => void;
  } = $props();

  let search = $state('');
  let open = $state(false);

  let models = $derived.by(() => {
    const source = category === 'embedding' ? openrouter.embeddingModels : openrouter.chatModels;
    if (!search) return source;
    const q = search.toLowerCase();
    return source.filter(m => m.id.toLowerCase().includes(q) || m.name.toLowerCase().includes(q));
  });

  onMount(() => {
    openrouter.loadModels();
  });

  function selectModel(model: OpenRouterModel) {
    value = model.id;
    open = false;
    search = '';
    onValueChange?.(model.id);
  }

  function formatCost(costStr: string): string {
    const cost = parseFloat(costStr);
    if (cost === 0) return 'FREE';
    if (cost < 0.01) return `$${cost.toFixed(4)}`;
    if (cost < 1) return `$${cost.toFixed(2)}`;
    return `$${cost.toFixed(2)}`;
  }

  function handleBlur() {
    // Delay to allow click on option
    setTimeout(() => { open = false; }, 200);
  }
</script>

<div class="relative">
  {#if label}
    <span class="block text-xs text-gray-400 mb-1">{label}</span>
  {/if}

  <button
    type="button"
    onclick={() => open = !open}
    onblur={handleBlur}
    class="w-full px-3 py-1.5 bg-gray-800 border border-gray-700 rounded-lg text-sm text-left flex items-center justify-between hover:border-gray-600 transition-colors"
  >
    <span class="truncate {value ? 'text-gray-200' : 'text-gray-500'}">
      {value || placeholder}
    </span>
    <svg class="w-4 h-4 text-gray-500 shrink-0 ml-2" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <polyline points="6 9 12 15 18 9"/>
    </svg>
  </button>

  {#if open}
    <div class="absolute z-50 mt-1 w-full max-h-64 overflow-auto bg-gray-800 border border-gray-700 rounded-lg shadow-xl">
      <div class="p-2 sticky top-0 bg-gray-800 border-b border-gray-700">
        <input
          type="text"
          bind:value={search}
          placeholder="Search models..."
          class="w-full px-2 py-1 bg-gray-900 border border-gray-700 rounded text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-indigo-500"
        />
      </div>

      {#if models.length === 0}
        <div class="px-3 py-4 text-sm text-gray-500 text-center">No models found</div>
      {:else}
        {#each models as model (model.id)}
          <button
            type="button"
            onclick={() => selectModel(model)}
            class="w-full px-3 py-2 text-left hover:bg-gray-700 transition-colors flex items-center gap-2"
          >
            <div class="flex-1 min-w-0">
              <div class="text-sm text-gray-200 truncate">{model.id}</div>
              <div class="text-xs text-gray-500 truncate">{model.name}</div>
            </div>
            <div class="flex items-center gap-2 shrink-0">
              {#if model.is_free}
                <span class="text-xs px-1.5 py-0.5 rounded bg-green-500/20 text-green-400 border border-green-500/30 font-medium">
                  FREE
                </span>
              {:else}
                <div class="text-right">
                  <div class="text-xs text-gray-400">{formatCost(model.pricing.prompt)}/1M in</div>
                  <div class="text-xs text-gray-400">{formatCost(model.pricing.completion)}/1M out</div>
                </div>
              {/if}
            </div>
          </button>
        {/each}
      {/if}
    </div>
  {/if}
</div>
