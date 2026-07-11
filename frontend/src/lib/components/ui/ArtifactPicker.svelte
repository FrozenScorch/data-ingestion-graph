<script lang="ts">
  import { onMount } from 'svelte';
  import { artifactService } from '$lib/services/artifactService.js';
  import type { IngestionFile } from '$lib/types/artifact.js';

  let { value = [], onValueChange }: { value?: string[]; onValueChange: (ids: string[]) => void } = $props();
  let files = $state<IngestionFile[]>([]);
  let loading = $state(true);
  let error = $state('');
  let uploading = $state(false);
  let progress = $state(0);

  async function refresh() {
    loading = true;
    error = '';
    try { files = (await artifactService.list()).files; }
    catch (e) { error = e instanceof Error ? e.message : 'Could not load files'; }
    finally { loading = false; }
  }

  async function upload(event: Event) {
    const input = event.target as HTMLInputElement;
    const selected = Array.from(input.files ?? []);
    if (!selected.length) return;
    uploading = true;
    progress = 0;
    error = '';
    try {
      const added = await artifactService.upload(selected, percent => progress = percent);
      await refresh();
      // An empty selection means "all files", including the newly uploaded ones.
      if (value.length) onValueChange([...new Set([...value, ...added.map(item => item.id)])]);
    } catch (e) { error = e instanceof Error ? e.message : 'Upload failed'; }
    finally { uploading = false; input.value = ''; }
  }

  function toggle(id: string) {
    onValueChange(value.includes(id) ? value.filter(item => item !== id) : [...value, id]);
  }

  onMount(refresh);
</script>

<div class="rounded-lg border border-gray-700 bg-gray-950/40 p-3 space-y-2">
  <div class="flex items-center justify-between gap-2">
    <span class="text-xs text-gray-400">{value.length ? `${value.length} selected` : 'All uploaded files'}</span>
    <label class="text-xs text-cyan-400 hover:text-cyan-300 cursor-pointer">
      {uploading ? `Uploading ${progress}%` : 'Upload files'}
      <input class="hidden" type="file" multiple accept=".pdf,.doc,.docx,.csv,.txt,.md,.json,.xml,.html,.htm" onchange={upload} disabled={uploading} />
    </label>
  </div>
  {#if loading}
    <p class="text-xs text-gray-600">Loading files...</p>
  {:else if files.length === 0}
    <p class="text-xs text-gray-600">No files yet. Upload documents here or from Files.</p>
  {:else}
    <div class="max-h-40 overflow-y-auto space-y-1">
      {#each files as file (file.id)}
        <label class="flex items-center gap-2 rounded px-1 py-1 hover:bg-gray-800 cursor-pointer">
          <input type="checkbox" checked={value.includes(file.id)} onchange={() => toggle(file.id)} />
          <span class="min-w-0 flex-1 truncate text-xs text-gray-300" title={file.name}>{file.name}</span>
          <span class="text-[10px] text-gray-600">{Math.ceil(file.size / 1024)} KB</span>
        </label>
      {/each}
    </div>
  {/if}
  {#if error}<p class="text-xs text-red-400">{error}</p>{/if}
  <a href="/files" class="block text-right text-xs text-cyan-400 hover:text-cyan-300">Manage files</a>
</div>
