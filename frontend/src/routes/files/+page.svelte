<script lang="ts">
  import { onMount } from 'svelte';
  import { artifactService } from '$lib/services/artifactService.js';
  import type { IngestionFile } from '$lib/types/artifact.js';

  let files = $state<IngestionFile[]>([]);
  let loading = $state(true);
  let busy = $state(false);
  let progress = $state(0);
  let message = $state('');
  let dragging = $state(false);

  async function refresh() {
    loading = true;
    try { files = (await artifactService.list()).files; }
    catch (e) { message = e instanceof Error ? e.message : 'Could not load files'; }
    finally { loading = false; }
  }

  async function add(selected: File[]) {
    if (!selected.length) return;
    busy = true; progress = 0; message = '';
    try {
      const uploaded = await artifactService.upload(selected, value => progress = value);
      message = `Uploaded ${uploaded.length} file${uploaded.length === 1 ? '' : 's'}`;
      await refresh();
    } catch (e) { message = e instanceof Error ? e.message : 'Upload failed'; }
    finally { busy = false; }
  }

  async function remove(file: IngestionFile) {
    if (!confirm(`Delete ${file.name}? Files referenced by saved graph history cannot be deleted.`)) return;
    try { await artifactService.delete(file.id); await refresh(); }
    catch (e) { message = e instanceof Error ? e.message : 'Delete failed'; }
  }

  function choose(event: Event) {
    const input = event.target as HTMLInputElement;
    add(Array.from(input.files ?? []));
    input.value = '';
  }

  onMount(refresh);
</script>

<div class="h-full overflow-y-auto p-6">
  <div class="max-w-5xl mx-auto space-y-5">
    <div>
      <h1 class="text-xl font-bold text-gray-100">Files</h1>
      <p class="mt-1 text-sm text-gray-500">Private, Studio-managed inputs for document ingestion graphs.</p>
    </div>

    <label
      class="block rounded-xl border-2 border-dashed {dragging ? 'border-cyan-500 bg-cyan-500/5' : 'border-gray-700 bg-gray-900'} p-8 text-center cursor-pointer"
      ondragover={(e) => { e.preventDefault(); dragging = true; }}
      ondragleave={() => dragging = false}
      ondrop={(e) => { e.preventDefault(); dragging = false; add(Array.from(e.dataTransfer?.files ?? [])); }}
    >
      <input class="hidden" type="file" multiple accept=".pdf,.doc,.docx,.csv,.txt,.md,.json,.xml,.html,.htm" onchange={choose} disabled={busy} />
      <p class="text-sm text-gray-300">{busy ? `Uploading ${progress}%` : 'Drop files here or click to browse'}</p>
      <p class="mt-1 text-xs text-gray-600">PDF, Word, CSV, text, Markdown, JSON, XML, or HTML &middot; up to 100 MB each</p>
    </label>

    {#if message}<p class="text-sm text-gray-400">{message}</p>{/if}

    <div class="rounded-xl border border-gray-800 bg-gray-900 overflow-hidden">
      {#if loading}
        <p class="p-5 text-sm text-gray-500">Loading files...</p>
      {:else if files.length === 0}
        <p class="p-8 text-center text-sm text-gray-600">No files uploaded yet.</p>
      {:else}
        {#each files as file (file.id)}
          <div class="flex items-center gap-4 border-b last:border-b-0 border-gray-800 px-4 py-3">
            <div class="min-w-0 flex-1">
              <p class="truncate text-sm text-gray-200">{file.name}</p>
              <p class="text-xs text-gray-600">{(file.size / 1024).toFixed(1)} KB &middot; {new Date(file.created_at).toLocaleString()}</p>
            </div>
            <button onclick={() => remove(file)} class="text-xs text-red-400 hover:text-red-300">Delete</button>
          </div>
        {/each}
      {/if}
    </div>
  </div>
</div>
