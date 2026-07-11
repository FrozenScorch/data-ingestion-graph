<script lang="ts">
  import { onMount } from 'svelte';
  import { connectionService } from '$lib/services/connectionService.js';
  import type { ConnectionTypeDefinition, SavedConnection } from '$lib/types/connection.js';

  let connections = $state<SavedConnection[]>([]);
  let definitions = $state<ConnectionTypeDefinition[]>([]);
  let selectedType = $state('postgres');
  let name = $state('');
  let config = $state<Record<string, unknown>>({ port: 5432 });
  let showCreate = $state(false);
  let busy = $state(false);
  let message = $state('');

  let definition = $derived(definitions.find(item => item.type === selectedType));

  async function refresh() {
    const response = await connectionService.list();
    connections = response.connections;
  }

  function resetConfig(type: string) {
    selectedType = type;
    const selected = definitions.find(item => item.type === type);
    config = Object.fromEntries(
      (selected?.fields ?? [])
        .filter(field => field.default !== undefined)
        .map(field => [field.name, field.default])
    );
  }

  onMount(async () => {
    try {
      const [typeResponse] = await Promise.all([connectionService.types(), refresh()]);
      definitions = typeResponse.types;
      if (definitions.length) resetConfig(definitions[0].type);
    } catch (error) {
      message = error instanceof Error ? error.message : 'Failed to load connections';
    }
  });

  async function createConnection() {
    if (!name.trim()) return;
    busy = true;
    message = '';
    try {
      await connectionService.create({ name: name.trim(), type: selectedType, config });
      await refresh();
      name = '';
      resetConfig(selectedType);
      showCreate = false;
      message = 'Connection saved. Test it before using it in a pipeline.';
    } catch (error) {
      message = error instanceof Error ? error.message : 'Failed to save connection';
    } finally {
      busy = false;
    }
  }

  async function testConnection(connection: SavedConnection) {
    busy = true;
    message = '';
    try {
      const result = await connectionService.test(connection.id);
      await refresh();
      message = result.message;
    } catch (error) {
      message = error instanceof Error ? error.message : 'Connection test failed';
    } finally {
      busy = false;
    }
  }

  async function deleteConnection(connection: SavedConnection) {
    if (!confirm(`Delete connection "${connection.name}"?`)) return;
    busy = true;
    try {
      await connectionService.delete(connection.id);
      await refresh();
      message = 'Connection deleted.';
    } catch (error) {
      message = error instanceof Error ? error.message : 'Failed to delete connection';
    } finally {
      busy = false;
    }
  }
</script>

<section id="connections" class="bg-gray-900 border border-gray-800 rounded-xl p-5 md:col-span-2">
  <div class="flex items-start justify-between gap-4">
    <div>
      <h2 class="text-sm font-medium text-gray-200">Connection Center</h2>
      <p class="text-xs text-gray-500 mt-1">Encrypted credentials shared by SDK-backed graph nodes.</p>
    </div>
    <button onclick={() => showCreate = !showCreate} class="px-3 py-1.5 text-xs rounded-lg bg-indigo-600 text-white hover:bg-indigo-500">
      {showCreate ? 'Cancel' : 'New connection'}
    </button>
  </div>

  {#if showCreate && definition}
    <div class="mt-4 rounded-lg border border-gray-700 bg-gray-950 p-4 space-y-3">
      <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
        <label class="text-xs text-gray-400">Name
          <input bind:value={name} class="mt-1 w-full rounded bg-gray-800 border border-gray-700 px-3 py-2 text-sm text-gray-100" placeholder="Production database" />
        </label>
        <label class="text-xs text-gray-400">Type
          <select value={selectedType} onchange={(event) => resetConfig(event.currentTarget.value)} class="mt-1 w-full rounded bg-gray-800 border border-gray-700 px-3 py-2 text-sm text-gray-100">
            {#each definitions as item}<option value={item.type}>{item.label}</option>{/each}
          </select>
        </label>
      </div>
      <p class="text-xs text-gray-600">{definition.description}</p>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
        {#each definition.fields as field}
          <label class="text-xs text-gray-400">{field.label}{field.required ? ' *' : ''}
            <input
              type={field.format === 'password' ? 'password' : field.type === 'integer' ? 'number' : 'text'}
              value={String(config[field.name] ?? '')}
              oninput={(event) => config = { ...config, [field.name]: field.type === 'integer' ? Number(event.currentTarget.value) : event.currentTarget.value }}
              class="mt-1 w-full rounded bg-gray-800 border border-gray-700 px-3 py-2 text-sm text-gray-100"
            />
          </label>
        {/each}
      </div>
      <button onclick={createConnection} disabled={busy || !name.trim()} class="px-3 py-1.5 text-xs rounded-lg bg-indigo-600 text-white disabled:bg-gray-700">Save encrypted connection</button>
    </div>
  {/if}

  {#if message}<p class="mt-3 text-xs text-cyan-300">{message}</p>{/if}

  <div class="mt-4 space-y-2">
    {#if connections.length === 0}
      <p class="text-sm text-gray-600">No saved connections yet.</p>
    {/if}
    {#each connections as connection (connection.id)}
      <div class="flex items-center justify-between gap-3 rounded-lg border border-gray-800 bg-gray-950 px-3 py-2">
        <div class="min-w-0">
          <p class="text-sm text-gray-200 truncate">{connection.name}</p>
          <p class="text-xs text-gray-600">{connection.type} · {connection.is_valid ? 'tested' : 'not tested'}</p>
        </div>
        <div class="flex gap-2">
          <button onclick={() => testConnection(connection)} disabled={busy} class="text-xs text-cyan-300 hover:text-cyan-200">Test</button>
          <button onclick={() => deleteConnection(connection)} disabled={busy} class="text-xs text-red-400 hover:text-red-300">Delete</button>
        </div>
      </div>
    {/each}
  </div>
</section>
