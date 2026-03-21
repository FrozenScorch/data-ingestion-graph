<script lang="ts">
  import { auth } from '$lib/stores';
  import { goto } from '$app/navigation';
  import { onMount } from 'svelte';
  import { page } from '$app/stores';
  import '../app.css';

  let { children } = $props();

  onMount(async () => {
    if (auth.token && !auth.user) {
      await auth.fetchUser();
    }
  });

  // Auth guard: redirect to login if not authenticated (except login page)
  $effect(() => {
    const currentPath = $page.url.pathname;
    if (!auth.isAuthenticated && currentPath !== '/login') {
      goto('/login');
    }
  });
</script>

{#if !$page.url.pathname.startsWith('/login')}
  <div class="h-screen flex flex-col bg-gray-950 text-gray-100 overflow-hidden">
    <!-- Header -->
    <header class="h-11 border-b border-gray-800 bg-gray-950 flex items-center justify-between px-4 shrink-0">
      <div class="flex items-center gap-3">
        <a href="/graphs" class="flex items-center gap-2 text-indigo-400 hover:text-indigo-300 transition-colors">
          <svg class="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="5" cy="6" r="3"/>
            <circle cx="19" cy="6" r="3"/>
            <circle cx="12" cy="18" r="3"/>
            <line x1="8" y1="6" x2="16" y2="6"/>
            <line x1="7" y1="8" x2="10" y2="16"/>
            <line x1="17" y1="8" x2="14" y2="16"/>
          </svg>
          <span class="font-semibold text-sm tracking-wide">Ingestion Graph</span>
        </a>
      </div>

      <div class="flex items-center gap-4">
        <nav class="flex items-center gap-1 text-sm">
          <a href="/graphs" class="px-3 py-1 rounded-md text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition-colors">Graphs</a>
          <a href="/executions" class="px-3 py-1 rounded-md text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition-colors">Executions</a>
          <a href="/dead-letter" class="px-3 py-1 rounded-md text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition-colors">DLQ</a>
          <a href="/settings" class="px-3 py-1 rounded-md text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition-colors">Settings</a>
        </nav>

        {#if auth.user}
          <div class="flex items-center gap-3 pl-3 border-l border-gray-800">
            <span class="text-xs text-gray-500">{auth.user.username}</span>
            <span class="text-xs px-2 py-0.5 rounded-full bg-indigo-500/20 text-indigo-400 border border-indigo-500/30">{auth.user.role}</span>
            <button
              onclick={() => { auth.logout(); goto('/login'); }}
              class="text-gray-500 hover:text-gray-300 transition-colors"
              title="Logout"
            >
              <svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/>
                <polyline points="16 17 21 12 16 7"/>
                <line x1="21" y1="12" x2="9" y2="12"/>
              </svg>
            </button>
          </div>
        {/if}
      </div>
    </header>

    <main class="flex-1 overflow-hidden">
      {#if auth.token && !auth.user}
        <div class="flex items-center justify-center h-full">
          <div class="text-gray-500 text-sm">Loading...</div>
        </div>
      {:else if auth.isAuthenticated}
        {@render children()}
      {/if}
    </main>
  </div>
{:else}
  {@render children()}
{/if}
