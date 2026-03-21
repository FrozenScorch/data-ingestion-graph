<script lang="ts">
  import Header from './Header.svelte';
  import { auth } from '$lib/stores';
  import { onMount } from 'svelte';

  let { children } = $props();

  onMount(async () => {
    if (auth.token && !auth.user) {
      await auth.fetchUser();
    }
  });
</script>

<div class="h-screen flex flex-col bg-gray-950 text-gray-100 overflow-hidden">
  <Header />
  <main class="flex-1 overflow-hidden">
    {#if auth.token && !auth.user}
      <div class="flex items-center justify-center h-full">
        <div class="text-gray-500 text-sm">Loading...</div>
      </div>
    {:else}
      {@render children()}
    {/if}
  </main>
</div>
