<script lang="ts">
  import Login from '$lib/components/ui/Login.svelte';
  import { auth } from '$lib/stores';
  import { goto } from '$app/navigation';

  // Redirect if already authenticated
  $effect(() => {
    if (auth.isAuthenticated) {
      goto('/graphs');
    }
  });
</script>

{#if !auth.isAuthenticated}
  <Login />
{/if}
