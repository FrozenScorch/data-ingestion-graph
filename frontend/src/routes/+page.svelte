<script lang="ts">
  import { goto } from '$app/navigation';
  import { onMount } from 'svelte';

  onMount(() => {
    goto('/graphs');
  });
</script>

<div class="flex items-center justify-center h-full">
  <div class="text-gray-500 text-sm">Redirecting...</div>
</div>
