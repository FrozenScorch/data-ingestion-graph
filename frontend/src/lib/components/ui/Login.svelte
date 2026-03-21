<script lang="ts">
  import { auth } from '$lib/stores';

  let username = $state('');
  let password = $state('');
  let submitting = $state(false);

  async function handleSubmit(e: Event) {
    e.preventDefault();
    if (!username || !password) return;
    submitting = true;
    const success = await auth.login(username, password);
    if (success) {
      window.location.href = '/graphs';
    }
    submitting = false;
  }
</script>

<div class="min-h-screen flex items-center justify-center bg-gray-950">
  <div class="w-full max-w-sm">
    <div class="text-center mb-8">
      <div class="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-indigo-500/10 border border-indigo-500/20 mb-4">
        <svg class="w-8 h-8 text-indigo-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="5" cy="6" r="3"/>
          <circle cx="19" cy="6" r="3"/>
          <circle cx="12" cy="18" r="3"/>
          <line x1="8" y1="6" x2="16" y2="6"/>
          <line x1="7" y1="8" x2="10" y2="16"/>
          <line x1="17" y1="8" x2="14" y2="16"/>
        </svg>
      </div>
      <h1 class="text-2xl font-bold text-gray-100">Ingestion Graph</h1>
      <p class="text-sm text-gray-500 mt-1">Visual data pipeline builder</p>
    </div>

    <form onsubmit={handleSubmit} class="bg-gray-900 border border-gray-800 rounded-xl p-6 space-y-4">
      {#if auth.error}
        <div class="bg-red-500/10 border border-red-500/20 text-red-400 text-sm px-3 py-2 rounded-lg">
          {auth.error}
        </div>
      {/if}

      <div>
        <label for="username" class="block text-sm text-gray-400 mb-1.5">Username</label>
        <input
          id="username"
          type="text"
          bind:value={username}
          required
          autocomplete="username"
          class="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-100 text-sm placeholder-gray-500 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-colors"
          placeholder="Enter your username"
        />
      </div>

      <div>
        <label for="password" class="block text-sm text-gray-400 mb-1.5">Password</label>
        <input
          id="password"
          type="password"
          bind:value={password}
          required
          autocomplete="current-password"
          class="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-100 text-sm placeholder-gray-500 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-colors"
          placeholder="Enter your password"
        />
      </div>

      <button
        type="submit"
        disabled={submitting || !username || !password}
        class="w-full py-2 px-4 bg-indigo-600 hover:bg-indigo-500 disabled:bg-gray-700 disabled:text-gray-500 text-white text-sm font-medium rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 focus:ring-offset-gray-900"
      >
        {submitting ? 'Signing in...' : 'Sign In'}
      </button>
    </form>
  </div>
</div>
