<script lang="ts">
  import { auth } from '$lib/stores';
</script>

<div class="p-6 space-y-6">
  <h1 class="text-xl font-bold text-gray-100">Settings</h1>

  <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
    <!-- User Info -->
    <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
      <h2 class="text-sm font-medium text-gray-300 mb-4">User Information</h2>
      {#if auth.user}
        <dl class="space-y-3">
          <div>
            <dt class="text-xs text-gray-500">Username</dt>
            <dd class="text-sm text-gray-200">{auth.user.username}</dd>
          </div>
          <div>
            <dt class="text-xs text-gray-500">Email</dt>
            <dd class="text-sm text-gray-200">{auth.user.email}</dd>
          </div>
          <div>
            <dt class="text-xs text-gray-500">Role</dt>
            <dd class="text-sm text-gray-200">{auth.user.role}</dd>
          </div>
          <div>
            <dt class="text-xs text-gray-500">Created</dt>
            <dd class="text-sm text-gray-200">{new Date(auth.user.created_at).toLocaleString()}</dd>
          </div>
        </dl>
      {:else}
        <p class="text-sm text-gray-600">Not authenticated</p>
      {/if}
    </div>

    <!-- API Info -->
    <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
      <h2 class="text-sm font-medium text-gray-300 mb-4">API Configuration</h2>
      <dl class="space-y-3">
        <div>
          <dt class="text-xs text-gray-500">Backend URL</dt>
          <dd class="text-sm text-gray-200 font-mono">{import.meta.env.VITE_API_URL || `http://localhost:8040`}</dd>
        </div>
        <div>
          <dt class="text-xs text-gray-500">AI Provider</dt>
          <dd class="text-sm text-gray-200">OpenRouter</dd>
        </div>
        <div>
          <dt class="text-xs text-gray-500">Frontend Port</dt>
          <dd class="text-sm text-gray-200 font-mono">3000</dd>
        </div>
      </dl>
    </div>

    <!-- About -->
    <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 md:col-span-2">
      <h2 class="text-sm font-medium text-gray-300 mb-4">About</h2>
      <p class="text-sm text-gray-500">
        Ingestion Graph is a visual, node-based data ingestion pipeline builder.
        Construct DAGs on a canvas, configure AI-powered processing nodes,
        and execute pipelines with checkpointing, parallel fan-out/fan-in, retry logic,
        data lineage, and run replay.
      </p>
      <div class="flex items-center gap-4 mt-4 text-xs text-gray-600">
        <span>SvelteKit + TypeScript</span>
        <span>@xyflow/svelte</span>
        <span>Tailwind CSS</span>
        <span>FastAPI Backend</span>
      </div>
    </div>
  </div>
</div>
