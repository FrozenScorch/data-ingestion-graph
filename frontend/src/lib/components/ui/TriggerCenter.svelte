<script lang="ts">
  import { onDestroy, onMount, tick } from 'svelte';
  import { triggerService } from '$lib/services/triggerService.js';
  import type {
    ManagedTriggerType,
    ScheduleKind,
    TriggerCreate,
    TriggerResponse,
    TriggerUpdate
  } from '$lib/types';

  interface Props {
    graphId: string;
    currentVersionId?: string | null;
    currentVersionNumber?: number | null;
    onClose: () => void;
  }

  interface OneTimeSecret {
    triggerId: string;
    triggerName: string;
    secret: string;
  }

  interface Confirmation {
    kind: 'rotate' | 'delete';
    trigger: TriggerResponse;
  }

  type IntervalUnit = 'minutes' | 'hours';

  let {
    graphId,
    currentVersionId = null,
    currentVersionNumber = null,
    onClose
  }: Props = $props();

  let triggers = $state<TriggerResponse[]>([]);
  let loading = $state(true);
  let refreshing = $state(false);
  let loadError = $state<string | null>(null);
  let actionError = $state<string | null>(null);
  let notice = $state<string | null>(null);
  let busyTriggerId = $state<string | null>(null);
  let view = $state<'list' | 'create'>('list');
  let editingId = $state<string | null>(null);
  let confirmation = $state<Confirmation | null>(null);
  let oneTimeSecret = $state<OneTimeSecret | null>(null);
  let origin = $state('');
  let copiedKey = $state<string | null>(null);

  let closeButton: HTMLButtonElement;
  let confirmationCancelButton = $state<HTMLButtonElement>();
  let centerPanel: HTMLDivElement;
  let confirmationPanel = $state<HTMLDivElement>();
  let returnFocusElement: HTMLElement | null = null;
  let confirmationReturnFocus: HTMLElement | null = null;
  let copyTimer: ReturnType<typeof setTimeout> | null = null;

  // Create form state.
  let createType = $state<ManagedTriggerType>('schedule');
  let createName = $state('');
  let createEnabled = $state(true);
  let createPinVersion = $state(false);
  let createScheduleKind = $state<ScheduleKind>('interval');
  let createIntervalAmount = $state(15);
  let createIntervalUnit = $state<IntervalUnit>('minutes');
  let createCronExpression = $state('0 * * * *');
  let createTimezone = $state('UTC');
  let createRateLimit = $state(60);
  let createError = $state<string | null>(null);
  let creating = $state(false);

  // Edit form state.
  let editName = $state('');
  let editScheduleKind = $state<ScheduleKind>('interval');
  let editIntervalAmount = $state(15);
  let editIntervalUnit = $state<IntervalUnit>('minutes');
  let editCronExpression = $state('0 * * * *');
  let editTimezone = $state('UTC');
  let editRateLimit = $state(60);
  let editError = $state<string | null>(null);

  const timezones = [
    'UTC',
    'America/New_York',
    'America/Chicago',
    'America/Denver',
    'America/Los_Angeles',
    'America/Sao_Paulo',
    'Europe/London',
    'Europe/Paris',
    'Europe/Berlin',
    'Africa/Johannesburg',
    'Asia/Dubai',
    'Asia/Kolkata',
    'Asia/Singapore',
    'Asia/Tokyo',
    'Australia/Sydney',
    'Pacific/Auckland'
  ];

  onMount(() => {
    returnFocusElement = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    origin = window.location.origin;
    void loadTriggers();
    void tick().then(() => closeButton?.focus());
  });

  onDestroy(() => {
    if (copyTimer) clearTimeout(copyTimer);
    // Plaintext secrets intentionally die with this component instance.
    oneTimeSecret = null;
    returnFocusElement?.focus();
  });

  $effect(() => {
    if (confirmation) {
      void tick().then(() => confirmationCancelButton?.focus());
    }
  });

  async function loadTriggers(isRefresh = false) {
    if (isRefresh) {
      refreshing = true;
    } else {
      loading = true;
    }
    loadError = null;
    try {
      triggers = await triggerService.list(graphId);
    } catch (error) {
      loadError = error instanceof Error ? error.message : 'Unable to load triggers';
    } finally {
      loading = false;
      refreshing = false;
    }
  }

  function closeCenter() {
    oneTimeSecret = null;
    confirmation = null;
    onClose();
  }

  function handleKeydown(event: KeyboardEvent) {
    if (event.key === 'Tab') {
      const container = confirmation ? confirmationPanel : centerPanel;
      if (!container) return;
      const focusable = Array.from(container.querySelectorAll<HTMLElement>(
        'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
      )).filter(element => element.getAttribute('aria-hidden') !== 'true');
      if (focusable.length === 0) {
        event.preventDefault();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      const active = document.activeElement;
      if (event.shiftKey && (active === first || !container.contains(active))) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && (active === last || !container.contains(active))) {
        event.preventDefault();
        first.focus();
      }
      return;
    }

    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      if (confirmation) {
        closeConfirmation();
        return;
      }
      closeCenter();
    }
  }

  function openCreate(type: ManagedTriggerType) {
    createType = type;
    createName = type === 'schedule' ? 'Scheduled ingestion' : 'Inbound webhook';
    createEnabled = true;
    createPinVersion = Boolean(currentVersionId);
    createScheduleKind = 'interval';
    createIntervalAmount = 15;
    createIntervalUnit = 'minutes';
    createCronExpression = '0 * * * *';
    createTimezone = 'UTC';
    createRateLimit = 60;
    createError = null;
    actionError = null;
    editingId = null;
    view = 'create';
  }

  function cancelCreate() {
    createError = null;
    view = 'list';
  }

  function isValidTimezone(value: string): boolean {
    try {
      new Intl.DateTimeFormat('en-US', { timeZone: value }).format();
      return true;
    } catch {
      return false;
    }
  }

  function scheduleFields(
    scheduleKind: ScheduleKind,
    intervalAmount: number,
    intervalUnit: IntervalUnit,
    cronExpression: string,
    timezone: string
  ): { data: TriggerUpdate } | { error: string } {
    if (scheduleKind === 'interval') {
      const multiplier = intervalUnit === 'hours' ? 3600 : 60;
      const intervalSeconds = intervalAmount * multiplier;
      if (!Number.isFinite(intervalAmount) || intervalAmount <= 0) {
        return { error: 'Enter an interval greater than zero.' };
      }
      if (!Number.isSafeInteger(intervalSeconds)) {
        return { error: 'The interval must resolve to a whole number of seconds.' };
      }
      return {
        data: {
          schedule_kind: 'interval',
          interval_seconds: intervalSeconds
        }
      };
    }

    const normalizedCron = cronExpression.trim().replace(/\s+/g, ' ');
    if (normalizedCron.split(' ').length !== 5) {
      return { error: 'Cron expressions must contain exactly five fields.' };
    }
    const normalizedTimezone = timezone.trim();
    if (!normalizedTimezone || !isValidTimezone(normalizedTimezone)) {
      return { error: 'Enter a valid IANA timezone, such as UTC or America/New_York.' };
    }
    return {
      data: {
        schedule_kind: 'cron',
        cron_expression: normalizedCron,
        timezone: normalizedTimezone
      }
    };
  }

  function validateRateLimit(value: number): string | null {
    if (!Number.isSafeInteger(value) || value < 1) {
      return 'Rate limit must be a whole number of requests per minute.';
    }
    return null;
  }

  async function createTrigger() {
    const name = createName.trim();
    if (!name) {
      createError = 'Give this trigger a name.';
      return;
    }

    const payload: TriggerCreate = {
      name,
      trigger_type: createType,
      enabled: createEnabled
    };
    if (createPinVersion && currentVersionId) {
      payload.graph_version_id = currentVersionId;
    }

    if (createType === 'schedule') {
      const result = scheduleFields(
        createScheduleKind,
        createIntervalAmount,
        createIntervalUnit,
        createCronExpression,
        createTimezone
      );
      if ('error' in result) {
        createError = result.error;
        return;
      }
      Object.assign(payload, result.data);
    } else {
      const rateError = validateRateLimit(createRateLimit);
      if (rateError) {
        createError = rateError;
        return;
      }
      payload.rate_limit_per_minute = createRateLimit;
    }

    creating = true;
    createError = null;
    actionError = null;
    notice = null;
    try {
      const response = await triggerService.create(graphId, payload);
      triggers = [response.trigger, ...triggers.filter(item => item.id !== response.trigger.id)];
      view = 'list';
      notice = response.trigger.trigger_type === 'schedule'
        ? 'Schedule created.'
        : 'Webhook created.';

      if (
        response.trigger.trigger_type === 'webhook'
        && typeof response.secret === 'string'
        && response.secret.length > 0
      ) {
        oneTimeSecret = {
          triggerId: response.trigger.id,
          triggerName: response.trigger.name,
          secret: response.secret
        };
      } else if (response.trigger.trigger_type === 'webhook') {
        oneTimeSecret = null;
        actionError = 'The webhook was created, but no secret was returned. Rotate the secret before sending deliveries.';
      }
    } catch (error) {
      createError = error instanceof Error ? error.message : 'Unable to create trigger';
    } finally {
      creating = false;
    }
  }

  function startEditing(trigger: TriggerResponse) {
    view = 'list';
    editingId = trigger.id;
    editName = trigger.name;
    editScheduleKind = trigger.schedule_kind ?? 'interval';
    const seconds = trigger.interval_seconds ?? 900;
    if (seconds >= 3600 && seconds % 3600 === 0) {
      editIntervalAmount = seconds / 3600;
      editIntervalUnit = 'hours';
    } else {
      editIntervalAmount = seconds / 60;
      editIntervalUnit = 'minutes';
    }
    editCronExpression = trigger.cron_expression ?? '0 * * * *';
    editTimezone = trigger.timezone ?? 'UTC';
    editRateLimit = trigger.rate_limit_per_minute ?? 60;
    editError = null;
    actionError = null;
  }

  function cancelEditing() {
    editingId = null;
    editError = null;
  }

  function replaceTrigger(updated: TriggerResponse) {
    const exists = triggers.some(item => item.id === updated.id);
    triggers = exists
      ? triggers.map(item => item.id === updated.id ? updated : item)
      : [updated, ...triggers];
  }

  async function saveEdit(trigger: TriggerResponse) {
    const name = editName.trim();
    if (!name) {
      editError = 'Give this trigger a name.';
      return;
    }

    const payload: TriggerUpdate = { name };
    if (trigger.trigger_type === 'schedule') {
      const result = scheduleFields(
        editScheduleKind,
        editIntervalAmount,
        editIntervalUnit,
        editCronExpression,
        editTimezone
      );
      if ('error' in result) {
        editError = result.error;
        return;
      }
      Object.assign(payload, result.data);
    } else {
      const rateError = validateRateLimit(editRateLimit);
      if (rateError) {
        editError = rateError;
        return;
      }
      payload.rate_limit_per_minute = editRateLimit;
    }

    busyTriggerId = trigger.id;
    editError = null;
    actionError = null;
    notice = null;
    try {
      const updated = await triggerService.update(trigger.id, payload);
      replaceTrigger(updated);
      editingId = null;
      notice = 'Trigger updated.';
    } catch (error) {
      editError = error instanceof Error ? error.message : 'Unable to update trigger';
    } finally {
      busyTriggerId = null;
    }
  }

  async function toggleTrigger(trigger: TriggerResponse) {
    busyTriggerId = trigger.id;
    actionError = null;
    notice = null;
    try {
      const updated = await triggerService.update(trigger.id, { enabled: !trigger.enabled });
      replaceTrigger(updated);
      notice = updated.enabled ? 'Trigger enabled.' : 'Trigger disabled.';
    } catch (error) {
      actionError = error instanceof Error ? error.message : 'Unable to change trigger status';
    } finally {
      busyTriggerId = null;
    }
  }

  function askToRotate(trigger: TriggerResponse) {
    actionError = null;
    confirmationReturnFocus = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    confirmation = { kind: 'rotate', trigger };
  }

  function askToDelete(trigger: TriggerResponse) {
    actionError = null;
    confirmationReturnFocus = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    confirmation = { kind: 'delete', trigger };
  }

  function closeConfirmation() {
    confirmation = null;
    void tick().then(() => {
      if (confirmationReturnFocus?.isConnected) {
        confirmationReturnFocus.focus();
      } else {
        closeButton?.focus();
      }
    });
  }

  async function confirmAction() {
    const pending = confirmation;
    if (!pending) return;
    confirmation = null;
    busyTriggerId = pending.trigger.id;
    actionError = null;
    notice = null;

    try {
      if (pending.kind === 'rotate') {
        const response = await triggerService.rotateSecret(pending.trigger.id);
        replaceTrigger(response.trigger);
        oneTimeSecret = {
          triggerId: response.trigger.id,
          triggerName: response.trigger.name,
          secret: response.secret
        };
        notice = 'Webhook secret rotated. Update every sender before dismissing the secret.';
      } else {
        await triggerService.delete(pending.trigger.id);
        triggers = triggers.filter(item => item.id !== pending.trigger.id);
        if (editingId === pending.trigger.id) editingId = null;
        if (oneTimeSecret?.triggerId === pending.trigger.id) oneTimeSecret = null;
        notice = 'Trigger deleted.';
      }
    } catch (error) {
      actionError = error instanceof Error
        ? error.message
        : pending.kind === 'rotate'
          ? 'Unable to rotate webhook secret'
          : 'Unable to delete trigger';
    } finally {
      busyTriggerId = null;
      void tick().then(() => {
        if (confirmationReturnFocus?.isConnected) {
          confirmationReturnFocus.focus();
        } else {
          closeButton?.focus();
        }
      });
    }
  }

  function webhookUrl(triggerId: string): string {
    return (origin || '') + '/api/webhooks/' + triggerId;
  }

  function buildSigningExample(secretState: OneTimeSecret): string {
    const url = webhookUrl(secretState.triggerId);
    return [
      "import { createHmac, randomUUID } from 'node:crypto';",
      '',
      'const secret = ' + JSON.stringify(secretState.secret) + ';',
      "const body = JSON.stringify({ records: [{ id: 'example-1' }] });",
      'const timestamp = Math.floor(Date.now() / 1000).toString();',
      'const delivery = randomUUID();',
      "const signature = createHmac('sha256', secret)",
      "  .update(timestamp + '.' + body, 'utf8')",
      "  .digest('hex');",
      '',
      'await fetch(' + JSON.stringify(url) + ', {',
      "  method: 'POST',",
      '  headers: {',
      "    'Content-Type': 'application/json',",
      "    'X-Ingestion-Timestamp': timestamp,",
      "    'X-Ingestion-Delivery': delivery,",
      "    'X-Ingestion-Signature': 'sha256=' + signature",
      '  },',
      '  body // Sign and send this exact string; do not stringify it again.',
      '});'
    ].join('\n');
  }

  async function copyText(value: string, key: string) {
    actionError = null;
    try {
      await navigator.clipboard.writeText(value);
      copiedKey = key;
      if (copyTimer) clearTimeout(copyTimer);
      copyTimer = setTimeout(() => {
        copiedKey = null;
        copyTimer = null;
      }, 1800);
    } catch {
      actionError = 'Clipboard access was blocked. Select the value and copy it manually.';
    }
  }

  function dismissSecret() {
    oneTimeSecret = null;
    copiedKey = null;
  }

  function formatDateTime(value: string | null): string {
    if (!value) return 'Never';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: 'medium',
      timeStyle: 'short'
    }).format(date);
  }

  function scheduleSummary(trigger: TriggerResponse): string {
    if (trigger.schedule_kind === 'cron') {
      return (trigger.cron_expression ?? 'Cron') + ' · ' + (trigger.timezone ?? 'UTC');
    }
    const seconds = trigger.interval_seconds;
    if (!seconds) return 'Interval';
    if (seconds % 3600 === 0) {
      const hours = seconds / 3600;
      return 'Every ' + hours + ' hour' + (hours === 1 ? '' : 's');
    }
    if (seconds % 60 === 0) {
      const minutes = seconds / 60;
      return 'Every ' + minutes + ' minute' + (minutes === 1 ? '' : 's');
    }
    return 'Every ' + seconds + ' seconds';
  }

  function shortId(value: string): string {
    return value.length > 12 ? value.slice(0, 8) + '…' : value;
  }
</script>

<svelte:window onkeydown={handleKeydown} />

<div
  class="fixed inset-0 z-50 flex items-stretch justify-center bg-black/75 p-0 backdrop-blur-sm sm:p-4 lg:p-8"
  role="dialog"
  aria-modal="true"
  aria-labelledby="trigger-center-title"
>
  <div bind:this={centerPanel} class="flex h-full w-full max-w-6xl flex-col overflow-hidden border border-gray-800 bg-gray-950 shadow-2xl sm:rounded-2xl">
    <header class="flex shrink-0 items-start justify-between gap-4 border-b border-gray-800 bg-gray-900/80 px-4 py-4 sm:px-6">
      <div class="min-w-0">
        <div class="flex items-center gap-2">
          <span class="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-indigo-500/20 bg-indigo-500/10 text-indigo-300">
            <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
              <circle cx="12" cy="12" r="3"/>
              <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"/>
            </svg>
          </span>
          <div class="min-w-0">
            <h2 id="trigger-center-title" class="text-base font-semibold text-gray-100">Trigger Center</h2>
            <p class="truncate text-xs text-gray-500">Run this graph on a schedule or from an authenticated webhook.</p>
          </div>
        </div>
      </div>
      <button
        bind:this={closeButton}
        type="button"
        onclick={closeCenter}
        class="rounded-lg border border-gray-800 bg-gray-900 p-2 text-gray-500 transition-colors hover:border-gray-700 hover:text-gray-200 focus:outline-none focus:ring-2 focus:ring-indigo-500"
        aria-label="Close Trigger Center"
        title="Close (Escape)"
      >
        <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
          <path d="M18 6 6 18M6 6l12 12"/>
        </svg>
      </button>
    </header>

    <main class="min-h-0 flex-1 overflow-y-auto">
      <div class="mx-auto max-w-5xl space-y-5 px-4 py-5 sm:px-6 sm:py-6">
        {#if oneTimeSecret}
          <section class="overflow-hidden rounded-xl border border-amber-500/40 bg-amber-500/[0.06]" aria-labelledby="one-time-secret-title">
            <div class="flex items-start gap-3 border-b border-amber-500/20 bg-amber-500/[0.06] px-4 py-3">
              <svg class="mt-0.5 h-5 w-5 shrink-0 text-amber-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
                <path d="M12 9v4M12 17h.01"/>
                <path d="M10.3 2.86 1.82 17a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.7 2.86a2 2 0 0 0-3.4 0Z"/>
              </svg>
              <div class="min-w-0 flex-1">
                <h3 id="one-time-secret-title" class="text-sm font-semibold text-amber-200">Copy this webhook secret now</h3>
                <p class="mt-0.5 text-xs leading-5 text-amber-100/60">
                  This is the only time the secret for <span class="font-medium text-amber-100">{oneTimeSecret.triggerName}</span> will be shown.
                  Store it in your sender's secret manager before dismissing this panel.
                </p>
              </div>
              <button
                type="button"
                onclick={dismissSecret}
                class="shrink-0 rounded-md px-2 py-1 text-xs text-amber-200/70 hover:bg-amber-500/10 hover:text-amber-100 focus:outline-none focus:ring-2 focus:ring-amber-400"
              >
                I've saved it
              </button>
            </div>

            <div class="space-y-4 p-4">
              <div>
                <label for="one-time-webhook-url" class="mb-1.5 block text-xs font-medium text-gray-400">Webhook URL</label>
                <div class="flex gap-2">
                  <input
                    id="one-time-webhook-url"
                    value={webhookUrl(oneTimeSecret.triggerId)}
                    readonly
                    spellcheck="false"
                    class="min-w-0 flex-1 rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 font-mono text-xs text-gray-300 focus:border-indigo-500 focus:outline-none"
                  />
                  <button
                    type="button"
                    onclick={() => copyText(webhookUrl(oneTimeSecret!.triggerId), 'secret-url')}
                    class="rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-xs text-gray-300 hover:border-gray-600 hover:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
                    aria-label="Copy webhook URL"
                  >
                    {copiedKey === 'secret-url' ? 'Copied' : 'Copy URL'}
                  </button>
                </div>
              </div>

              <div>
                <label for="one-time-webhook-secret" class="mb-1.5 block text-xs font-medium text-gray-400">Signing secret</label>
                <div class="flex gap-2">
                  <input
                    id="one-time-webhook-secret"
                    type="text"
                    value={oneTimeSecret.secret}
                    readonly
                    autocomplete="off"
                    spellcheck="false"
                    class="min-w-0 flex-1 rounded-lg border border-amber-500/30 bg-gray-950 px-3 py-2 font-mono text-xs text-amber-100 focus:border-amber-400 focus:outline-none"
                  />
                  <button
                    type="button"
                    onclick={() => copyText(oneTimeSecret!.secret, 'secret')}
                    class="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-100 hover:bg-amber-500/15 focus:outline-none focus:ring-2 focus:ring-amber-400"
                    aria-label="Copy webhook signing secret"
                  >
                    {copiedKey === 'secret' ? 'Copied' : 'Copy secret'}
                  </button>
                </div>
              </div>

              <div>
                <div class="mb-1.5 flex items-center justify-between gap-3">
                  <span class="text-xs font-medium text-gray-400">Node.js signing example</span>
                  <button
                    type="button"
                    onclick={() => copyText(buildSigningExample(oneTimeSecret!), 'example')}
                    class="text-xs text-indigo-300 hover:text-indigo-200 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                  >
                    {copiedKey === 'example' ? 'Copied example' : 'Copy example'}
                  </button>
                </div>
                <pre class="max-h-72 overflow-auto rounded-lg border border-gray-800 bg-gray-950 p-3 text-[11px] leading-5 text-gray-300"><code>{buildSigningExample(oneTimeSecret)}</code></pre>
                <p class="mt-2 text-[11px] leading-4 text-gray-500">
                  The signature is HMAC-SHA256 over <code class="text-gray-400">timestamp + "." + exact raw request bytes</code>.
                  Build the JSON body once, then sign and send that same string.
                </p>
              </div>
            </div>
          </section>
        {/if}

        <div aria-live="polite" class="space-y-2">
          {#if actionError}
            <div class="flex items-start justify-between gap-3 rounded-lg border border-red-500/20 bg-red-500/10 px-3 py-2 text-xs text-red-300">
              <span>{actionError}</span>
              <button type="button" onclick={() => actionError = null} class="text-red-300/70 hover:text-red-200" aria-label="Dismiss error">×</button>
            </div>
          {/if}
          {#if notice}
            <div class="flex items-start justify-between gap-3 rounded-lg border border-emerald-500/20 bg-emerald-500/10 px-3 py-2 text-xs text-emerald-300">
              <span>{notice}</span>
              <button type="button" onclick={() => notice = null} class="text-emerald-300/70 hover:text-emerald-200" aria-label="Dismiss message">×</button>
            </div>
          {/if}
        </div>

        {#if view === 'create'}
          <section class="overflow-hidden rounded-xl border border-indigo-500/30 bg-gray-900" aria-labelledby="create-trigger-title">
            <div class="flex items-start justify-between gap-3 border-b border-gray-800 px-4 py-3 sm:px-5">
              <div>
                <h3 id="create-trigger-title" class="text-sm font-medium text-gray-100">Create {createType === 'schedule' ? 'schedule' : 'webhook'}</h3>
                <p class="mt-0.5 text-xs text-gray-500">
                  {createType === 'schedule'
                    ? 'Start runs automatically on an interval or cron expression.'
                    : 'Accept signed HTTP deliveries at a graph-specific endpoint.'}
                </p>
              </div>
              <button type="button" onclick={cancelCreate} class="text-xs text-gray-500 hover:text-gray-200">Cancel</button>
            </div>

            <form class="space-y-5 p-4 sm:p-5" onsubmit={(event) => { event.preventDefault(); void createTrigger(); }}>
              <fieldset>
                <legend class="mb-2 text-xs font-medium text-gray-400">Trigger type</legend>
                <div class="grid grid-cols-2 gap-2 rounded-lg bg-gray-950 p-1">
                  <button
                    type="button"
                    aria-pressed={createType === 'schedule'}
                    onclick={() => createType = 'schedule'}
                    class="rounded-md px-3 py-2 text-xs font-medium transition-colors {createType === 'schedule' ? 'bg-indigo-600 text-white' : 'text-gray-500 hover:text-gray-300'}"
                  >
                    Schedule
                  </button>
                  <button
                    type="button"
                    aria-pressed={createType === 'webhook'}
                    onclick={() => createType = 'webhook'}
                    class="rounded-md px-3 py-2 text-xs font-medium transition-colors {createType === 'webhook' ? 'bg-indigo-600 text-white' : 'text-gray-500 hover:text-gray-300'}"
                  >
                    Webhook
                  </button>
                </div>
              </fieldset>

              <div class="grid gap-4 sm:grid-cols-2">
                <label class="text-xs font-medium text-gray-400">
                  Name
                  <input
                    bind:value={createName}
                    maxlength="120"
                    required
                    placeholder={createType === 'schedule' ? 'Hourly customer sync' : 'Production events'}
                    class="mt-1.5 w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 placeholder:text-gray-600 focus:border-indigo-500 focus:outline-none"
                  />
                </label>
                <label class="flex items-center gap-3 self-end rounded-lg border border-gray-800 bg-gray-950 px-3 py-2.5 text-xs text-gray-400">
                  <input type="checkbox" bind:checked={createEnabled} class="h-4 w-4 rounded border-gray-600 bg-gray-900 text-indigo-600 focus:ring-indigo-500" />
                  Enable immediately
                </label>
              </div>

              <label class="flex items-start gap-3 rounded-lg border border-gray-800 bg-gray-950 px-3 py-3 text-xs text-gray-400">
                <input
                  type="checkbox"
                  bind:checked={createPinVersion}
                  disabled={!currentVersionId}
                  class="mt-0.5 h-4 w-4 rounded border-gray-600 bg-gray-900 text-indigo-600 focus:ring-indigo-500 disabled:opacity-40"
                />
                <span>
                  <span class="block font-medium text-gray-300">Pin to current saved version</span>
                  {#if currentVersionId}
                    <span class="mt-0.5 block text-gray-600">
                      {currentVersionNumber ? 'Version ' + currentVersionNumber + ' · ' : ''}{currentVersionId}
                    </span>
                  {:else}
                    <span class="mt-0.5 block text-amber-500/80">Save the graph first to pin it. Unpinned triggers use the latest saved version.</span>
                  {/if}
                </span>
              </label>

              {#if createType === 'schedule'}
                <fieldset class="space-y-3">
                  <legend class="text-xs font-medium text-gray-400">Schedule</legend>
                  <div class="flex gap-2">
                    <button
                      type="button"
                      aria-pressed={createScheduleKind === 'interval'}
                      onclick={() => createScheduleKind = 'interval'}
                      class="rounded-lg border px-3 py-1.5 text-xs transition-colors {createScheduleKind === 'interval' ? 'border-indigo-500/50 bg-indigo-500/10 text-indigo-300' : 'border-gray-700 text-gray-500 hover:text-gray-300'}"
                    >
                      Friendly interval
                    </button>
                    <button
                      type="button"
                      aria-pressed={createScheduleKind === 'cron'}
                      onclick={() => createScheduleKind = 'cron'}
                      class="rounded-lg border px-3 py-1.5 text-xs transition-colors {createScheduleKind === 'cron' ? 'border-indigo-500/50 bg-indigo-500/10 text-indigo-300' : 'border-gray-700 text-gray-500 hover:text-gray-300'}"
                    >
                      Advanced cron
                    </button>
                  </div>

                  {#if createScheduleKind === 'interval'}
                    <div class="grid grid-cols-[minmax(0,1fr)_minmax(8rem,0.55fr)] gap-3">
                      <label class="text-xs text-gray-400">
                        Every
                        <input
                          type="number"
                          min="0.01"
                          step="any"
                          bind:value={createIntervalAmount}
                          class="mt-1.5 w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 focus:border-indigo-500 focus:outline-none"
                        />
                      </label>
                      <label class="text-xs text-gray-400">
                        Unit
                        <select bind:value={createIntervalUnit} class="mt-1.5 w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 focus:border-indigo-500 focus:outline-none">
                          <option value="minutes">Minutes</option>
                          <option value="hours">Hours</option>
                        </select>
                      </label>
                    </div>
                  {:else}
                    <div class="grid gap-3 sm:grid-cols-[minmax(0,1fr)_minmax(12rem,0.7fr)]">
                      <label class="text-xs text-gray-400">
                        Five-field cron expression
                        <input
                          bind:value={createCronExpression}
                          required
                          spellcheck="false"
                          placeholder="0 2 * * *"
                          class="mt-1.5 w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 font-mono text-sm text-gray-100 placeholder:text-gray-600 focus:border-indigo-500 focus:outline-none"
                        />
                      </label>
                      <label class="text-xs text-gray-400">
                        IANA timezone
                        <input
                          bind:value={createTimezone}
                          list="trigger-timezones"
                          required
                          placeholder="America/New_York"
                          class="mt-1.5 w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 placeholder:text-gray-600 focus:border-indigo-500 focus:outline-none"
                        />
                      </label>
                    </div>
                  {/if}
                </fieldset>
              {:else}
                <div class="grid gap-3 sm:grid-cols-2">
                  <label class="text-xs font-medium text-gray-400">
                    Authenticated rate limit
                    <div class="mt-1.5 flex items-center rounded-lg border border-gray-700 bg-gray-950 focus-within:border-indigo-500">
                      <input
                        type="number"
                        min="1"
                        step="1"
                        bind:value={createRateLimit}
                        class="min-w-0 flex-1 rounded-l-lg bg-transparent px-3 py-2 text-sm text-gray-100 focus:outline-none"
                      />
                      <span class="shrink-0 border-l border-gray-800 px-3 text-xs text-gray-500">requests / minute</span>
                    </div>
                  </label>
                  <div class="rounded-lg border border-gray-800 bg-gray-950 px-3 py-2.5 text-xs leading-5 text-gray-500">
                    Every delivery must include a fresh timestamp, unique delivery ID, and valid HMAC signature.
                  </div>
                </div>
              {/if}

              {#if createError}
                <p class="rounded-lg border border-red-500/20 bg-red-500/10 px-3 py-2 text-xs text-red-300" role="alert">{createError}</p>
              {/if}

              <div class="flex flex-col-reverse gap-2 border-t border-gray-800 pt-4 sm:flex-row sm:justify-end">
                <button type="button" onclick={cancelCreate} class="rounded-lg px-4 py-2 text-sm text-gray-400 hover:bg-gray-800 hover:text-gray-200">Cancel</button>
                <button
                  type="submit"
                  disabled={creating}
                  class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-indigo-500 disabled:cursor-wait disabled:bg-gray-700 disabled:text-gray-500"
                >
                  {creating ? 'Creating…' : 'Create ' + createType}
                </button>
              </div>
            </form>
          </section>
        {:else}
          <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h3 class="text-sm font-medium text-gray-200">Automation</h3>
              <p class="mt-0.5 text-xs text-gray-500">{triggers.length} configured trigger{triggers.length === 1 ? '' : 's'}</p>
            </div>
            <div class="flex flex-wrap items-center gap-2">
              <button
                type="button"
                onclick={() => loadTriggers(true)}
                disabled={refreshing}
                class="rounded-lg border border-gray-800 bg-gray-900 px-3 py-2 text-xs text-gray-400 hover:border-gray-700 hover:text-gray-200 disabled:opacity-50"
              >
                {refreshing ? 'Refreshing…' : 'Refresh'}
              </button>
              <button
                type="button"
                onclick={() => openCreate('schedule')}
                class="rounded-lg border border-indigo-500/30 bg-indigo-500/10 px-3 py-2 text-xs font-medium text-indigo-300 hover:bg-indigo-500/15"
              >
                + Schedule
              </button>
              <button
                type="button"
                onclick={() => openCreate('webhook')}
                class="rounded-lg bg-indigo-600 px-3 py-2 text-xs font-medium text-white hover:bg-indigo-500"
              >
                + Webhook
              </button>
            </div>
          </div>

          {#if loading}
            <div class="flex min-h-56 items-center justify-center rounded-xl border border-gray-800 bg-gray-900/50">
              <div class="text-center">
                <div class="mx-auto h-5 w-5 animate-spin rounded-full border-2 border-gray-700 border-t-indigo-400"></div>
                <p class="mt-3 text-xs text-gray-500">Loading triggers…</p>
              </div>
            </div>
          {:else if loadError}
            <div class="flex min-h-56 flex-col items-center justify-center rounded-xl border border-red-500/20 bg-red-500/[0.04] px-5 text-center">
              <p class="text-sm font-medium text-red-300">Triggers could not be loaded</p>
              <p class="mt-1 max-w-lg text-xs text-red-200/60">{loadError}</p>
              <button type="button" onclick={() => loadTriggers()} class="mt-4 rounded-lg border border-red-500/30 px-3 py-2 text-xs text-red-200 hover:bg-red-500/10">Try again</button>
            </div>
          {:else if triggers.length === 0}
            <div class="flex min-h-64 flex-col items-center justify-center rounded-xl border border-dashed border-gray-800 bg-gray-900/30 px-5 text-center">
              <span class="flex h-11 w-11 items-center justify-center rounded-xl border border-gray-800 bg-gray-900 text-gray-600">
                <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
                  <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4"/>
                  <circle cx="12" cy="12" r="3"/>
                </svg>
              </span>
              <h4 class="mt-3 text-sm font-medium text-gray-300">No triggers yet</h4>
              <p class="mt-1 max-w-md text-xs leading-5 text-gray-600">Add a schedule for recurring ingestion or a signed webhook for event-driven runs.</p>
              <div class="mt-4 flex gap-2">
                <button type="button" onclick={() => openCreate('schedule')} class="rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300 hover:bg-gray-800">Create schedule</button>
                <button type="button" onclick={() => openCreate('webhook')} class="rounded-lg bg-indigo-600 px-3 py-2 text-xs text-white hover:bg-indigo-500">Create webhook</button>
              </div>
            </div>
          {:else}
            <div class="space-y-3">
              {#each triggers as trigger (trigger.id)}
                <article class="overflow-hidden rounded-xl border border-gray-800 bg-gray-900/70">
                  <div class="p-4 sm:p-5">
                    <div class="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
                      <div class="flex min-w-0 items-start gap-3">
                        <span class="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border {trigger.trigger_type === 'schedule' ? 'border-violet-500/20 bg-violet-500/10 text-violet-300' : 'border-cyan-500/20 bg-cyan-500/10 text-cyan-300'}">
                          {#if trigger.trigger_type === 'schedule'}
                            <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
                              <circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>
                            </svg>
                          {:else}
                            <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
                              <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/>
                              <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>
                            </svg>
                          {/if}
                        </span>
                        <div class="min-w-0">
                          <div class="flex flex-wrap items-center gap-2">
                            <h4 class="truncate text-sm font-medium text-gray-100">{trigger.name}</h4>
                            <span class="rounded-full border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide {trigger.enabled ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300' : 'border-gray-700 bg-gray-800 text-gray-500'}">
                              {trigger.enabled ? 'Enabled' : 'Disabled'}
                            </span>
                            <span class="text-[10px] uppercase tracking-wide text-gray-600">{trigger.trigger_type}</span>
                          </div>
                          <p class="mt-1 break-all font-mono text-[11px] text-gray-600">{trigger.id}</p>
                        </div>
                      </div>

                      <div class="flex flex-wrap items-center gap-x-3 gap-y-2">
                        <button
                          type="button"
                          role="switch"
                          aria-checked={trigger.enabled}
                          aria-label={(trigger.enabled ? 'Disable ' : 'Enable ') + trigger.name}
                          title={trigger.enabled ? 'Disable trigger' : 'Enable trigger'}
                          disabled={busyTriggerId === trigger.id}
                          onclick={() => toggleTrigger(trigger)}
                          class="relative h-5 w-9 rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:opacity-50 {trigger.enabled ? 'bg-indigo-600' : 'bg-gray-700'}"
                        >
                          <span class="absolute top-0.5 h-4 w-4 rounded-full bg-white shadow transition-all {trigger.enabled ? 'left-[18px]' : 'left-0.5'}"></span>
                        </button>
                        <button type="button" onclick={() => startEditing(trigger)} disabled={busyTriggerId === trigger.id} class="text-xs text-gray-400 hover:text-gray-100 disabled:opacity-40">Edit</button>
                        {#if trigger.trigger_type === 'webhook'}
                          <button type="button" onclick={() => askToRotate(trigger)} disabled={busyTriggerId === trigger.id} class="text-xs text-amber-400 hover:text-amber-300 disabled:opacity-40">Rotate secret</button>
                        {/if}
                        <button type="button" onclick={() => askToDelete(trigger)} disabled={busyTriggerId === trigger.id} class="text-xs text-red-400 hover:text-red-300 disabled:opacity-40">Delete</button>
                      </div>
                    </div>

                    <dl class="mt-4 grid grid-cols-2 gap-x-4 gap-y-3 border-t border-gray-800/80 pt-4 lg:grid-cols-4">
                      <div class="min-w-0">
                        <dt class="text-[10px] font-medium uppercase tracking-wide text-gray-600">Configuration</dt>
                        <dd class="mt-1 truncate text-xs text-gray-300" title={trigger.trigger_type === 'schedule' ? scheduleSummary(trigger) : (trigger.rate_limit_per_minute ?? 'Default') + ' requests / minute'}>
                          {trigger.trigger_type === 'schedule' ? scheduleSummary(trigger) : (trigger.rate_limit_per_minute ?? 'Default') + ' req/min'}
                        </dd>
                      </div>
                      <div class="min-w-0">
                        <dt class="text-[10px] font-medium uppercase tracking-wide text-gray-600">Pinned version</dt>
                        <dd class="mt-1 truncate text-xs text-gray-300" title={trigger.graph_version_id ?? 'Uses the latest saved version'}>
                          {trigger.graph_version_id ? shortId(trigger.graph_version_id) : 'Latest saved'}
                        </dd>
                      </div>
                      <div class="min-w-0">
                        <dt class="text-[10px] font-medium uppercase tracking-wide text-gray-600">Next run</dt>
                        <dd class="mt-1 truncate text-xs text-gray-300" title={trigger.next_run_at ?? ''}>
                          {trigger.trigger_type === 'webhook' ? 'On request' : trigger.enabled && trigger.next_run_at ? formatDateTime(trigger.next_run_at) : 'Not scheduled'}
                        </dd>
                      </div>
                      <div class="min-w-0">
                        <dt class="text-[10px] font-medium uppercase tracking-wide text-gray-600">Last run</dt>
                        <dd class="mt-1 truncate text-xs">
                          {#if trigger.last_run_id}
                            <a
                              href={'/graphs/' + graphId + '/runs/' + trigger.last_run_id}
                              class="text-indigo-300 hover:text-indigo-200 hover:underline"
                              title={trigger.last_run_at ?? trigger.last_run_id}
                            >
                              {trigger.last_run_at ? formatDateTime(trigger.last_run_at) : shortId(trigger.last_run_id)}
                            </a>
                          {:else}
                            <span class="text-gray-500">{trigger.last_run_at ? formatDateTime(trigger.last_run_at) : 'Never'}</span>
                          {/if}
                        </dd>
                      </div>
                    </dl>

                    {#if trigger.trigger_type === 'webhook'}
                      <div class="mt-4 flex min-w-0 items-center gap-2 rounded-lg border border-gray-800 bg-gray-950 px-3 py-2">
                        <span class="shrink-0 text-[10px] font-medium uppercase tracking-wide text-gray-600">Endpoint</span>
                        <code class="min-w-0 flex-1 truncate text-[11px] text-cyan-300/80">{webhookUrl(trigger.id)}</code>
                        <button
                          type="button"
                          onclick={() => copyText(webhookUrl(trigger.id), 'url-' + trigger.id)}
                          class="shrink-0 text-[11px] text-gray-500 hover:text-gray-200"
                          aria-label={'Copy webhook URL for ' + trigger.name}
                        >
                          {copiedKey === 'url-' + trigger.id ? 'Copied' : 'Copy URL'}
                        </button>
                      </div>
                    {/if}
                  </div>

                  {#if editingId === trigger.id}
                    <form
                      class="space-y-4 border-t border-indigo-500/20 bg-indigo-500/[0.03] p-4 sm:p-5"
                      onsubmit={(event) => { event.preventDefault(); void saveEdit(trigger); }}
                    >
                      <div class="flex items-center justify-between">
                        <h5 class="text-xs font-medium uppercase tracking-wide text-indigo-300">Edit {trigger.trigger_type}</h5>
                        <button type="button" onclick={cancelEditing} class="text-xs text-gray-500 hover:text-gray-200">Cancel</button>
                      </div>
                      <label class="block text-xs font-medium text-gray-400">
                        Name
                        <input bind:value={editName} maxlength="120" required class="mt-1.5 w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 focus:border-indigo-500 focus:outline-none" />
                      </label>

                      {#if trigger.trigger_type === 'schedule'}
                        <fieldset class="space-y-3">
                          <legend class="text-xs font-medium text-gray-400">Schedule</legend>
                          <div class="flex gap-2">
                            <button type="button" aria-pressed={editScheduleKind === 'interval'} onclick={() => editScheduleKind = 'interval'} class="rounded-lg border px-3 py-1.5 text-xs {editScheduleKind === 'interval' ? 'border-indigo-500/50 bg-indigo-500/10 text-indigo-300' : 'border-gray-700 text-gray-500'}">Interval</button>
                            <button type="button" aria-pressed={editScheduleKind === 'cron'} onclick={() => editScheduleKind = 'cron'} class="rounded-lg border px-3 py-1.5 text-xs {editScheduleKind === 'cron' ? 'border-indigo-500/50 bg-indigo-500/10 text-indigo-300' : 'border-gray-700 text-gray-500'}">Advanced cron</button>
                          </div>
                          {#if editScheduleKind === 'interval'}
                            <div class="grid grid-cols-[minmax(0,1fr)_minmax(8rem,0.55fr)] gap-3">
                              <label class="text-xs text-gray-400">Every
                                <input type="number" min="0.01" step="any" bind:value={editIntervalAmount} class="mt-1.5 w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 focus:border-indigo-500 focus:outline-none" />
                              </label>
                              <label class="text-xs text-gray-400">Unit
                                <select bind:value={editIntervalUnit} class="mt-1.5 w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 focus:border-indigo-500 focus:outline-none">
                                  <option value="minutes">Minutes</option>
                                  <option value="hours">Hours</option>
                                </select>
                              </label>
                            </div>
                          {:else}
                            <div class="grid gap-3 sm:grid-cols-[minmax(0,1fr)_minmax(12rem,0.7fr)]">
                              <label class="text-xs text-gray-400">Five-field cron expression
                                <input bind:value={editCronExpression} required spellcheck="false" class="mt-1.5 w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 font-mono text-sm text-gray-100 focus:border-indigo-500 focus:outline-none" />
                              </label>
                              <label class="text-xs text-gray-400">IANA timezone
                                <input bind:value={editTimezone} list="trigger-timezones" required class="mt-1.5 w-full rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 focus:border-indigo-500 focus:outline-none" />
                              </label>
                            </div>
                          {/if}
                        </fieldset>
                      {:else}
                        <label class="block text-xs font-medium text-gray-400">
                          Authenticated rate limit
                          <div class="mt-1.5 flex items-center rounded-lg border border-gray-700 bg-gray-950 focus-within:border-indigo-500">
                            <input type="number" min="1" step="1" bind:value={editRateLimit} class="min-w-0 flex-1 rounded-l-lg bg-transparent px-3 py-2 text-sm text-gray-100 focus:outline-none" />
                            <span class="shrink-0 border-l border-gray-800 px-3 text-xs text-gray-500">requests / minute</span>
                          </div>
                        </label>
                      {/if}

                      {#if editError}
                        <p class="rounded-lg border border-red-500/20 bg-red-500/10 px-3 py-2 text-xs text-red-300" role="alert">{editError}</p>
                      {/if}
                      <div class="flex justify-end gap-2">
                        <button type="button" onclick={cancelEditing} class="rounded-lg px-3 py-2 text-xs text-gray-400 hover:bg-gray-800 hover:text-gray-200">Cancel</button>
                        <button type="submit" disabled={busyTriggerId === trigger.id} class="rounded-lg bg-indigo-600 px-3 py-2 text-xs font-medium text-white hover:bg-indigo-500 disabled:bg-gray-700 disabled:text-gray-500">
                          {busyTriggerId === trigger.id ? 'Saving…' : 'Save changes'}
                        </button>
                      </div>
                    </form>
                  {/if}
                </article>
              {/each}
            </div>
          {/if}
        {/if}
      </div>
    </main>
  </div>

  {#if confirmation}
    <div class="fixed inset-0 z-[60] flex items-center justify-center bg-black/70 p-4" role="alertdialog" aria-modal="true" aria-labelledby="trigger-confirmation-title" aria-describedby="trigger-confirmation-description">
      <div bind:this={confirmationPanel} class="w-full max-w-md rounded-xl border border-gray-700 bg-gray-900 p-5 shadow-2xl">
        <div class="flex items-start gap-3">
          <span class="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg {confirmation.kind === 'delete' ? 'bg-red-500/10 text-red-400' : 'bg-amber-500/10 text-amber-400'}">
            <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
              <path d="M12 9v4M12 17h.01"/>
              <path d="M10.3 2.86 1.82 17a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.7 2.86a2 2 0 0 0-3.4 0Z"/>
            </svg>
          </span>
          <div>
            <h3 id="trigger-confirmation-title" class="text-sm font-semibold text-gray-100">
              {confirmation.kind === 'delete' ? 'Delete trigger?' : 'Rotate webhook secret?'}
            </h3>
            <p id="trigger-confirmation-description" class="mt-1 text-xs leading-5 text-gray-400">
              {#if confirmation.kind === 'delete'}
                <strong class="font-medium text-gray-200">{confirmation.trigger.name}</strong> will stop accepting new invocations immediately.
                Existing run history will remain available.
              {:else}
                The current secret for <strong class="font-medium text-gray-200">{confirmation.trigger.name}</strong> will stop working immediately.
                Every sender must be updated with the new one-time secret.
              {/if}
            </p>
          </div>
        </div>
        <div class="mt-5 flex justify-end gap-2">
          <button
            bind:this={confirmationCancelButton}
            type="button"
            onclick={closeConfirmation}
            class="rounded-lg px-3 py-2 text-sm text-gray-400 hover:bg-gray-800 hover:text-gray-200 focus:outline-none focus:ring-2 focus:ring-indigo-500"
          >
            Cancel
          </button>
          <button
            type="button"
            onclick={confirmAction}
            class="rounded-lg px-3 py-2 text-sm font-medium text-white focus:outline-none focus:ring-2 {confirmation.kind === 'delete' ? 'bg-red-600 hover:bg-red-500 focus:ring-red-500' : 'bg-amber-600 hover:bg-amber-500 focus:ring-amber-500'}"
          >
            {confirmation.kind === 'delete' ? 'Delete trigger' : 'Rotate secret'}
          </button>
        </div>
      </div>
    </div>
  {/if}

  <datalist id="trigger-timezones">
    {#each timezones as timezone}
      <option value={timezone}></option>
    {/each}
  </datalist>
</div>
