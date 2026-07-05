<script module lang="ts">
  import type { AgentRunStatus } from '../../../types/agent';

  export type AgentRunStatusProps = {
    status: AgentRunStatus;
    error: string | null;
    fileCount: number;
    batchLabel: string | null;
    variant?: 'floating' | 'toolbar';
  };
</script>

<script lang="ts">
  let { status, error, fileCount, batchLabel, variant = 'floating' }: AgentRunStatusProps = $props();

  const title = $derived.by(() => {
    if (status === 'running') {
      return 'Reading documents';
    }

    if (status === 'complete') {
      return 'Documents processed';
    }

    return 'Document run failed';
  });

  const detail = $derived.by(() => {
    if (status === 'running') {
      return fileCount === 1 ? '1 file in background' : `${fileCount} files in background`;
    }

    if (status === 'complete') {
      return batchLabel ? `${batchLabel} refreshed` : 'Claims refreshed';
    }

    return error || 'Unable to process batch';
  });
</script>

{#if status === 'running' || status === 'complete' || status === 'failed'}
  <aside
    class={`pointer-events-none flex items-center bg-[#050505] text-white ${variant} ${status}`}
    aria-live="polite"
    aria-label={title}
  >
    <span class="matrix-mark" aria-hidden="true">
      {#each Array.from({ length: 9 }) as _, index}
        <span style={`--cell:${index}`}></span>
      {/each}
    </span>
    <span class="min-w-0">
      <span class="status-detail block truncate font-medium text-[#8d8d8d]">{detail}</span>
      <span class="status-title block truncate leading-tight font-semibold tracking-normal text-white">{title}</span>
    </span>
  </aside>
{/if}

<style>
  aside {
    animation: status-enter 420ms cubic-bezier(0.22, 1, 0.36, 1) both;
  }

  aside.complete {
    background: #071a14;
  }

  aside.failed {
    background: #220b08;
  }

  aside.floating {
    position: fixed;
    top: 20px;
    left: 50%;
    z-index: 50;
    min-width: 292px;
    transform: translateX(-50%);
    gap: 12px;
    border-radius: 4px 4px 28px 28px;
    padding: 16px 24px;
    box-shadow: 0 22px 55px rgba(0, 0, 0, 0.22);
  }

  aside.toolbar {
    min-width: 0;
    max-width: 250px;
    gap: 9px;
    border-left: 1px solid rgba(255, 255, 255, 0.14);
    border-radius: 999px;
    margin-left: 2px;
    padding: 5px 10px 5px 8px;
    box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.04);
  }

  aside.toolbar.complete {
    background: #0a1f18;
  }

  aside.toolbar.failed {
    background: #2a0f0b;
  }

  .status-detail {
    font-size: 15px;
  }

  .status-title {
    font-size: 24px;
  }

  aside.toolbar .status-detail {
    font-size: 10px;
    line-height: 1.05;
  }

  aside.toolbar .status-title {
    font-size: 12px;
    line-height: 1.12;
  }

  .matrix-mark {
    display: grid;
    grid-template-columns: repeat(3, 8px);
    grid-template-rows: repeat(3, 8px);
    gap: 3px;
    width: 30px;
    height: 30px;
    flex: 0 0 auto;
    filter: drop-shadow(0 0 14px rgba(195, 132, 82, 0.24));
  }

  aside.toolbar .matrix-mark {
    grid-template-columns: repeat(3, 5px);
    grid-template-rows: repeat(3, 5px);
    gap: 2px;
    width: 19px;
    height: 19px;
  }

  .matrix-mark span {
    display: block;
    border-radius: 2px;
    background: #c38452;
    opacity: 0.24;
    transform: scale(0.58);
    animation: matrix-shift 1800ms ease-in-out infinite;
    animation-delay: calc(var(--cell) * 95ms);
  }

  aside.complete .matrix-mark span {
    background: #10b981;
  }

  aside.failed .matrix-mark span {
    background: #c93b2b;
  }

  @keyframes status-enter {
    from {
      opacity: 0;
      transform: translateY(-6px) scale(0.98);
    }

    to {
      opacity: 1;
      transform: translateY(0) scale(1);
    }
  }

  aside.floating {
    animation-name: floating-status-enter;
  }

  @keyframes floating-status-enter {
    from {
      opacity: 0;
      transform: translate(-50%, -18px) scale(0.98);
    }

    to {
      opacity: 1;
      transform: translate(-50%, 0) scale(1);
    }
  }

  @keyframes matrix-shift {
    0%,
    100% {
      opacity: 0.2;
      transform: scale(0.55);
      border-radius: 2px;
    }

    34% {
      opacity: 1;
      transform: scale(1);
      border-radius: 3px;
    }

    68% {
      opacity: 0.45;
      transform: scale(0.78) rotate(45deg);
      border-radius: 1px;
    }
  }

  @media (prefers-reduced-motion: reduce) {
    aside,
    .matrix-mark span {
      animation: none;
    }
  }
</style>
