<script lang="ts">
  import { cn } from '$utils';
  import { fade } from 'svelte/transition';

  export let onclick: ((event: MouseEvent) => void) | undefined = undefined;
  export let ariaLabel = 'Close panel';
  let className = '';
  export { className as class };
</script>

<button
  type="button"
  aria-label={ariaLabel}
  {onclick}
  class={cn('fixed inset-0 z-10 cursor-pointer border-0 bg-slate-950/20', className)}
  transition:fade={{ duration: 280 }}
>
  <slot />
</button>
