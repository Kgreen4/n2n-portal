<script lang="ts">
  import { cn } from '$utils';
  import { fly } from 'svelte/transition';

  export let ariaLabel: string | undefined = undefined;
  let className = '';
  export { className as class };
</script>

<aside
  aria-label={ariaLabel}
  class={cn(
    'fixed top-0 right-0 z-20 flex h-screen w-full max-w-md flex-col border-l border-slate-200 bg-white p-6 shadow-2xl',
    className
  )}
  transition:fly={{ x: 42, duration: 360 }}
>
  <slot />
</aside>
