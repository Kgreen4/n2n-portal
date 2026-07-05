<script lang="ts">
  import { cn } from '$utils';

  export let type = 'text';
  export let value = '';
  export let placeholder = '';
  export let ariaLabel: string | undefined = undefined;
  let className = '';
  export { className as class };
</script>

<input
  {type}
  bind:value
  {placeholder}
  aria-label={ariaLabel}
  class={cn(
    'flex h-10 w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-950 outline-none transition-colors placeholder:text-slate-400 focus-visible:border-slate-950 disabled:cursor-not-allowed disabled:opacity-50',
    className
  )}
/>
