<script lang="ts">
  import { cn } from '$utils';

  let className = '';
  export { className as class };
</script>

<h2 class={cn('text-lg font-semibold tracking-normal text-slate-950', className)}>
  <slot />
</h2>
