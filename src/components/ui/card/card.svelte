<script lang="ts">
  import { cn } from '$utils';

  let className = '';
  export { className as class };
</script>

<section class={cn('rounded-lg border border-slate-200 bg-white text-slate-950 shadow-sm', className)}>
  <slot />
</section>
