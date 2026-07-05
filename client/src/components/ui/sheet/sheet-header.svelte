<script lang="ts">
  import { cn } from '$utils';

  let className = '';
  export { className as class };
</script>

<div class={cn('flex items-start justify-between gap-6 border-b border-slate-100 pb-5', className)}>
  <slot />
</div>
