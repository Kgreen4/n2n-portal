<script lang="ts">
  import { cn } from '$utils';

  let className = '';
  export { className as class };
</script>

<h3 class={cn('text-lg font-semibold leading-none tracking-normal', className)}>
  <slot />
</h3>
