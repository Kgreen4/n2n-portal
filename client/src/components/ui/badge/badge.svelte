<script lang="ts">
  import { cn } from '$utils';

  type BadgeVariant = 'default' | 'secondary' | 'outline';

  export let variant: BadgeVariant = 'default';
  let className = '';
  export { className as class };

  const variants: Record<BadgeVariant, string> = {
    default: 'border-transparent bg-slate-950 text-white',
    secondary: 'border-transparent bg-slate-100 text-slate-950',
    outline: 'border-slate-200 text-slate-700'
  };
</script>

<span
  class={cn(
    'inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-semibold',
    variants[variant],
    className
  )}
>
  <slot />
</span>
