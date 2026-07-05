<script lang="ts">
  import { cn } from '$utils';

  type ButtonVariant = 'default' | 'secondary' | 'outline' | 'ghost';
  type ButtonSize = 'default' | 'sm' | 'icon';

  export let type: 'button' | 'submit' | 'reset' = 'button';
  export let variant: ButtonVariant = 'default';
  export let size: ButtonSize = 'default';
  export let disabled = false;
  export let onclick: ((event: MouseEvent) => void) | undefined = undefined;
  export let ariaLabel: string | undefined = undefined;
  let className = '';
  export { className as class };

  const variants: Record<ButtonVariant, string> = {
    default: 'border-transparent bg-slate-950 text-white hover:bg-slate-800',
    secondary: 'border-transparent bg-slate-100 text-slate-950 hover:bg-slate-200',
    outline: 'border-slate-200 bg-white text-slate-950 hover:bg-slate-50',
    ghost: 'border-transparent bg-transparent text-slate-950 hover:bg-slate-100'
  };

  const sizes: Record<ButtonSize, string> = {
    default: 'h-10 px-4 py-2',
    sm: 'h-9 px-3',
    icon: 'h-10 w-10'
  };
</script>

<button
  {type}
  {disabled}
  {onclick}
  aria-label={ariaLabel}
  class={cn(
    'inline-flex items-center justify-center rounded-md border text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-400 disabled:pointer-events-none disabled:opacity-50',
    variants[variant],
    sizes[size],
    className
  )}
>
  <slot />
</button>
