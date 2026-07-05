<script lang="ts">
  import { goto } from '$app/navigation';
  import { Button } from '$ui/button';

  const navItems = [
    { label: 'Dashboard', href: '/' },
    { label: 'Documents', href: '/documents' },
    { label: 'Reports', href: '/reports' }
  ];
  let { activeMenu = $bindable('Dashboard') }: { activeMenu?: string } = $props();

  const navigate = (label: string, href: string) => {
    activeMenu = label;
    void goto(href);
  };
</script>

<header class="mb-8 flex shrink-0 items-center justify-between">
  <div class="flex items-center gap-3">
    <div class="rounded-full bg-[#1a1a1a] px-[18px] py-2 text-sm font-semibold text-white">Exobee</div>
    <div class="flex gap-1 rounded-full bg-[#edebe8] p-1">
      <Button variant="ghost" class="inline-flex h-auto items-center gap-1 rounded-2xl border-0 bg-transparent px-3.5 py-1.5 text-[13px] font-medium text-[#757573]">
        Settings
        <svg
          width="12"
          height="12"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
        ><path d="m6 9 6 6 6-6" /></svg>
      </Button>
      {#each navItems as item}
        <Button
          variant="ghost"
          class={`h-auto rounded-2xl border-0 px-3.5 py-1.5 text-[13px] font-medium ${
            activeMenu === item.label ? 'bg-[#1a1a1a] text-white' : 'bg-transparent text-[#757573]'
          }`}
          onclick={() => navigate(item.label, item.href)}
        >
          {item.label}
        </Button>
      {/each}
    </div>
  </div>
  <Button variant="secondary" class="h-auto rounded-full border-0 bg-[#edebe8] px-4 py-2 text-[13px] font-semibold">Log out</Button>
</header>
