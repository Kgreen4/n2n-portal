<script module lang="ts">
  export type UserRow = {
    name: string;
    phone: string;
    dealership: string;
    badge?: string;
    toggle: boolean;
    roles: string[];
    status: string;
    hasApprove?: boolean;
    inactiveStyle?: boolean;
  };
</script>

<script lang="ts">
  import { Badge } from '$ui/badge';
  import { Button } from '$ui/button';
  import ContextMenu from './context-menu.svelte';

  let { users = [] }: { users: UserRow[] } = $props();
  let openMenuIndex = $state<number | null>(null);
  let hoverMenuIndex = $state<number | null>(null);

  const headerClass = 'sticky top-0 z-2 border-b border-[#e6e6e4] bg-[#f9f9f7] px-4 py-3 text-left text-[13px] font-medium text-[#757573]';
  const cellClass = 'border-b border-[#e6e6e4] p-4 align-top';

  const toggleMenu = (index: number) => {
    openMenuIndex = openMenuIndex === index ? null : index;
  };
</script>

<div class="min-h-0 w-full flex-1 overflow-auto border-t border-[#e6e6e4]">
  <table class="w-full border-collapse text-left text-[13px]">
    <thead>
      <tr>
        <th class={headerClass}>Name <span>▲▼</span></th>
        <th class={headerClass}>Dealerships <span>▲▼</span></th>
        <th class={headerClass}>Role</th>
        <th class={headerClass}>Status</th>
        <th class={headerClass}></th>
      </tr>
    </thead>
    <tbody>
      {#each users as user, index}
        <tr>
          <td class={cellClass}>
            <div class="flex flex-col">
              <span class="flex items-center gap-1.5 font-semibold">
                <svg
                  width="14"
                  height="14"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  stroke-width="2"
                ><path d="m6 9 6 6 6-6" /></svg>
                {user.name}
              </span>
              <span class="mt-0.5 text-[#757573]">{user.phone}</span>
            </div>
          </td>
          <td class={cellClass}>
            <div class="flex items-center gap-1.5">
              <span class={`relative h-2.5 w-4 rounded-full after:absolute after:top-0.5 after:size-1.5 after:rounded-full after:bg-white ${user.toggle ? 'bg-[#111] after:right-0.5' : 'bg-[#ccc] after:left-0.5'}`}></span>
              <span class={user.inactiveStyle ? 'text-[#757573]' : ''}>{user.dealership}</span>
              {#if user.badge}
                <Badge variant="secondary" class="rounded-md border-0 bg-[#edebe8] px-1.5 py-0.5 text-[11px] font-semibold text-[#111]">{user.badge}</Badge>
              {/if}
            </div>
          </td>
          <td class={cellClass}>
            <div class={`flex flex-col ${user.inactiveStyle ? 'text-[#757573]' : ''}`}>
              {#each user.roles as role, roleIndex}
                <span class={roleIndex > 0 ? 'mt-0.5 text-[#757573]' : ''}>{role}</span>
              {/each}
            </div>
          </td>
          <td class={cellClass}>
            <span class={user.status === 'Active' ? '' : 'mt-0.5 text-[#757573]'}>{user.status}</span>
          </td>
          <td class={`${cellClass} text-right`}>
            <div
              class="relative inline-flex gap-2"
              role="group"
              onmouseenter={() => (hoverMenuIndex = index)}
              onmouseleave={() => (hoverMenuIndex = null)}
              onfocusin={() => (hoverMenuIndex = index)}
              onfocusout={() => (hoverMenuIndex = null)}
            >
              {#if user.hasApprove}
                <Button variant="outline" class="h-auto cursor-pointer rounded-2xl border border-[#111] bg-transparent px-3.5 py-1.5 text-xs font-semibold">Approve</Button>
              {/if}
              <Button
                variant="secondary"
                size="icon"
                class="size-7 cursor-pointer rounded-full border-0 bg-[#edebe8]"
                onclick={() => toggleMenu(index)}
                ariaLabel={`Open actions for ${user.name}`}
              >
                ···
              </Button>
              {#if openMenuIndex === index || hoverMenuIndex === index}
                <ContextMenu />
              {/if}
            </div>
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
</div>
