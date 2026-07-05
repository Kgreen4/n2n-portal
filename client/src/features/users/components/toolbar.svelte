<script lang="ts">
  import { Button } from '$ui/button';

  let {
    currentTab = $bindable('Customers'),
    searchQuery = $bindable(''),
    onAdd
  }: {
    currentTab: string;
    searchQuery: string;
    onAdd: () => void;
  } = $props();
</script>

<div class="mb-5 flex shrink-0 items-center justify-between">
  <div class="flex rounded-lg bg-[#edebe8] p-[3px]">
    <Button
      variant="ghost"
      class={`h-auto rounded-md border-0 px-3 py-1.5 text-[13px] font-medium ${currentTab === 'Customers' ? 'bg-white text-[#111111]' : 'bg-transparent text-[#757573]'}`}
      onclick={() => (currentTab = 'Customers')}
    >
      Customers
    </Button>
    <Button
      variant="ghost"
      class={`h-auto rounded-md border-0 px-3 py-1.5 text-[13px] font-medium ${currentTab === 'Employees' ? 'bg-white text-[#111111]' : 'bg-transparent text-[#757573]'}`}
      onclick={() => (currentTab = 'Employees')}
    >
      Employees
    </Button>
  </div>

  <div class="flex gap-2">
    <div class="relative">
      <svg
        class="absolute top-1/2 left-3 -translate-y-1/2 text-[#969694]"
        width="16"
        height="16"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width="2"
      >
        <circle cx="11" cy="11" r="8" /><path d="m21 21-4.3-4.3" />
      </svg>
      <input
        type="text"
        class="w-60 rounded-full border-0 bg-[#edebe8] py-2.5 pr-3 pl-9 text-[13px] outline-none"
        placeholder="Search for customers..."
        bind:value={searchQuery}
      />
    </div>
    <Button variant="secondary" class="h-auto rounded-full border-0 bg-[#edebe8] px-[18px] py-2.5 text-[13px] font-medium">Filters</Button>
    <Button class="h-auto rounded-full border-0 bg-[#1a1a1a] px-[18px] py-2.5 text-[13px] font-medium text-white" onclick={onAdd}>Add new customer</Button>
  </div>
</div>
