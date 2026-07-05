<script lang="ts">
  import { Button } from '$ui/button';
  import { Input } from '$ui/input';

  let {
    currentTab = $bindable('Patients'),
    searchQuery = $bindable(''),
    onAdd
  }: {
    currentTab: string;
    searchQuery: string;
    onAdd: () => void;
  } = $props();
</script>

<div class="mb-6 flex shrink-0 items-center justify-between">
  <div class="flex gap-5">
    <Button
      variant="ghost"
      class={`h-auto rounded-none border-0 bg-transparent px-0 pb-2 text-base font-medium ${currentTab === 'Patients' ? 'border-b-2 border-[#1a1a1a] text-[#111111]' : 'text-[#757573]'}`}
      onclick={() => (currentTab = 'Patients')}
    >
      Patients
    </Button>
    <Button
      variant="ghost"
      class={`h-auto rounded-none border-0 bg-transparent px-0 pb-2 text-base font-medium ${currentTab === 'Transaction Ledger' ? 'border-b-2 border-[#1a1a1a] text-[#111111]' : 'text-[#757573]'}`}
      onclick={() => (currentTab = 'Transaction Ledger')}
    >
      Transaction Ledger
    </Button>
  </div>

  <div class="flex items-center gap-3">
    <div class="relative">
      <svg class="absolute top-1/2 left-3 -translate-y-1/2 text-[#757573]" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
      </svg>
      <Input
        type="text"
        class="w-60 rounded-full border-0 bg-[#edebe8] py-2.5 pr-3 pl-9 text-[13px] outline-none"
        placeholder="Search patients..."
        bind:value={searchQuery}
      />
    </div>
    <Button variant="secondary" class="flex h-auto items-center gap-1.5 rounded-full border-0 bg-[#edebe8] px-[18px] py-2.5 text-[13px] font-medium text-[#111111]">
      Filters
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" />
      </svg>
    </Button>
    <Button class="flex h-auto items-center gap-1.5 rounded-full border-0 bg-[#1a1a1a] px-[18px] py-2.5 text-[13px] font-medium text-white" onclick={onAdd}>
      Add patient
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
        <line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
      </svg>
    </Button>
  </div>
</div>
