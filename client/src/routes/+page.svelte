<script module lang="ts">
  import type { PatientLedgerRow } from '$features/dashboard';
  import type { PageData } from './$types';
</script>

<script lang="ts">
  import { ClaimsLedger, MetricRibbon, Sidebar } from '$features/dashboard';
  import { Button } from '$ui/button';
  import { Input } from '$ui/input';
  import { SheetContent, SheetHeader, SheetOverlay, SheetTitle } from '$ui/sheet';
  import { Textarea } from '$ui/textarea';
  import { useAgent } from '$hooks/agent/use-agent.svelte';
  import { useMedia } from '$hooks/media/use-media.svelte';
  import { invalidateAll } from '$app/navigation';
  import { MIME } from '../types/media';
  import { fly } from 'svelte/transition';

  type Operation = 'upload' | 'search' | 'filter';

  let { data }: { data: PageData } = $props();
  let activeOperation = $state<Operation | null>(null);
  let searchQuery = $state('');
  let batchNotes = $state('');
  let uploadPanelOpen = $state(false);
  const agent = useAgent();
  const media = useMedia({
    types: [MIME.documents.types.pdf],
    extensions: [MIME.documents.extentions.pdf]
  });

  const closeUploadPanel = () => {
    uploadPanelOpen = false;
    media.clear();
  };

  const handleFileSelection = async (event: Event) => {
    const input = event.currentTarget as HTMLInputElement;

    if (input.files) {
      await media.addFiles(input.files);
      input.value = '';
    }
  };

  const handleSubmitBatch = (event: SubmitEvent) => {
    event.preventDefault();

    const documents = media.files
      .filter((file) => file.status === 'ready')
      .map((file) => ({
        id: file.id,
        name: file.name,
        type: file.type,
        extension: file.extension,
        size: file.size,
        base64: file.base64
      }));

    if (documents.length === 0) {
      return;
    }

    void agent
      .execute({
        batchId: crypto.randomUUID(),
        notes: batchNotes,
        documents
      })
      .then(() => invalidateAll());

    uploadPanelOpen = false;
    batchNotes = '';
    media.clear();
  };

  let patients = $derived(data.patients satisfies PatientLedgerRow[]);

  let visiblePatients = $derived(
    patients.filter((patient) => {
      const query = searchQuery.trim().toLowerCase();

      if (!query) {
        return true;
      }

      return [
        patient.patientName,
        patient.dob,
        patient.payerName,
        patient.memberId,
        ...patient.claims.flatMap((claim) => [
          claim.serviceCode,
          claim.serviceDesc,
          claim.dateOfService,
          claim.providerName,
          claim.reasonText,
          claim.actionLabel
        ])
      ]
        .join(' ')
        .toLowerCase()
        .includes(query);
    })
  );
</script>

<div class="relative flex h-screen w-screen overflow-hidden border-0 bg-[#fcfcfb]">
  <Sidebar />
  <main class="flex min-w-0 flex-1 flex-col gap-10 overflow-y-auto p-10">
    <header class="flex items-center justify-between border-b border-black/5 pb-6">
      <h1 class="m-0 text-2xl font-medium tracking-normal text-black">Insurance Explanations of Benefits (EOB)</h1>
      <div
        class="inline-flex min-h-[42px] items-center gap-1 rounded-full border border-[#0e0e0e] bg-[#151515] p-1 shadow-[0_14px_28px_rgba(0,0,0,0.08)]"
        role="toolbar"
        tabindex="0"
        aria-label="EOB operations"
        onmouseleave={() => (activeOperation = null)}
      >
        <button
          class={`group relative inline-flex size-[34px] cursor-pointer items-center justify-center rounded-full border-0 transition-[background-color,color,transform] duration-300 ease-out ${
            activeOperation === 'upload' ? '-translate-y-px bg-[#f4f4f1] text-[#151515]' : 'bg-transparent text-white hover:-translate-y-px hover:bg-[#f4f4f1] hover:text-[#151515]'
          }`}
          type="button"
          aria-label="Upload source document batch"
          onclick={() => {
            activeOperation = 'upload';
            uploadPanelOpen = true;
          }}
        >
          <svg class="size-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <path d="M12 16V4" />
            <path d="m7 9 5-5 5 5" />
            <path d="M5 20h14" />
          </svg>
          <span class="pointer-events-none absolute top-[calc(100%+8px)] left-1/2 z-10 -translate-x-1/2 -translate-y-1 rounded-full bg-[#151515] px-2.5 py-1 text-[11px] font-medium whitespace-nowrap text-white opacity-0 transition-all duration-300 group-hover:translate-y-0 group-hover:opacity-100">Upload</span>
        </button>

        <div class={`inline-flex items-center rounded-full transition-[gap,padding-right] duration-[340ms] ease-out ${activeOperation === 'search' ? 'gap-1.5 pr-2.5' : 'gap-0 pr-0'}`}>
          <button
            class={`group relative inline-flex size-[34px] cursor-pointer items-center justify-center rounded-full border-0 transition-[background-color,color,transform] duration-300 ease-out ${
              activeOperation === 'search' ? '-translate-y-px bg-[#f4f4f1] text-[#151515]' : 'bg-transparent text-white hover:-translate-y-px hover:bg-[#f4f4f1] hover:text-[#151515]'
            }`}
            type="button"
            aria-label="Search claims"
            onclick={() => (activeOperation = 'search')}
          >
            <svg class="size-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" aria-hidden="true">
              <circle cx="11" cy="11" r="7" />
              <path d="m20 20-4-4" />
            </svg>
            <span class="pointer-events-none absolute top-[calc(100%+8px)] left-1/2 z-10 -translate-x-1/2 -translate-y-1 rounded-full bg-[#151515] px-2.5 py-1 text-[11px] font-medium whitespace-nowrap text-white opacity-0 transition-all duration-300 group-hover:translate-y-0 group-hover:opacity-100">Search</span>
          </button>

          {#if activeOperation === 'search'}
            <span in:fly={{ x: -10, duration: 340 }} out:fly={{ x: -10, duration: 220 }}>
              <Input
                class="h-7 w-[190px] rounded-full border-0 bg-[#f4f4f1] px-3 text-[13px] text-[#151515] outline-none placeholder:text-[#848481]"
                type="search"
                bind:value={searchQuery}
                placeholder="Search claims"
                ariaLabel="Search claims"
              />
            </span>
          {/if}
        </div>

        <button
          class={`group relative inline-flex size-[34px] cursor-pointer items-center justify-center rounded-full border-0 transition-[background-color,color,transform] duration-300 ease-out ${
            activeOperation === 'filter' ? '-translate-y-px bg-[#f4f4f1] text-[#151515]' : 'bg-transparent text-white hover:-translate-y-px hover:bg-[#f4f4f1] hover:text-[#151515]'
          }`}
          type="button"
          aria-label="Filter claims"
          onclick={() => (activeOperation = 'filter')}
        >
          <svg class="size-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <path d="M4 6h16" />
            <path d="M7 12h10" />
            <path d="M10 18h4" />
          </svg>
          <span class="pointer-events-none absolute top-[calc(100%+8px)] left-1/2 z-10 -translate-x-1/2 -translate-y-1 rounded-full bg-[#151515] px-2.5 py-1 text-[11px] font-medium whitespace-nowrap text-white opacity-0 transition-all duration-300 group-hover:translate-y-0 group-hover:opacity-100">Filter</span>
        </button>
      </div>
      {#if agent.status === 'running'}
        <p class="m-0 text-right text-xs font-semibold text-[#848481]">Agent run started</p>
      {:else if agent.status === 'failed' && agent.error}
        <p class="m-0 text-right text-xs font-semibold text-[#c93b2b]">{agent.error}</p>
      {/if}
    </header>

    <MetricRibbon metrics={data.metrics} />
    <ClaimsLedger patients={visiblePatients} />
  </main>

  {#if uploadPanelOpen}
    <SheetOverlay
      class="fixed inset-0 z-10 cursor-pointer border-0 bg-[#151515]/18"
      ariaLabel="Close upload panel"
      onclick={closeUploadPanel}
    />
    <SheetContent
      class="fixed top-0 right-0 z-11 flex h-screen w-[min(420px,100vw)] flex-col gap-7 border-l border-black/6 bg-[#fcfcfb] p-[34px] shadow-[-24px_0_60px_rgba(0,0,0,0.12)]"
      ariaLabel="Upload EOB documents"
    >
      <SheetHeader class="flex items-start justify-between gap-6 border-b border-black/5 pb-[22px]">
        <div>
          <p class="m-0 mb-[7px] text-[11px] font-semibold tracking-[0.5px] text-[#848481] uppercase">New batch</p>
          <SheetTitle class="m-0 text-[22px] font-medium tracking-normal text-black">Upload source files</SheetTitle>
        </div>
        <Button
          class="inline-flex size-[34px] cursor-pointer items-center justify-center rounded-full border border-transparent bg-[#f4f4f1] text-[#151515] transition-colors duration-200 hover:border-[#d4d4d0] hover:bg-white"
          type="button"
          ariaLabel="Close upload panel"
          onclick={closeUploadPanel}
        >
          <svg class="size-[17px]" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" aria-hidden="true">
            <path d="M6 6l12 12" />
            <path d="M18 6 6 18" />
          </svg>
        </Button>
      </SheetHeader>

      <form class="flex flex-1 flex-col gap-[22px]" onsubmit={handleSubmitBatch}>
        <label class="flex min-h-[210px] cursor-pointer flex-col items-center justify-center gap-2.5 rounded-[22px] border border-dashed border-[#c8c8c2] bg-[#f5f5f3] p-7 text-center text-[#151515] transition-colors duration-200 hover:border-[#151515] hover:bg-white">
          <input
            class="pointer-events-none absolute size-px opacity-0"
            type="file"
            multiple
            accept={`${MIME.documents.types.pdf},${MIME.documents.extentions.pdf}`}
            aria-label="Upload multiple source documents"
            onchange={handleFileSelection}
          />
          <span class="inline-flex size-[46px] items-center justify-center rounded-full bg-[#151515] text-white">
            <svg class="size-[22px]" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
              <path d="M12 15V3" />
              <path d="m7 8 5-5 5 5" />
              <path d="M4 17v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-2" />
            </svg>
          </span>
          <span class="text-[15px] font-semibold text-black">Select or drop files</span>
          <span class="max-w-60 text-xs leading-6 text-[#848481]">PDF files are validated in memory before submission</span>
        </label>

        {#if media.files.length > 0}
          <div class="flex max-h-[168px] flex-col gap-2 overflow-y-auto rounded-2xl border border-black/5 bg-white p-2.5">
            {#each media.files as file (file.id)}
              <div class="flex items-center justify-between gap-3 rounded-xl bg-[#f5f5f3] px-3 py-2.5">
                <div class="min-w-0">
                  <p class="m-0 truncate text-[13px] font-semibold text-[#151515]">{file.name}</p>
                  <p class={`m-0 mt-1 text-[11px] ${file.status === 'ready' ? 'text-[#10b981]' : 'text-[#c93b2b]'}`}>
                    {file.status === 'ready' ? `${media.formatBytes(file.size)} encoded in memory` : file.error}
                  </p>
                </div>
                <Button
                  class="inline-flex size-7 shrink-0 cursor-pointer items-center justify-center rounded-full border border-transparent bg-white text-[#151515] transition-colors duration-200 hover:border-[#d4d4d0]"
                  type="button"
                  ariaLabel={`Remove ${file.name}`}
                  onclick={() => media.removeFile(file.id)}
                >
                  <svg class="size-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true">
                    <path d="M6 6l12 12" />
                    <path d="M18 6 6 18" />
                  </svg>
                </Button>
              </div>
            {/each}
          </div>
        {/if}

        <label class="flex flex-col gap-2">
          <span class="text-[11px] font-semibold tracking-[0.5px] text-[#848481] uppercase">Batch notes</span>
          <Textarea
            class="min-h-[150px] resize-y rounded-2xl border border-transparent bg-[#f5f5f3] px-[15px] py-3.5 text-[13px] leading-normal text-[#151515] outline-none transition-colors duration-200 placeholder:text-[#848481] focus:border-[#151515] focus:bg-white"
            placeholder="Add payer, date range, or routing notes for this upload"
            bind:value={batchNotes}
          />
        </label>

        <div class="mt-auto flex justify-end gap-2.5 pt-[18px]">
          <Button
            class="h-[38px] cursor-pointer rounded-full border border-[#d4d4d0] bg-white px-4 text-[13px] font-semibold text-[#151515] transition-colors duration-200 hover:border-[#151515]"
            type="button"
            onclick={closeUploadPanel}
          >
            Cancel
          </Button>
          <Button
            class="h-[38px] cursor-pointer rounded-full border border-[#151515] bg-[#151515] px-4 text-[13px] font-semibold text-white transition-colors duration-200 hover:border-[#2a2a2a] hover:bg-[#2a2a2a]"
            type="submit"
            disabled={media.files.filter((file) => file.status === 'ready').length === 0 || agent.status === 'running'}
          >
            Submit batch
          </Button>
        </div>
      </form>
    </SheetContent>
  {/if}
</div>
