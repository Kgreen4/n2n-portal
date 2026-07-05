<script module lang="ts">
  export type Claim = {
    serviceCode: string;
    serviceDesc: string;
    dateOfService: string;
    providerName: string;
    allowedAmt: number;
    insurancePaid: number;
    patientOwes: number;
    owesLabel: string;
    statusClass: 'clean' | 'denied';
    reasonText: string;
    actionLabel: string;
    fixRequired: boolean;
  };

  export type PatientLedgerRow = {
    id: string;
    patientName: string;
    dob: string;
    payerName: string;
    memberId: string;
    claimCount: number;
    totalAllowed: number;
    totalInsurancePaid: number;
    totalPatientOwes: number;
    deniedClaimsValue: number;
    hasFixRequired: boolean;
    claims: Claim[];
  };
</script>

<script lang="ts">
  import { Button } from '$ui/button';

  let { patients }: { patients: PatientLedgerRow[] } = $props();
  const pageSizeOptions = [25, 50, 100, 250] as const;
  let pageSize = $state(50);
  let currentPage = $state(1);
  let expandedPatientIds = $state<Set<string>>(new Set());
  let totalPages = $derived(Math.max(1, Math.ceil(patients.length / pageSize)));
  let safePage = $derived(Math.min(currentPage, totalPages));
  let pageStart = $derived((safePage - 1) * pageSize);
  let pageEnd = $derived(Math.min(pageStart + pageSize, patients.length));
  let visiblePatients = $derived(patients.slice(pageStart, pageEnd));

  $effect(() => {
    if (currentPage > totalPages) {
      currentPage = totalPages;
    }
  });

  const previousPage = () => {
    currentPage = Math.max(1, currentPage - 1);
  };

  const nextPage = () => {
    currentPage = Math.min(totalPages, currentPage + 1);
  };

  const handlePageSizeChange = (event: Event) => {
    pageSize = Number((event.currentTarget as HTMLSelectElement).value);
    currentPage = 1;
  };

  const togglePatient = (patientId: string) => {
    const next = new Set(expandedPatientIds);
    if (next.has(patientId)) {
      next.delete(patientId);
    } else {
      next.add(patientId);
    }
    expandedPatientIds = next;
  };
</script>

<div class="w-full">
  <div class="w-full">
    <table class="w-full border-collapse text-left text-[13px]">
      <thead>
        <tr>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Patient Details</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Insurance Payer / ID</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Claims</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Allowed Total</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Ins. Paid</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Patient Owes</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Status</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]"></th>
        </tr>
      </thead>
      <tbody>
        {#each visiblePatients as patient}
          <tr class="hover:bg-black/[0.005]">
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle leading-tight text-black">
              <div class="flex items-center gap-2">
                <Button
                  class="inline-flex size-6 cursor-pointer items-center justify-center rounded-full border border-[#d4d4d0] bg-transparent text-sm leading-none text-black"
                  type="button"
                  ariaLabel={`${expandedPatientIds.has(patient.id) ? 'Collapse' : 'Expand'} claims for ${patient.patientName}`}
                  onclick={() => togglePatient(patient.id)}
                >
                  {expandedPatientIds.has(patient.id) ? '⌄' : '›'}
                </Button>
                <div>
                  <span class="text-sm font-medium">{patient.patientName}</span>
                  <div class="mt-0.5 text-[11px] text-[#848481]">DOB: {patient.dob}</div>
                </div>
              </div>
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle font-mono leading-tight text-black tabular-nums">
              {patient.payerName}
              <div class="mt-0.5 text-[11px] text-[#848481]">ID: {patient.memberId}</div>
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle font-mono leading-tight text-black tabular-nums">
              {patient.claimCount}
              <div class="mt-0.5 text-[11px] text-[#848481]">claim lines</div>
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle font-mono leading-tight text-black tabular-nums">${patient.totalAllowed.toFixed(2)}</td>
            <td
              class={`border-b border-black/5 px-3.5 py-3.5 align-middle font-mono leading-tight tabular-nums ${
                patient.hasFixRequired ? 'font-medium text-[#c93b2b]' : patient.totalInsurancePaid > 0 ? 'font-medium text-[#10b981]' : 'text-black'
              }`}
            >
              ${patient.totalInsurancePaid.toFixed(2)}
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle font-mono leading-tight text-black tabular-nums">
              ${patient.totalPatientOwes.toFixed(2)}
              <div class="mt-0.5 text-[11px] text-[#848481]">Responsibility</div>
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle leading-tight text-black">
              <div class="inline-flex items-center gap-1.5">
                <span class={`size-1.5 rounded-full ${patient.hasFixRequired ? 'bg-[#c93b2b]' : 'bg-[#10b981]'}`}></span>
                <span class={`block max-w-[200px] text-xs ${patient.hasFixRequired ? 'font-medium text-[#c93b2b]' : 'text-black'}`}>
                  {patient.hasFixRequired ? `${patient.deniedClaimsValue.toFixed(2)} requires appeal` : 'Processed'}
                </span>
              </div>
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 text-right align-middle leading-tight text-black">
              <Button
                class={`cursor-pointer rounded border px-3 py-1.5 text-xs font-medium transition-all duration-150 ${
                  patient.hasFixRequired
                    ? 'border-[#c93b2b] bg-[#fff1f0] text-[#c93b2b]'
                    : 'border-[#d4d4d0] bg-transparent text-black hover:border-black hover:bg-white'
                }`}
              >
                {patient.hasFixRequired ? 'Fix & Appeal' : 'Post to Chart'}
              </Button>
            </td>
          </tr>
          {#if expandedPatientIds.has(patient.id)}
            <tr>
              <td class="border-b border-black/5 bg-[#fbfbfa] px-3.5 py-4" colspan="8">
                <div class="overflow-x-auto border-l-2 border-[#d8d8d4] pl-4">
                  <table class="w-full min-w-[980px] border-collapse text-left text-xs">
                    <thead>
                      <tr>
                        <th class="border-b border-black/5 py-2 pr-3 font-medium text-[#848481]">DOS</th>
                        <th class="border-b border-black/5 py-2 pr-3 font-medium text-[#848481]">Provider</th>
                        <th class="border-b border-black/5 py-2 pr-3 font-medium text-[#848481]">Service / Code</th>
                        <th class="border-b border-black/5 py-2 pr-3 font-medium text-[#848481]">Allowed</th>
                        <th class="border-b border-black/5 py-2 pr-3 font-medium text-[#848481]">Paid</th>
                        <th class="border-b border-black/5 py-2 pr-3 font-medium text-[#848481]">Patient owes</th>
                        <th class="border-b border-black/5 py-2 pr-3 font-medium text-[#848481]">Reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {#each patient.claims as claim}
                        <tr class="[&:last-child_td]:border-b-0">
                          <td class="border-b border-black/5 py-2.5 pr-3 font-mono tabular-nums">{claim.dateOfService}</td>
                          <td class="border-b border-black/5 py-2.5 pr-3">{claim.providerName}</td>
                          <td class="border-b border-black/5 py-2.5 pr-3">
                            <span class="font-semibold">{claim.serviceCode}</span>
                            <div class="mt-0.5 text-[#848481]">{claim.serviceDesc}</div>
                          </td>
                          <td class="border-b border-black/5 py-2.5 pr-3 font-mono tabular-nums">${claim.allowedAmt.toFixed(2)}</td>
                          <td class={`border-b border-black/5 py-2.5 pr-3 font-mono tabular-nums ${claim.fixRequired ? 'font-semibold text-[#c93b2b]' : claim.insurancePaid > 0 ? 'font-semibold text-[#10b981]' : 'text-black'}`}>
                            ${claim.insurancePaid.toFixed(2)}
                          </td>
                          <td class="border-b border-black/5 py-2.5 pr-3 font-mono tabular-nums">
                            ${claim.patientOwes.toFixed(2)}
                            <div class="mt-0.5 text-[#848481]">{claim.owesLabel}</div>
                          </td>
                          <td class={`border-b border-black/5 py-2.5 pr-3 ${claim.fixRequired ? 'font-medium text-[#c93b2b]' : 'text-black'}`}>{claim.reasonText}</td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                </div>
              </td>
            </tr>
          {/if}
        {/each}
      </tbody>
    </table>
  </div>
  <div class="flex items-center justify-between border-t border-black/5 pt-4 text-xs text-[#848481]">
    <span class="font-mono tabular-nums">
      Showing {patients.length === 0 ? 0 : pageStart + 1}-{pageEnd} of {patients.length}
    </span>
    <div class="inline-flex items-center gap-3">
      <label class="inline-flex items-center gap-2">
        <span>Rows</span>
        <span class="relative inline-flex h-8 w-[74px] items-center">
          <select
            class="h-full w-full cursor-pointer appearance-none rounded-full border border-[#d4d4d0] bg-transparent pr-8 pl-4 text-center text-xs font-medium text-black outline-none"
            value={pageSize}
            onchange={handlePageSizeChange}
            aria-label="Rows per page"
          >
            {#each pageSizeOptions as option}
              <option value={option}>{option}</option>
            {/each}
          </select>
          <svg class="pointer-events-none absolute right-3 size-3.5 text-black" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <path d="m5 7 5 5 5-5" />
          </svg>
        </span>
      </label>
      <Button
        class="h-8 cursor-pointer rounded-full border border-[#d4d4d0] bg-transparent px-3 text-xs font-medium text-black transition-colors duration-150 disabled:cursor-not-allowed disabled:opacity-40"
        type="button"
        disabled={safePage === 1}
        onclick={previousPage}
      >
        Previous
      </Button>
      <span class="font-mono tabular-nums">Page {safePage} / {totalPages}</span>
      <Button
        class="h-8 cursor-pointer rounded-full border border-[#d4d4d0] bg-transparent px-3 text-xs font-medium text-black transition-colors duration-150 disabled:cursor-not-allowed disabled:opacity-40"
        type="button"
        disabled={safePage === totalPages}
        onclick={nextPage}
      >
        Next
      </Button>
    </div>
  </div>
</div>
