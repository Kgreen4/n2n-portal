<script module lang="ts">
  export type ClaimRow = {
    claim_number: string;
    patient_id: string;
    provider_name: string | null;
    date_of_service: string;
    cpt_hcpcs_code: string | null;
    service_description: string | null;
    billed_amount: number;
    allowed_amount: number;
    disallowed_amount: number;
    co_pay: number;
    deductible: number;
    co_insurance: number;
    discount_amount: number;
    paid_amount: number;
    patient_responsibility: number;
    explanation_code: string | null;
    created_at: string;
  };

  export type PatientRow = {
    id: string;
    first_name: string;
    last_name: string;
    date_of_birth: string | null;
    insurance_member_id: string;
    created_at: string;
    claims: ClaimRow[];
  };
</script>

<script lang="ts">
  import { Button } from '$ui/button';

  let { patients = [] }: { patients: PatientRow[] } = $props();
  let expandedPatientIds = $state<Set<string>>(new Set());

  const headerClass = 'sticky top-0 z-2 border-b border-[#e6e6e4] bg-[#f9f9f7] px-4 py-3 text-left text-[13px] font-medium text-[#757573]';
  const cellClass = 'border-b border-[#e6e6e4] p-4 align-top leading-[1.4]';
  const claimHeaderClass = 'border-b border-[#e6e6e4] bg-[#f9f9f7] py-2.5 pr-3 pb-[9px] text-left text-xs font-semibold whitespace-nowrap text-[#757573]';
  const claimCellClass = 'border-b border-[#eeeeeb] py-[11px] pr-3 whitespace-nowrap';

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD'
    }).format(value);
  };

  const patientTotal = (patient: PatientRow) => {
    return patient.claims.reduce((total, claim) => total + claim.billed_amount, 0);
  };

  const claimTotal = (patient: PatientRow, key: keyof Pick<ClaimRow, 'allowed_amount' | 'paid_amount' | 'patient_responsibility'>) => {
    return patient.claims.reduce((total, claim) => total + claim[key], 0);
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

<div class="min-h-0 w-full flex-1 overflow-auto border-t border-[#e6e6e4]">
  <table class="w-full border-collapse text-left text-[13px]">
    <thead>
      <tr>
        <th class={headerClass}>First name</th>
        <th class={headerClass}>Last name</th>
        <th class={headerClass}>Date of birth</th>
        <th class={headerClass}>Insurance member ID</th>
        <th class={headerClass}>Created at</th>
        <th class={headerClass}>Total billed</th>
        <th class={headerClass}></th>
      </tr>
    </thead>
    <tbody>
      {#each patients as patient}
        <tr>
          <td class={cellClass}>
            <div class="flex items-center gap-2">
              <Button
                variant="ghost"
                size="icon"
                class="size-6 border-0 bg-transparent text-lg text-[#757573]"
                onclick={() => togglePatient(patient.id)}
                ariaLabel={`${expandedPatientIds.has(patient.id) ? 'Collapse' : 'Expand'} claims for ${patient.first_name} ${patient.last_name}`}
              >
                {expandedPatientIds.has(patient.id) ? '⌄' : '›'}
              </Button>
              <span class="font-semibold">{patient.first_name}</span>
            </div>
          </td>
          <td class={cellClass}>
            <span class="font-semibold">{patient.last_name}</span>
          </td>
          <td class={cellClass}>
            <span class={patient.date_of_birth ? '' : 'mt-0.5 text-xs text-[#757573]'}>{patient.date_of_birth ?? 'Not provided'}</span>
          </td>
          <td class={cellClass}>
            <span class="rounded-md bg-[#edebe8] px-2 py-[3px] text-xs font-semibold text-[#111111]">{patient.insurance_member_id}</span>
          </td>
          <td class={cellClass}>
            <span class="mt-0.5 text-xs text-[#757573]">{patient.created_at}</span>
          </td>
          <td class={cellClass}>
            <span class="font-bold tracking-normal text-[#111111] tabular-nums">{formatCurrency(patientTotal(patient))}</span>
          </td>
          <td class={`${cellClass} text-right`}>
            <div class="inline-flex items-center gap-2">
              <Button variant="outline" class="h-auto cursor-pointer rounded-2xl border border-[#111111] bg-transparent px-3.5 py-1.5 text-xs font-semibold">View</Button>
              <Button variant="secondary" size="icon" class="size-7 cursor-pointer rounded-full border-0 bg-[#edebe8] font-bold">···</Button>
            </div>
          </td>
        </tr>
        {#if expandedPatientIds.has(patient.id)}
          <tr>
            <td class="border-b border-[#e6e6e4] bg-[#f9f9f7] pt-0 pr-4 pb-[18px] pl-14" colspan="7">
              <div class="overflow-visible border-l-2 border-[#d8d8d4] bg-transparent pl-4">
                <div class="flex flex-wrap gap-4 border-b border-[#e6e6e4] pt-0.5 pb-2.5 text-xs font-semibold text-[#757573]">
                  <span>{patient.claims.length} claims</span>
                  <span>Allowed <strong class="font-bold text-[#111111] tabular-nums">{formatCurrency(claimTotal(patient, 'allowed_amount'))}</strong></span>
                  <span>Paid <strong class="font-bold text-[#1e7e34] tabular-nums">{formatCurrency(claimTotal(patient, 'paid_amount'))}</strong></span>
                  <span>Patient responsibility <strong class="font-bold text-[#8a5a00] tabular-nums">{formatCurrency(claimTotal(patient, 'patient_responsibility'))}</strong></span>
                </div>

                <div class="overflow-x-auto">
                  <table class="w-full min-w-[1500px] border-collapse text-left text-xs">
                    <thead>
                      <tr>
                        <th class={claimHeaderClass}>Claim #</th>
                        <th class={claimHeaderClass}>Provider</th>
                        <th class={claimHeaderClass}>DOS</th>
                        <th class={claimHeaderClass}>CPT/HCPCS</th>
                        <th class={claimHeaderClass}>Description</th>
                        <th class={claimHeaderClass}>Billed</th>
                        <th class={claimHeaderClass}>Allowed</th>
                        <th class={claimHeaderClass}>Disallowed</th>
                        <th class={claimHeaderClass}>Co-pay</th>
                        <th class={claimHeaderClass}>Deductible</th>
                        <th class={claimHeaderClass}>Co-ins</th>
                        <th class={claimHeaderClass}>Discount</th>
                        <th class={claimHeaderClass}>Paid</th>
                        <th class={claimHeaderClass}>Patient resp.</th>
                        <th class={claimHeaderClass}>Explanation</th>
                        <th class={claimHeaderClass}>Created at</th>
                      </tr>
                    </thead>
                    <tbody>
                      {#each patient.claims as claim}
                        <tr class="[&:last-child_td]:border-b-0">
                          <td class={claimCellClass}><span class="font-bold">{claim.claim_number}</span></td>
                          <td class={claimCellClass}>{claim.provider_name ?? 'Not provided'}</td>
                          <td class={claimCellClass}>{claim.date_of_service}</td>
                          <td class={claimCellClass}>{claim.cpt_hcpcs_code ?? '-'}</td>
                          <td class={claimCellClass}>{claim.service_description ?? '-'}</td>
                          <td class={claimCellClass}><span class="font-semibold tracking-normal text-[#111111] tabular-nums">{formatCurrency(claim.billed_amount)}</span></td>
                          <td class={claimCellClass}><span class="font-semibold tracking-normal text-[#1f5f7a] tabular-nums">{formatCurrency(claim.allowed_amount)}</span></td>
                          <td class={claimCellClass}><span class="font-semibold tracking-normal text-[#757573] tabular-nums">{formatCurrency(claim.disallowed_amount)}</span></td>
                          <td class={claimCellClass}><span class="font-semibold tracking-normal text-[#757573] tabular-nums">{formatCurrency(claim.co_pay)}</span></td>
                          <td class={claimCellClass}><span class="font-semibold tracking-normal text-[#757573] tabular-nums">{formatCurrency(claim.deductible)}</span></td>
                          <td class={claimCellClass}><span class="font-semibold tracking-normal text-[#757573] tabular-nums">{formatCurrency(claim.co_insurance)}</span></td>
                          <td class={claimCellClass}><span class="font-semibold tracking-normal text-[#757573] tabular-nums">{formatCurrency(claim.discount_amount)}</span></td>
                          <td class={claimCellClass}><span class="font-semibold tracking-normal text-[#1e7e34] tabular-nums">{formatCurrency(claim.paid_amount)}</span></td>
                          <td class={claimCellClass}><span class="font-semibold tracking-normal text-[#8a5a00] tabular-nums">{formatCurrency(claim.patient_responsibility)}</span></td>
                          <td class={claimCellClass}>{claim.explanation_code ?? '-'}</td>
                          <td class={claimCellClass}>{claim.created_at}</td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                </div>
              </div>
            </td>
          </tr>
        {/if}
      {/each}
    </tbody>
  </table>
</div>
