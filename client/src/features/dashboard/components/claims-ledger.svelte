<script module lang="ts">
  export type Claim = {
    patientName: string;
    dob: string;
    payerName: string;
    memberId: string;
    serviceCode: string;
    serviceDesc: string;
    allowedAmt: number;
    insurancePaid: number;
    patientOwes: number;
    owesLabel: string;
    statusClass: 'clean' | 'denied';
    reasonText: string;
    actionLabel: string;
    fixRequired: boolean;
  };
</script>

<script lang="ts">
  import { Button } from '$ui/button';

  let { claims }: { claims: Claim[] } = $props();
</script>

<div class="w-full">
  <div class="w-full">
    <table class="w-full border-collapse text-left text-[13px]">
      <thead>
        <tr>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Patient Details</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Insurance Payer / ID</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Service / Code</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Allowed Amt</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Ins. Paid</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Patient Owes</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]">Denial / Adjustment Reason</th>
          <th class="border-b border-[#1a1a1a] px-3.5 py-3 text-xs font-medium text-[#848481]"></th>
        </tr>
      </thead>
      <tbody>
        {#each claims as claim}
          <tr class="hover:bg-black/[0.005]">
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle leading-tight text-black">
              <span class="text-sm font-medium">{claim.patientName}</span>
              <div class="mt-0.5 text-[11px] text-[#848481]">DOB: {claim.dob}</div>
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle font-mono leading-tight text-black tabular-nums">
              {claim.payerName}
              <div class="mt-0.5 text-[11px] text-[#848481]">ID: {claim.memberId}</div>
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle leading-tight text-black">
              <span class="text-[13px] font-medium">{claim.serviceCode}</span>
              <div class="mt-0.5 text-[11px] text-[#848481]">{claim.serviceDesc}</div>
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle font-mono leading-tight text-black tabular-nums">${claim.allowedAmt.toFixed(2)}</td>
            <td
              class={`border-b border-black/5 px-3.5 py-3.5 align-middle font-mono leading-tight tabular-nums ${
                claim.fixRequired ? 'font-medium text-[#c93b2b]' : claim.insurancePaid > 0 ? 'font-medium text-[#10b981]' : 'text-black'
              }`}
            >
              ${claim.insurancePaid.toFixed(2)}
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle font-mono leading-tight text-black tabular-nums">
              ${claim.patientOwes.toFixed(2)}
              <div class="mt-0.5 text-[11px] text-[#848481]">{claim.owesLabel}</div>
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 align-middle leading-tight text-black">
              <div class="inline-flex items-center gap-1.5">
                <span class={`size-1.5 rounded-full ${claim.fixRequired ? 'bg-[#c93b2b]' : 'bg-[#10b981]'}`}></span>
                <span class={`block max-w-[200px] text-xs ${claim.fixRequired ? 'font-medium text-[#c93b2b]' : 'text-black'}`}>
                  {claim.reasonText}
                </span>
              </div>
            </td>
            <td class="border-b border-black/5 px-3.5 py-3.5 text-right align-middle leading-tight text-black">
              <Button
                class={`cursor-pointer rounded border px-3 py-1.5 text-xs font-medium transition-all duration-150 ${
                  claim.fixRequired
                    ? 'border-[#c93b2b] bg-[#fff1f0] text-[#c93b2b]'
                    : 'border-[#d4d4d0] bg-transparent text-black hover:border-black hover:bg-white'
                }`}
              >
                {claim.actionLabel}
              </Button>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</div>
