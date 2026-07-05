<script module lang="ts">
  export type DashboardMetrics = {
    totalInsurancePaid: number;
    transferredToPatient: number;
    deniedClaimsValue: number;
    averageClaimTurnaroundDays: number;
  };
</script>

<script lang="ts">
  let { metrics }: { metrics: DashboardMetrics } = $props();

  const currency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(value);
  };
</script>

<section class="grid grid-cols-4 gap-6 border-y border-t-black border-b-black/5 py-4">
  <div class="flex flex-col justify-between">
    <span class="text-[11px] font-medium tracking-[0.5px] text-[#848481] uppercase">Total Insurance Paid</span>
    <div class="mt-2 flex items-baseline justify-between">
      <span class="text-2xl font-normal tracking-normal text-black tabular-nums">{currency(metrics.totalInsurancePaid)}</span>
      <span class="font-mono text-xs text-[#10b981]">Check Cleared</span>
    </div>
  </div>
  <div class="flex flex-col justify-between">
    <span class="text-[11px] font-medium tracking-[0.5px] text-[#848481] uppercase">Transferred to Patient</span>
    <div class="mt-2 flex items-baseline justify-between">
      <span class="text-2xl font-normal tracking-normal text-black tabular-nums">{currency(metrics.transferredToPatient)}</span>
      <span class="font-mono text-xs text-[#848481]">Co-pays / Deductibles</span>
    </div>
  </div>
  <div class="flex flex-col justify-between">
    <span class="text-[11px] font-medium tracking-[0.5px] text-[#848481] uppercase">Denied Claims Value</span>
    <div class="mt-2 flex items-baseline justify-between">
      <span class="text-2xl font-normal tracking-normal text-[#c93b2b] tabular-nums">{currency(metrics.deniedClaimsValue)}</span>
      <span class="font-mono text-xs text-[#c93b2b]">Requires Appeal</span>
    </div>
  </div>
  <div class="flex flex-col justify-between">
    <span class="text-[11px] font-medium tracking-[0.5px] text-[#848481] uppercase">Avg. Claim Turnaround</span>
    <div class="mt-2 flex items-baseline justify-between">
      <span class="text-2xl font-normal tracking-normal text-black tabular-nums">{metrics.averageClaimTurnaroundDays} Days</span>
      <span class="font-mono text-xs text-[#848481]">Normal Window</span>
    </div>
  </div>
</section>
