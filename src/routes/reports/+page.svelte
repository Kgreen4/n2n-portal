<script lang="ts">
  import { MainLayout } from '$components/layout';
  import { ReportTable, ReportToolbar, type PatientRow } from '$features/reports';
  import { Card } from '$ui/card';

  let patients: PatientRow[] = [
    {
      id: 'PAT-1001',
      first_name: 'Anthony',
      last_name: 'Dionne',
      date_of_birth: '1982-04-18',
      insurance_member_id: 'INS-20491',
      created_at: '2026-07-04 09:12:31 UTC',
      claims: [
        {
          claim_number: 'CLM-900120',
          patient_id: 'PAT-1001',
          provider_name: 'North Clinic Group',
          date_of_service: '2026-06-12',
          cpt_hcpcs_code: '99213',
          service_description: 'Established patient office visit',
          billed_amount: 240,
          allowed_amount: 185,
          disallowed_amount: 55,
          co_pay: 25,
          deductible: 0,
          co_insurance: 18.5,
          discount_amount: 0,
          paid_amount: 141.5,
          patient_responsibility: 43.5,
          explanation_code: 'CO-45',
          created_at: '2026-07-04 09:14:02 UTC'
        },
        {
          claim_number: 'CLM-900121',
          patient_id: 'PAT-1001',
          provider_name: 'North Clinic Group',
          date_of_service: '2026-06-19',
          cpt_hcpcs_code: '80053',
          service_description: 'Comprehensive metabolic panel',
          billed_amount: 120,
          allowed_amount: 74,
          disallowed_amount: 46,
          co_pay: 0,
          deductible: 20,
          co_insurance: 5.4,
          discount_amount: 0,
          paid_amount: 48.6,
          patient_responsibility: 25.4,
          explanation_code: 'PR-1',
          created_at: '2026-07-04 09:15:33 UTC'
        }
      ]
    },
    {
      id: 'PAT-1002',
      first_name: 'Nick',
      last_name: 'Yahodin',
      date_of_birth: '1976-11-02',
      insurance_member_id: 'INS-11872',
      created_at: '2026-07-04 09:18:04 UTC',
      claims: [
        {
          claim_number: 'CLM-900210',
          patient_id: 'PAT-1002',
          provider_name: 'Metro Imaging Associates',
          date_of_service: '2026-05-28',
          cpt_hcpcs_code: '71046',
          service_description: 'Chest x-ray two views',
          billed_amount: 315,
          allowed_amount: 210,
          disallowed_amount: 105,
          co_pay: 35,
          deductible: 50,
          co_insurance: 12.5,
          discount_amount: 0,
          paid_amount: 112.5,
          patient_responsibility: 97.5,
          explanation_code: 'PR-2',
          created_at: '2026-07-04 09:19:44 UTC'
        }
      ]
    },
    {
      id: 'PAT-1003',
      first_name: 'Mujeeb',
      last_name: 'Aimaq',
      date_of_birth: null,
      insurance_member_id: 'INS-77340',
      created_at: '2026-07-04 10:03:45 UTC',
      claims: [
        {
          claim_number: 'CLM-900330',
          patient_id: 'PAT-1003',
          provider_name: 'Central Care Partners',
          date_of_service: '2026-06-03',
          cpt_hcpcs_code: '97110',
          service_description: 'Therapeutic exercises',
          billed_amount: 180,
          allowed_amount: 132,
          disallowed_amount: 48,
          co_pay: 20,
          deductible: 0,
          co_insurance: 11.2,
          discount_amount: 0,
          paid_amount: 100.8,
          patient_responsibility: 31.2,
          explanation_code: 'CO-253',
          created_at: '2026-07-04 10:05:12 UTC'
        },
        {
          claim_number: 'CLM-900331',
          patient_id: 'PAT-1003',
          provider_name: 'Central Care Partners',
          date_of_service: '2026-06-10',
          cpt_hcpcs_code: '97530',
          service_description: 'Therapeutic activities',
          billed_amount: 210,
          allowed_amount: 150,
          disallowed_amount: 60,
          co_pay: 20,
          deductible: 10,
          co_insurance: 12,
          discount_amount: 0,
          paid_amount: 108,
          patient_responsibility: 42,
          explanation_code: 'PR-3',
          created_at: '2026-07-04 10:07:18 UTC'
        }
      ]
    }
  ];

  let currentTab = $state('Patients');
  let searchQuery = $state('');
  let activeMenu = $state('Reports');

  const handleAddReport = () => {
    window.alert('Add patient action triggered');
  };
</script>

<Card class="relative m-0 min-h-screen w-full max-w-none rounded-none border-0 bg-[#f9f9f7] px-[clamp(20px,4vw,56px)] py-6 shadow-none">
  <MainLayout bind:activeMenu />

  <main class="flex h-[calc(100vh-88px)] min-h-0 flex-col">
    <h1 class="mt-4 mb-8 shrink-0 text-[32px] font-bold tracking-normal text-[#111111]">Reports management</h1>

    <ReportToolbar bind:currentTab bind:searchQuery onAdd={handleAddReport} />

    <ReportTable {patients} />
  </main>
</Card>
