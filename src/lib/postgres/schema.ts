export type TableSchema = {
  name: string;
  columns: string[];
};

export const schemas: TableSchema[] = [
  {
    name: 'patients',
    columns: ['id', 'first_name', 'last_name', 'date_of_birth', 'insurance_member_id', 'created_at']
  },
  {
    name: 'claims',
    columns: [
      'id',
      'claim_number',
      'patient_id',
      'provider_name',
      'date_of_service',
      'cpt_hcpcs_code',
      'service_description',
      'billed_amount',
      'allowed_amount',
      'disallowed_amount',
      'co_pay',
      'deductible',
      'co_insurance',
      'discount_amount',
      'paid_amount',
      'patient_responsibility',
      'explanation_code',
      'created_at'
    ]
  }
];
