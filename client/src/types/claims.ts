export type Claim = {
  id: number;
  claim_number: string;
  patient_id: string;
  provider_name: string | null;
  date_of_service: Date;
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
  created_at: Date;
};

export type ClaimWithPatient = Omit<Claim, 'date_of_service' | 'created_at'> & {
  date_of_service: string;
  created_at: string;
  patient_first_name: string;
  patient_last_name: string;
  patient_date_of_birth: string | null;
  insurance_member_id: string | null;
};

export type ClaimRepository = {
  getOneByID: (id: number) => Promise<Claim | null>;
  getManyWithPatients: () => Promise<ClaimWithPatient[]>;
  getManyByPatientID: (patientId: string) => Promise<Claim[]>;
  getManyByProvider: (providerName: string) => Promise<Claim[]>;
  put: (claim: Omit<Claim, 'id' | 'created_at'> & { id?: number; created_at?: Date }) => Promise<Claim>;
  delete: (id: number) => Promise<boolean>;
};
