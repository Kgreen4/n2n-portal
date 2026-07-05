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

export type ClaimRepository = {
  getOneByID: (id: number) => Promise<Claim | null>;
  getManyByPatientID: (patientId: string) => Promise<Claim[]>;
  getManyByProvider: (providerName: string) => Promise<Claim[]>;
  put: (claim: Omit<Claim, 'id' | 'created_at'> & { id?: number; created_at?: Date }) => Promise<Claim>;
  delete: (id: number) => Promise<boolean>;
};