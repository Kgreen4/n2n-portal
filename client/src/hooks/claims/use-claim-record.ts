import type { Claim, ClaimRepository } from '../../types/claims';

type ClaimInput = Parameters<ClaimRepository['put']>[0];

const dateKey = (value: Date | string) => {
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toISOString().slice(0, 10);
};

const textKey = (value: string | null | undefined) => {
  return value ?? '';
};

const numberKey = (value: number) => {
  return Number(value).toFixed(2);
};

const sameClaimLine = (left: Claim, right: ClaimInput) => {
  return (
    left.claim_number === right.claim_number &&
    left.patient_id === right.patient_id &&
    dateKey(left.date_of_service) === dateKey(right.date_of_service) &&
    textKey(left.cpt_hcpcs_code) === textKey(right.cpt_hcpcs_code) &&
    textKey(left.service_description) === textKey(right.service_description) &&
    numberKey(left.billed_amount) === numberKey(right.billed_amount) &&
    numberKey(left.allowed_amount) === numberKey(right.allowed_amount) &&
    numberKey(left.disallowed_amount) === numberKey(right.disallowed_amount) &&
    numberKey(left.co_pay) === numberKey(right.co_pay) &&
    numberKey(left.deductible) === numberKey(right.deductible) &&
    numberKey(left.co_insurance) === numberKey(right.co_insurance) &&
    numberKey(left.discount_amount) === numberKey(right.discount_amount) &&
    numberKey(left.paid_amount) === numberKey(right.paid_amount) &&
    numberKey(left.patient_responsibility) === numberKey(right.patient_responsibility) &&
    textKey(left.explanation_code) === textKey(right.explanation_code)
  );
};

export const useClaimRecord = (claims: ClaimRepository) => ({
  putUnique: async (claim: ClaimInput): Promise<Claim> => {
    const existingClaims = await claims.getManyByPatientID(claim.patient_id);
    const existing = existingClaims.find((item) => sameClaimLine(item, claim));

    return existing ?? claims.put(claim);
  }
});
