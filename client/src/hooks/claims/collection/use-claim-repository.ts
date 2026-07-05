import type { Sql } from 'postgres';
import type { Claim, ClaimRepository } from '../../../types/claims';

export const useClaimRepository = (sql: Sql): ClaimRepository => ({
  getOneByID: async (id) => {
    const [row] = await sql<Claim[]>`
      SELECT id, claim_number, patient_id, provider_name, date_of_service, cpt_hcpcs_code,
             service_description, billed_amount, allowed_amount, disallowed_amount,
             co_pay, deductible, co_insurance, discount_amount, paid_amount,
             patient_responsibility, explanation_code, created_at
      FROM claims
      WHERE id = ${id}
    `;

    return row || null;
  },

  getManyByPatientID: async (patientId) => {
    return await sql<Claim[]>`
      SELECT id, claim_number, patient_id, provider_name, date_of_service, cpt_hcpcs_code,
             service_description, billed_amount, allowed_amount, disallowed_amount,
             co_pay, deductible, co_insurance, discount_amount, paid_amount,
             patient_responsibility, explanation_code, created_at
      FROM claims
      WHERE patient_id = ${patientId}
      ORDER BY date_of_service DESC
    `;
  },

  getManyByProvider: async (providerName) => {
    return await sql<Claim[]>`
      SELECT id, claim_number, patient_id, provider_name, date_of_service, cpt_hcpcs_code,
             service_description, billed_amount, allowed_amount, disallowed_amount,
             co_pay, deductible, co_insurance, discount_amount, paid_amount,
             patient_responsibility, explanation_code, created_at
      FROM claims
      WHERE provider_name ILIKE ${'%' + providerName + '%'}
      ORDER BY date_of_service DESC
    `;
  },

  put: async (claim) => {
    const [row] = await sql<Claim[]>`
      INSERT INTO claims (
        claim_number, patient_id, provider_name, date_of_service, cpt_hcpcs_code,
        service_description, billed_amount, allowed_amount, disallowed_amount,
        co_pay, deductible, co_insurance, discount_amount, paid_amount,
        patient_responsibility, explanation_code, created_at
      ) VALUES (
        ${claim.claim_number}, ${claim.patient_id}, ${claim.provider_name}, ${claim.date_of_service}, ${claim.cpt_hcpcs_code},
        ${claim.service_description}, ${claim.billed_amount}, ${claim.allowed_amount}, ${claim.disallowed_amount},
        ${claim.co_pay}, ${claim.deductible}, ${claim.co_insurance}, ${claim.discount_amount}, ${claim.paid_amount},
        ${claim.patient_responsibility}, ${claim.explanation_code}, ${claim.created_at || new Date()}
      )
      RETURNING id, claim_number, patient_id, provider_name, date_of_service, cpt_hcpcs_code,
                service_description, billed_amount, allowed_amount, disallowed_amount,
                co_pay, deductible, co_insurance, discount_amount, paid_amount,
                patient_responsibility, explanation_code, created_at
    `;

    return row;
  },

  delete: async (id) => {
    const result = await sql`
      DELETE FROM claims WHERE id = ${id}
    `;

    return result.count > 0;
  }
});
