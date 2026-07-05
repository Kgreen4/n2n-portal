import type { Sql } from 'postgres';
import type { Claim, ClaimRepository, ClaimWithPatient } from '../../../types/claims';

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

  getManyWithPatients: async () => {
    return await sql<ClaimWithPatient[]>`
      SELECT
        claims.id,
        claims.claim_number,
        claims.patient_id,
        claims.provider_name,
        claims.date_of_service::text AS date_of_service,
        claims.cpt_hcpcs_code,
        claims.service_description,
        claims.billed_amount::float AS billed_amount,
        claims.allowed_amount::float AS allowed_amount,
        claims.disallowed_amount::float AS disallowed_amount,
        claims.co_pay::float AS co_pay,
        claims.deductible::float AS deductible,
        claims.co_insurance::float AS co_insurance,
        claims.discount_amount::float AS discount_amount,
        claims.paid_amount::float AS paid_amount,
        claims.patient_responsibility::float AS patient_responsibility,
        claims.explanation_code,
        claims.created_at::text AS created_at,
        patients.first_name AS patient_first_name,
        patients.last_name AS patient_last_name,
        patients.date_of_birth::text AS patient_date_of_birth,
        patients.insurance_member_id
      FROM claims
      INNER JOIN patients ON patients.id = claims.patient_id
      ORDER BY claims.created_at DESC, claims.date_of_service DESC, claims.id DESC
    `;
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
