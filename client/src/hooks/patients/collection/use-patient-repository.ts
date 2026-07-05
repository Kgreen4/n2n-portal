import type { Sql } from 'postgres';
import type { Patient, PatientRepository } from '../../../types/patient';

export const usePatientRepository = (sql: Sql): PatientRepository => ({
  getOneByID: async (id) => {
    const [row] = await sql<Patient[]>`
      SELECT id, first_name, last_name, date_of_birth, insurance_member_id, created_at
      FROM patients
      WHERE id = ${id}
    `;

    return row || null;
  },

  getOneByInsuranceID: async (memberId) => {
    const [row] = await sql<Patient[]>`
      SELECT id, first_name, last_name, date_of_birth, insurance_member_id, created_at
      FROM patients
      WHERE insurance_member_id = ${memberId}
    `;

    return row || null;
  },

  put: async (patient) => {
    const [row] = await sql<Patient[]>`
      INSERT INTO patients (id, first_name, last_name, date_of_birth, insurance_member_id, created_at)
      VALUES (
        ${patient.id},
        ${patient.first_name},
        ${patient.last_name},
        ${patient.date_of_birth},
        ${patient.insurance_member_id},
        ${patient.created_at || new Date()}
      )
      ON CONFLICT (id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        date_of_birth = EXCLUDED.date_of_birth,
        insurance_member_id = EXCLUDED.insurance_member_id
      RETURNING id, first_name, last_name, date_of_birth, insurance_member_id, created_at
    `;

    return row;
  },

  delete: async (id) => {
    const result = await sql`
      DELETE FROM patients WHERE id = ${id}
    `;

    return result.count > 0;
  }
});
