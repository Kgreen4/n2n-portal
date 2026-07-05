import type { Patient, PatientRepository } from '../../types/patient';

type PatientInput = Parameters<PatientRepository['put']>[0];

export const usePatientRecord = (patients: PatientRepository) => ({
  findOrPut: async (patient: PatientInput): Promise<Patient> => {
    const existingByInsuranceId = patient.insurance_member_id
      ? await patients.getOneByInsuranceID(patient.insurance_member_id)
      : null;
    const existingById = existingByInsuranceId ? null : await patients.getOneByID(patient.id);

    return patients.put({
      ...patient,
      id: existingByInsuranceId?.id ?? existingById?.id ?? patient.id
    });
  }
});
