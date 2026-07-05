import type {
  EngineOutput,
  MemoryRecord,
  MemoryResult
} from '../../features/agents/blue-cross-blue-shield/types';
import type { Collection } from '../../types/collection';

const persistBlueCrossRecord = async (collection: Collection, record: MemoryRecord) => {
  const existingPatient = record.patient.insurance_member_id
    ? await collection.patients.getOneByInsuranceID(record.patient.insurance_member_id)
    : null;
  const patient = await collection.patients.put({
    ...record.patient,
    id: existingPatient?.id ?? record.patient.id
  });
  const claims = await Promise.all(
    record.claims.map((claim) =>
      collection.claims.put({
        ...claim,
        patient_id: patient.id
      })
    )
  );

  return {
    patient,
    claims
  };
};

export const useMemory = (collection: Collection) => ({
  rememberBlueCrossBlueShield: async (output: EngineOutput): Promise<MemoryResult> => {
    const persisted = await Promise.all(output.memoryRecords.map((record) => persistBlueCrossRecord(collection, record)));

    return {
      patients: persisted.map((record) => record.patient),
      claims: persisted.flatMap((record) => record.claims),
      skipped: output.patientClaims.length - output.memoryRecords.length
    };
  }
});
